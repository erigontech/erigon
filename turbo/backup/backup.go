package backup

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	mdbx2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/torquem-ch/mdbx-go/mdbx"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

func OpenPair(from, to string, label kv.Label, targetPageSize datasize.ByteSize) (kv.RoDB, kv.RwDB) {
	src := mdbx2.NewMDBX(log.New()).Path(from).
		Label(label).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		Flags(func(flags uint) uint { return mdbx.Readonly | mdbx.Accede }).MustOpen()
	if targetPageSize <= 0 {
		targetPageSize = datasize.ByteSize(src.PageSize())
	}
	info, err := src.(*mdbx2.MdbxKV).Env().Info(nil)
	if err != nil {
		panic(err)
	}
	dst := mdbx2.NewMDBX(log.New()).Path(to).
		Label(label).
		PageSize(targetPageSize.Bytes()).
		MapSize(datasize.ByteSize(info.Geo.Upper)).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		MustOpen()
	return src, dst
}
func MdbxToMdbx(ctx context.Context, from, to string, label kv.Label, tables []string, targetPageSize datasize.ByteSize, readAheadThreads int) error {
	src := mdbx2.NewMDBX(log.New()).Path(from).
		Label(label).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		Flags(func(flags uint) uint { return mdbx.Readonly | mdbx.Accede }).MustOpen()
	if targetPageSize <= 0 {
		targetPageSize = datasize.ByteSize(src.PageSize())
	}
	info, err := src.(*mdbx2.MdbxKV).Env().Info(nil)
	if err != nil {
		return err
	}
	dst := mdbx2.NewMDBX(log.New()).Path(to).
		Label(label).
		PageSize(targetPageSize.Bytes()).
		MapSize(datasize.ByteSize(info.Geo.Upper)).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		MustOpen()
	return Kv2kv(ctx, src, dst, tables, readAheadThreads)
}

func Kv2kv(ctx context.Context, src kv.RoDB, dst kv.RwDB, tables []string, readAheadThreads int) error {
	srcTx, err1 := src.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer srcTx.Rollback()

	commitEvery := time.NewTicker(5 * time.Minute)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	tablesMap := src.AllTables()
	if len(tables) > 0 {
		tablesMapCopy := maps.Clone(tablesMap)
		tablesMap = kv.TableCfg{}
		for _, name := range tables {
			tablesMap[name] = tablesMapCopy[name]
		}
	}

	for name, b := range tablesMap {
		if b.IsDeprecated {
			continue
		}
		if err := backupTable(ctx, src, srcTx, dst, name, readAheadThreads, logEvery); err != nil {
			return err
		}
	}
	log.Info("done")
	return nil
}

func backupTable(ctx context.Context, src kv.RoDB, srcTx kv.Tx, dst kv.RwDB, table string, readAheadThreads int, logEvery *time.Ticker) error {
	var total uint64
	wg := sync.WaitGroup{}
	defer wg.Wait()
	warmupCtx, warmupCancel := context.WithCancel(ctx)
	defer warmupCancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		WarmupTable(warmupCtx, src, table, log.LvlTrace, readAheadThreads)
	}()
	srcC, err := srcTx.Cursor(table)
	if err != nil {
		return err
	}
	total, _ = srcC.Count()

	dstTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return err1
	}
	defer dstTx.Rollback()
	_ = dstTx.ClearBucket(table)

	c, err := dstTx.RwCursor(table)
	if err != nil {
		return err
	}
	casted, isDupsort := c.(kv.RwCursorDupSort)
	i := uint64(0)

	for k, v, err := srcC.First(); k != nil; k, v, err = srcC.Next() {
		if err != nil {
			return err
		}

		if isDupsort {
			if err = casted.AppendDup(k, v); err != nil {
				panic(err)
			}
		} else {
			if err = c.Append(k, v); err != nil {
				panic(err)
			}
		}

		i++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			log.Info("Progress", "table", table, "progress", fmt.Sprintf("%.1fm/%.1fm", float64(i)/1_000_000, float64(total)/1_000_000), "key", hex.EncodeToString(k),
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		default:
		}
	}
	// migrate bucket sequences to native mdbx implementation
	//currentID, err := srcTx.Sequence(name, 0)
	//if err != nil {
	//	return err
	//}
	//_, err = dstTx.Sequence(name, currentID)
	//if err != nil {
	//	return err
	//}
	if err2 := dstTx.Commit(); err2 != nil {
		return err2
	}
	return nil
}

const ReadAheadThreads = 128

func WarmupTable(ctx context.Context, db kv.RoDB, bucket string, lvl log.Lvl, readAheadThreads int) {
	var ThreadsLimit = readAheadThreads
	var total uint64
	db.View(ctx, func(tx kv.Tx) error {
		c, _ := tx.Cursor(bucket)
		total, _ = c.Count()
		return nil
	})
	if total < 10_000 {
		return
	}
	progress := atomic.Int64{}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ThreadsLimit)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			i := i
			j := j
			g.Go(func() error {
				return db.View(ctx, func(tx kv.Tx) error {
					it, err := tx.Prefix(bucket, []byte{byte(i), byte(j)})
					if err != nil {
						return err
					}
					for it.HasNext() {
						_, _, err = it.Next()
						if err != nil {
							return err
						}
						progress.Add(1)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-logEvery.C:
							log.Log(lvl, fmt.Sprintf("Progress: %s %.2f%%", bucket, 100*float64(progress.Load())/float64(total)))
						default:
						}
					}
					return nil
				})
			})
		}
	}
	for i := 0; i < 1_000; i++ {
		i := i
		g.Go(func() error {
			return db.View(ctx, func(tx kv.Tx) error {
				seek := make([]byte, 8)
				binary.BigEndian.PutUint64(seek, uint64(i*100_000))
				it, err := tx.Prefix(bucket, seek)
				if err != nil {
					return err
				}
				for it.HasNext() {
					_, _, err = it.Next()
					if err != nil {
						return err
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-logEvery.C:
						log.Log(lvl, fmt.Sprintf("Progress: %s %.2f%%", bucket, 100*float64(progress.Load())/float64(total)))
					default:
					}
				}
				return nil
			})
		})
	}
	_ = g.Wait()
}

func ClearTables(ctx context.Context, db kv.RoDB, tx kv.RwTx, tables ...string) error {
	for _, tbl := range tables {
		if err := ClearTable(ctx, db, tx, tbl); err != nil {
			return err
		}
	}
	return nil
}

func ClearTable(ctx context.Context, db kv.RoDB, tx kv.RwTx, table string) error {
	ctx, cancel := context.WithCancel(ctx)
	clean := warmup(ctx, db, table)
	defer func() {
		cancel()
		clean()
	}()
	log.Info("Clear", "table", table)
	return tx.ClearBucket(table)
}

func warmup(ctx context.Context, db kv.RoDB, bucket string) func() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		WarmupTable(ctx, db, bucket, log.LvlInfo, ReadAheadThreads)
	}()
	return func() { wg.Wait() }
}
