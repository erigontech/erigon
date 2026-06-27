// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package backup

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
)

func OpenPair(from, to string, label kv.Label, targetPageSize datasize.ByteSize, logger log.Logger) (kv.RoDB, kv.RwDB) {
	const ThreadsHardLimit = 9_000
	src := mdbx2.New(label, logger).Path(from).
		RoTxsLimiter(semaphore.NewWeighted(ThreadsHardLimit)).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		Accede(true).
		MustOpen()
	if targetPageSize <= 0 {
		targetPageSize = src.PageSize()
	}
	info, err := src.(*mdbx2.MdbxKV).Env().Info(nil)
	if err != nil {
		panic(err)
	}
	dst := mdbx2.New(label, logger).Path(to).
		PageSize(targetPageSize).
		MapSize(datasize.ByteSize(info.Geo.Upper)).
		GrowthStep(4 * datasize.GB).
		WriteMap(true).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		MustOpen()
	return src, dst
}

func Kv2kv(ctx context.Context, src kv.RoDB, dst kv.RwDB, tables []string, logger log.Logger) error {
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

	var copiedTables int
	var copiedRows uint64
	for _, name := range slices.Sorted(maps.Keys(tablesMap)) { // deterministic order for reproducible benchmarks
		if tablesMap[name].IsDeprecated {
			continue
		}
		rows, err := backupTable(ctx, src, srcTx, dst, name, logEvery, logger)
		if err != nil {
			return err
		}
		if rows > 0 {
			copiedTables++
			copiedRows += rows
		}
	}
	logger.Info("done", "tablesWithData", copiedTables, "rows", common.PrettyCounter(copiedRows))
	return nil
}

func backupTable(ctx context.Context, src kv.RoDB, srcTx kv.Tx, dst kv.RwDB, table string, logEvery *time.Ticker, logger log.Logger) (uint64, error) {
	t := time.Now()
	srcC, err := srcTx.Cursor(table)
	if err != nil {
		return 0, err
	}
	defer srcC.Close()
	total, _ := srcTx.Count(table)
	size, err := srcTx.BucketSize(table)
	if err != nil {
		return 0, err
	}
	if total > 0 {
		logger.Info("[mdbx_to_mdbx] copying", "table", table, "rows", common.PrettyCounter(total), "size", common.ByteCount(size))
	}

	// Parallel read-ahead: keep a bounded band of pages warm just ahead of the
	// copy cursor (cold page faults are slow; one reader can't saturate nvme).
	// No-op unless WARMUP_TABLE_WORKERS is set.
	var ra *kv.ReadAhead
	if workers := int(dbg.WarmupTableWorkers); workers > 0 && total > 0 {
		ra = kv.NewReadAhead(ctx, src, table, nil, workers)
	}
	defer ra.Close()

	if err := dst.Update(ctx, func(tx kv.RwTx) error {
		return tx.ClearTable(table)
	}); err != nil {
		return 0, err
	}
	dstTx, err := dst.BeginRw(ctx)
	if err != nil {
		return 0, err
	}
	defer dstTx.Rollback()

	c, err := dstTx.RwCursor(table)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	casted, isDupsort := c.(kv.RwCursorDupSort)
	i := uint64(0)

	for k, v, err := srcC.First(); k != nil; k, v, err = srcC.Next() {
		if err != nil {
			return 0, err
		}

		if isDupsort {
			if err = casted.AppendDup(k, v); err != nil {
				return 0, err
			}
		} else {
			if err = c.Append(k, v); err != nil {
				return 0, err
			}
		}

		i++
		if i%1000 == 0 {
			ra.SetPos(k)
		}
		if i%100_000 == 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				logger.Info("Progress", "table", table, "progress",
					fmt.Sprintf("%s/%s", common.PrettyCounter(i), common.PrettyCounter(total)),
					"size", common.ByteCount(size), "keys/s", uint64(float64(i)/time.Since(t).Seconds()),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			default:
			}
		}
	}

	// TODO: Unwind doesn't need to decrement the auto-increment sequence — it's
	// not exposed to users and not part of consensus, so we could switch to
	// mdbx's native Sequence.

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
		return 0, err2
	}
	return i, nil
}

// ClearTables empties each table with mdbx's native bulk range-delete on the
// caller's tx — atomic with the caller's other writes and, unlike a self-owned
// writer, safe to call inside an open write tx. db only drives read-only
// read-ahead that warms pages just ahead of the chunked delete cursor when
// WARMUP_TABLE_WORKERS>0, which is where the speed comes from.
func ClearTables(ctx context.Context, db kv.RoDB, tx kv.RwTx, tables ...string) error {
	for _, table := range tables {
		if err := clearTable(ctx, db, tx, table); err != nil {
			return fmt.Errorf("clearing %s: %w", table, err)
		}
	}
	return nil
}

func clearTable(ctx context.Context, db kv.RoDB, tx kv.RwTx, table string) error {
	dr, ok := tx.(kv.HasDeleteRange)
	if !ok { // backend has no native range-delete: drop the whole table
		return tx.ClearTable(table)
	}

	size, err := tx.BucketSize(table)
	if err != nil {
		return err
	}
	log.Info("[clear]", "table", table, "size", common.ByteCount(size))

	bounds, err := chunkBounds(tx, table, size)
	if err != nil {
		return err
	}
	if len(bounds) < 2 { // backend can't count-split: clear in one shot
		_, err := dr.DeleteRange(table, nil, nil)
		return err
	}

	var ra *kv.ReadAhead
	if workers := int(dbg.WarmupTableWorkers); workers > 0 {
		ra = kv.NewReadAhead(ctx, db, table, nil, workers)
		ra.SetLogLevel(log.LvlDebug) // [clear] already logs progress; keep read-ahead quiet
	}
	defer ra.Close()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	started := time.Now()
	var deleted uint64
	for i := 0; i+1 < len(bounds); i++ {
		ra.SetPos(bounds[i])
		n, err := dr.DeleteRange(table, bounds[i], bounds[i+1])
		if err != nil {
			return err
		}
		deleted += n

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			secs := time.Since(started).Seconds()
			clearedBytes := float64(i+1) / float64(len(bounds)-1) * float64(size) // estimate: chunks are count-balanced, not byte-balanced
			remaining, err := tx.BucketSize(table)
			if err != nil {
				return err
			}
			log.Info("[clear]", "table", table,
				"speed", common.ByteCount(uint64(clearedBytes/secs))+"/s",
				"keys", common.PrettyCounter(uint64(float64(deleted)/secs))+"/s",
				"progress", fmt.Sprintf("%d/%d", i+1, len(bounds)-1),
				"size", common.ByteCount(remaining),
			)
		default:
		}
	}
	return nil
}

// chunkBounds splits table into count-balanced ranges and returns the cloned
// boundaries (nil if the backend can't count-split).
func chunkBounds(tx kv.RwTx, table string, size uint64) (bounds [][]byte, err error) {
	s, ok := tx.(kv.DBWithDistributionSupport)
	if !ok {
		return nil, nil
	}
	const clearChunkSize = 64 * datasize.MB
	chunks := size / clearChunkSize.Bytes()
	b, err := s.DistributeCursors(table, nil, int(chunks))
	if err != nil {
		return nil, err
	}
	bounds = make([][]byte, len(b)) // interior keys are zero-copy, valid only until tx end
	for i, k := range b {
		bounds[i] = bytes.Clone(k)
	}
	return bounds, nil
}
