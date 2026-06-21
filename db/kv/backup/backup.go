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
	size, _ := srcTx.BucketSize(table)
	if total > 0 {
		logger.Info("[mdbx_to_mdbx] copying", "table", table, "rows", common.PrettyCounter(total), "size", common.ByteCount(size))
	}

	// Parallel read-ahead: keep a bounded band of pages warm just ahead of the
	// copy cursor (cold page faults are slow; one reader can't saturate nvme).
	// No-op unless WARMUP_TABLE_WORKERS is set.
	var ra *kv.ReadAheader
	if workers := int(dbg.WarmupTableWorkers); workers > 0 && total > 0 {
		ra = kv.NewReadAheader(ctx, src, table, nil, workers)
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

var (
	clearChunkSize   = 1 * datasize.GB
	clearCommitEvery = 20 * time.Second
)

// ClearTables empties each table with mdbx's native bulk range-delete instead of
// dropping it, owning its own write transactions and committing roughly every
// 20s so a single huge table can't blow up one transaction. When
// WARMUP_TABLE_WORKERS>0 it keeps pages warm just ahead of the delete cursor.
//
// It must NOT be called inside an open write tx: it opens its own (mdbx
// serializes writers, so a caller holding one would deadlock).
func ClearTables(ctx context.Context, db kv.RwDB, tables ...string) error {
	for _, table := range tables {
		if err := clearTable(ctx, db, table); err != nil {
			return fmt.Errorf("clearing %s: %w", table, err)
		}
	}
	return nil
}

// ClearTableInTx empties one table inside the caller's transaction using mdbx's
// native range-delete, for call sites that must stay atomic with surrounding
// work and so can't use the self-committing ClearTables. Falls back to a table
// drop when the backend has no range-delete.
func ClearTableInTx(tx kv.RwTx, table string) error {
	if dr, ok := tx.(kv.HasDeleteRange); ok {
		_, err := dr.DeleteRange(table, nil, nil)
		return err
	}
	return tx.ClearTable(table)
}

func clearTable(ctx context.Context, db kv.RwDB, table string) error {
	bounds, size, err := chunkBounds(ctx, db, table)
	if err != nil {
		return err
	}
	log.Info("[clear]", "table", table, "size", common.ByteCount(size))
	if len(bounds) < 2 { // backend can't count-split: clear in one shot
		return db.Update(ctx, func(tx kv.RwTx) error { return ClearTableInTx(tx, table) })
	}

	var ra *kv.ReadAheader
	if workers := int(dbg.WarmupTableWorkers); workers > 0 {
		ra = kv.NewReadAheader(ctx, db, table, nil, workers)
	}
	defer ra.Close()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	started := time.Now()
	var deleted uint64

	i := 0
	for i+1 < len(bounds) {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			dr, ok := tx.(kv.HasDeleteRange)
			if !ok {
				i = len(bounds) // no native range-delete: clear whole table once and stop
				return ClearTableInTx(tx, table)
			}
			start := time.Now()
			for i+1 < len(bounds) {
				n, err := dr.DeleteRange(table, bounds[i], bounds[i+1])
				if err != nil {
					return err
				}
				deleted += n
				i++
				if i+1 < len(bounds) {
					ra.SetPos(bounds[i]) // next chunk's lower bound (interior, non-nil)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					secs := time.Since(started).Seconds()
					frac := float64(i) / float64(len(bounds)-1)
					log.Info("[clear]", "table", table,
						"progress", fmt.Sprintf("%d/%d", i, len(bounds)-1),
						"size", common.ByteCount(size),
						"keys/s", common.PrettyCounter(uint64(float64(deleted)/secs)),
						"speed", common.ByteCount(uint64(frac*float64(size)/secs))+"/s")
				default:
				}
				if time.Since(start) >= clearCommitEvery {
					return nil // commit and reopen a fresh tx
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// chunkBounds splits table into ~clearChunkSize count-balanced ranges and
// returns the cloned boundaries (nil if the backend can't count-split) plus the
// table's on-disk size.
func chunkBounds(ctx context.Context, db kv.RoDB, table string) (bounds [][]byte, size uint64, err error) {
	err = db.View(ctx, func(tx kv.Tx) error {
		s, ok := tx.(kv.BucketSplitter)
		if !ok {
			return nil
		}
		size, _ = tx.BucketSize(table)
		b, err := s.SplitBucketByCount(table, nil, int(size/clearChunkSize.Bytes()))
		if err != nil {
			return err
		}
		bounds = make([][]byte, len(b)) // interior keys are zero-copy, valid only until tx end
		for i, k := range b {
			bounds[i] = bytes.Clone(k)
		}
		return nil
	})
	return bounds, size, err
}
