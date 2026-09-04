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
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
)

const (
	dataFileName = "mdbx.dat"
	lockFileName = "mdbx.lck"
	// compactDirName stages the copy inside the db's own directory. A datadir may
	// put a single db on its own volume (a symlink or mount at <datadir>/chaindata),
	// where a sibling directory would land on the parent's filesystem and make the
	// final rename cross-device.
	compactDirName = "compacting"
)

func OpenPair(from, to string, label kv.Label, targetPageSize datasize.ByteSize, logger log.Logger) (kv.RoDB, kv.RwDB) {
	var size int64
	if st, err := os.Stat(filepath.Join(from, dataFileName)); err == nil { // a db we can't stat fails the open below anyway
		size = st.Size()
	}
	src, dst, err := openPair(context.Background(), from, to, label, false, targetPageSize, growthStepFor(size), kv.TablesCfgByLabel(label), logger)
	if err != nil {
		panic(err)
	}
	return src, dst
}

// growthStepFor scales the growth step with the db. A bulk copy wants few file
// extensions, but mdbx rounds the file up to a whole step, so one step sized for
// chaindata would pad every small db in a datadir to that size.
func growthStepFor(size int64) datasize.ByteSize {
	return min(4*datasize.GB, max(1*datasize.MB, datasize.ByteSize(size)/64))
}

// openPair opens src and creates dst with src's page size and map size. Both get
// growthStep: opening src read-write resets its geometry to this process's
// defaults, whose 1GB step would inflate a small db before it is even read.
// exclusive makes mdbx refuse a src another process still has open. A nil tables
// takes the set from src's file instead of the label's schema.
func openPair(ctx context.Context, from, to string, label kv.Label, exclusive bool, targetPageSize, growthStep datasize.ByteSize, tables kv.TableCfg, logger log.Logger) (_ kv.RoDB, _ kv.RwDB, err error) {
	const ThreadsHardLimit = 9_000
	srcCfg := tables
	if srcCfg == nil {
		srcCfg = kv.TableCfg{} // src opens bare, then reports what the file holds
	}
	src, err := mdbx2.New(label, logger).Path(from).
		RoTxsLimiter(semaphore.NewWeighted(ThreadsHardLimit)).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return srcCfg }).
		GrowthStep(growthStep).
		Accede(true).
		Exclusive(exclusive).
		Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			src.Close()
		}
	}()
	if tables == nil {
		if tables, err = tablesOnDisk(ctx, src); err != nil {
			return nil, nil, err
		}
	}
	if targetPageSize <= 0 {
		targetPageSize = src.PageSize()
	}
	info, err := src.(*mdbx2.MdbxKV).Env().Info(nil)
	if err != nil {
		return nil, nil, err
	}
	dst, err := mdbx2.New(label, logger).Path(to).
		PageSize(targetPageSize).
		MapSize(datasize.ByteSize(info.Geo.Upper)).
		GrowthStep(growthStep).
		WriteMap(true).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return tables }).
		Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	return src, dst, nil
}

// CompactInPlace rewrites the mdbx db at dbDir without its free pages: it copies
// into a subdirectory, then moves the result back over the original. Needs free
// space for a second copy of the live data, and the db must not be in use.
func CompactInPlace(ctx context.Context, dbDir string, label kv.Label, logger log.Logger) error {
	dataFile := filepath.Join(dbDir, dataFileName)
	before, err := os.Stat(dataFile)
	if err != nil {
		return err
	}

	tmpDir := filepath.Join(dbDir, compactDirName)
	if err := dir.RemoveAll(tmpDir); err != nil {
		return err
	}
	// 0700: the copy holds the db's contents for the whole run, before the
	// original's mode is applied to it.
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return err
	}
	defer dir.RemoveAll(tmpDir) //nolint:errcheck

	logger.Info("[compact] compacting", "label", label, "db", dbDir, "size", common.ByteCount(uint64(before.Size())))
	if err := copyToDir(ctx, dbDir, tmpDir, label, growthStepFor(before.Size()), logger); err != nil {
		return err
	}

	// Mode and owner are applied before the rename, the last step that may fail.
	copied := filepath.Join(tmpDir, dataFileName)
	if err := os.Chmod(copied, before.Mode().Perm()); err != nil {
		return err
	}
	if err := restoreOwner(before, copied); err != nil {
		return err
	}
	if err := os.Rename(copied, dataFile); err != nil {
		return err
	}

	// The db is compacted from here on, so nothing below may fail the call: an
	// error would report a successful compaction as failed and make CompactDatadir
	// skip the datadir's remaining databases.
	if err := dir.FsyncDir(dbDir); err != nil {
		logger.Warn("[compact] fsync dir", "db", dbDir, "err", err)
	}
	if err := dir.RemoveFile(filepath.Join(dbDir, lockFileName)); err != nil && !os.IsNotExist(err) {
		logger.Warn("[compact] stale lock file left behind", "db", dbDir, "err", err)
	}
	args := []any{"label", label, "db", dbDir, "before", common.ByteCount(uint64(before.Size()))}
	if after, err := os.Stat(dataFile); err == nil {
		args = append(args, "after", common.ByteCount(uint64(after.Size())))
	} else {
		logger.Warn("[compact] size after compaction unavailable", "db", dbDir, "err", err)
	}
	logger.Info("[compact] compacted", args...)
	return nil
}

// copyToDir closes both databases before it returns: the caller moves the copy,
// which must not happen while mdbx still holds the file open.
func copyToDir(ctx context.Context, from, to string, label kv.Label, growthStep datasize.ByteSize, logger log.Logger) error {
	src, dst, err := openPair(ctx, from, to, label, true, 0, growthStep, nil, logger)
	if err != nil {
		return err
	}
	defer src.Close()
	defer dst.Close()
	return Kv2kv(ctx, src, dst, nil, logger)
}

// tablesOnDisk opens every table the file actually holds and reads back the flags
// mdbx stores per table, so a copy driven by it neither drops a table the current
// schema no longer names nor fails on a schema table the file predates. Only the
// flags carry over: a DBI handle belongs to the env it was opened in.
func tablesOnDisk(ctx context.Context, db kv.RoDB) (kv.TableCfg, error) {
	if err := db.View(ctx, func(tx kv.Tx) error {
		names, err := tx.ListTables()
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := tx.(kv.BucketMigrator).CreateTable(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	tables := kv.TableCfg{}
	for name, cfg := range db.AllTables() {
		tables[name] = kv.TableCfgItem{Flags: cfg.Flags}
	}
	return tables, nil
}

func Kv2kv(ctx context.Context, src kv.RoDB, dst kv.RwDB, tables []string, logger log.Logger) error {
	srcTx, err1 := src.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer srcTx.Rollback()

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
	logger.Info("[db-copy] done", "tablesWithData", copiedTables, "rows", common.PrettyCounter(copiedRows))
	return nil
}

func backupTable(ctx context.Context, src kv.RoDB, srcTx kv.Tx, dst kv.RwDB, table string, logEvery *time.Ticker, logger log.Logger) (uint64, error) {
	t := time.Now()
	srcC, err := srcTx.Cursor(table)
	if err != nil {
		return 0, err
	}
	defer srcC.Close()
	total, err := srcTx.Count(table)
	if err != nil {
		return 0, err
	}
	size, err := srcTx.BucketSize(table)
	if err != nil {
		return 0, err
	}
	if total > 0 {
		logger.Info("[db-copy] copying", "table", table, "rows", common.PrettyCounter(total), "size", common.ByteCount(size))
	}

	// Read-ahead warms pages (values too — the copy reads them) just ahead of the
	// copy cursor. No-op unless WARMUP_TABLE_WORKERS is set.
	var ra *kv.ReadAhead
	if workers := int(dbg.WarmupTableWorkers); workers > 0 && total > 0 {
		var bounds [][]byte
		bounds, _, err = kv.DistributeBounds(srcTx, table)
		if err != nil {
			logger.Warn("[db-copy] read-ahead disabled", "table", table, "err", err)
		} else {
			ra = kv.NewReadAhead(ctx, src, table, kv.ReadAheadCfg{Bounds: bounds, TableSize: size, Workers: workers, WarmValues: true})
		}
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
			if err := casted.AppendDup(k, v); err != nil {
				return 0, err
			}
		} else {
			if err := c.Append(k, v); err != nil {
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

	if err2 := dstTx.Commit(); err2 != nil {
		return 0, err2
	}
	return i, nil
}

// ClearTables empties each table on the caller's open write tx — atomic with the
// caller's other writes, and safe inside an open writer (a self-owned writer
// would deadlock, since MDBX serializes writers). db drives optional read-ahead only.
func ClearTables(ctx context.Context, db kv.RoDB, tx kv.RwTx, tables ...string) error {
	for _, table := range tables {
		if err := clearTable(ctx, db, tx, table); err != nil {
			return fmt.Errorf("clearing %s: %w", table, err)
		}
	}
	return nil
}

func clearTable(ctx context.Context, db kv.RoDB, tx kv.RwTx, table string) error {
	workers := int(dbg.WarmupTableWorkers)
	if workers == 0 { // chunked range-delete only pays off paired with read-ahead
		log.Info("[clear]", "table", table)
		return tx.ClearTable(table)
	}

	dr, ok := tx.(kv.HasDeleteRange)
	if !ok { // backend has no range-delete: drop the whole table
		return tx.ClearTable(table)
	}

	bounds, size, err := kv.DistributeBounds(tx, table)
	if err != nil {
		return err
	}
	log.Info("[clear]", "table", table, "size", common.ByteCount(size))
	if len(bounds) <= 2 { // one chunk ([nil,nil]): native drop beats distribute+warm+whole-table DeleteRange
		return tx.ClearTable(table)
	}

	// read-ahead over the same boundaries; keys-only — range-delete never reads values
	ra := kv.NewReadAhead(ctx, db, table, kv.ReadAheadCfg{Bounds: bounds, TableSize: size, Workers: workers})
	defer ra.Close()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	lastLog := time.Now()
	lastSize := size
	var deleted, lastDeleted uint64
	for i := 0; i+1 < len(bounds); i++ {
		ra.SetPos(bounds[i])
		t := time.Now()
		n, err := dr.DeleteRange(table, bounds[i], bounds[i+1])
		if took := time.Since(t); took > 500*time.Millisecond {
			log.Warn("[clear] delete", "table", table, "took", took)
		}
		if err != nil {
			return err
		}
		deleted += n

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			remaining, err := tx.BucketSize(table)
			if err != nil {
				continue // a failed progress read shouldn't abort the clear
			}
			now := time.Now()
			secs := now.Sub(lastLog).Seconds()
			log.Info("[clear]", "table", table,
				"speed", common.ByteCount(uint64(float64(lastSize-remaining)/secs))+"/s",
				"keys", common.PrettyCounter(uint64(float64(deleted-lastDeleted)/secs))+"/s",
				"remaining", common.ByteCount(remaining),
			)
			lastLog, lastSize, lastDeleted = now, remaining, deleted
		default:
		}
	}
	return nil
}
