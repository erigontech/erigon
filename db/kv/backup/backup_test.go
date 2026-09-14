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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

const testTable = "T"

func u64Key(i uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, i)
	return k
}

// newWriteMapDB matches the production default (--db.writemap=true), where a
// range-delete's freed pages are recycled and rewritten in place — the setting
// under which reusing stale chunk bounds would silently skip rows.
func newWriteMapDB(t *testing.T) kv.RwDB {
	t.Helper()
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), t.TempDir()).WriteMap(true).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{testTable: kv.TableCfgItem{}}
	}).MapSize(1 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	return db
}

func tableSize(t *testing.T, db kv.RwDB) uint64 {
	t.Helper()
	var sz uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		sz, err = tx.BucketSize(testTable)
		return err
	}))
	return sz
}

func tableCount(t *testing.T, db kv.RwDB) uint64 {
	t.Helper()
	var c uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		c, err = tx.Count(testTable)
		return err
	}))
	return c
}

func withWarmupWorkers(t *testing.T, n uint64) {
	t.Helper()
	prev := dbg.WarmupTableWorkers
	dbg.WarmupTableWorkers = n
	t.Cleanup(func() { dbg.WarmupTableWorkers = prev })
}

func TestClearTablesWarmupOff(t *testing.T) {
	withWarmupWorkers(t, 0) // default: plain one-shot clear, no chunking

	db := newWriteMapDB(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(testTable)
		require.NoError(t, err)
		defer c.Close()
		for i := range 1000 {
			require.NoError(t, c.Append(u64Key(uint64(i)), []byte{1}))
		}
		return nil
	}))

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return ClearTables(t.Context(), db, tx, testTable)
	}))
	require.Zero(t, tableCount(t, db))
}

// TestClearTablesWarmupOnSmallTable exercises the warmup-on path for a table
// under one chunk: it must skip distribution/read-ahead and fall back to the
// native drop, still leaving the table empty.
func TestClearTablesWarmupOnSmallTable(t *testing.T) {
	withWarmupWorkers(t, 4)

	db := newWriteMapDB(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(testTable)
		require.NoError(t, err)
		defer c.Close()
		for i := range 1000 {
			require.NoError(t, c.Append(u64Key(uint64(i)), []byte{1}))
		}
		return nil
	}))
	require.Less(t, tableSize(t, db), 32*datasize.MB.Bytes(), "table must be under one chunk")

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return ClearTables(t.Context(), db, tx, testTable)
	}))
	require.Zero(t, tableCount(t, db))
}

// TestClearTablesMultiChunkWriteMap clears a table large enough to split into
// several 32MB chunks with warmup enabled, so ClearTables walks its full chunked
// range-delete path. Every row must be gone — a stale-bounds regression would
// skip chunks and leave residual rows behind.
func TestClearTablesMultiChunkWriteMap(t *testing.T) {
	withWarmupWorkers(t, 4)

	db := newWriteMapDB(t)

	// Fill past 64MB (>= 2 chunks) in batches; monotonic keys keep Append valid.
	val := make([]byte, 2048)
	next := uint64(0)
	for tableSize(t, db) <= 64*datasize.MB.Bytes() {
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			c, err := tx.RwCursor(testTable)
			require.NoError(t, err)
			defer c.Close()
			for range 20_000 {
				require.NoError(t, c.Append(u64Key(next), val))
				next++
			}
			return nil
		}))
	}
	require.Greater(t, tableSize(t, db), 64*datasize.MB.Bytes(), "table must be multi-chunk")

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return ClearTables(t.Context(), db, tx, testTable)
	}))
	require.Zero(t, tableCount(t, db))
}

const dupTestTable = "TD"

func dataFileStat(t *testing.T, dbDir string) os.FileInfo {
	t.Helper()
	st, err := os.Stat(filepath.Join(dbDir, dataFileName))
	require.NoError(t, err)
	// Windows reads the file id lazily, from whatever file is at the path by then.
	require.True(t, os.SameFile(st, st))
	return st
}

const (
	testRows = 20_000
	testDups = 3 // >1, so a lost DupSort flag can't be copied into a plain table
)

var testVal = make([]byte, 1024)

func openTestDB(dbDir string) kv.RwDB {
	return mdbx.New(dbcfg.ChainDB, log.New()).Path(dbDir).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{testTable: {}, dupTestTable: {Flags: kv.DupSort}}
		}).
		GrowthStep(4 * datasize.MB).MapSize(1 * datasize.GB).WriteMap(true).MustOpen()
}

// writeTestDB fills a db at dbDir with testRows rows, then deletes the first deleted of them.
func writeTestDB(t *testing.T, dbDir string, deleted int) {
	t.Helper()
	const batch = 2_000
	require.NoError(t, os.MkdirAll(dbDir, 0755))
	db := openTestDB(dbDir)
	defer db.Close()
	for from := 0; from < testRows; from += batch {
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			c, err := tx.RwCursor(testTable)
			require.NoError(t, err)
			defer c.Close()
			d, err := tx.RwCursorDupSort(dupTestTable)
			require.NoError(t, err)
			defer d.Close()
			for i := from; i < from+batch; i++ {
				require.NoError(t, c.Append(u64Key(uint64(i)), testVal))
				for j := range testDups {
					require.NoError(t, d.AppendDup(u64Key(uint64(i)), u64Key(uint64(i*testDups+j))))
				}
			}
			return nil
		}))
	}
	for from := 0; from < deleted; from += batch {
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			for i := from; i < from+batch; i++ {
				require.NoError(t, tx.Delete(testTable, u64Key(uint64(i))))
				require.NoError(t, tx.Delete(dupTestTable, u64Key(uint64(i))))
			}
			return nil
		}))
	}
}

// TestCompactInPlace pins the swap: compaction must leave the db openable at the
// same path with every row intact — including tables the label's schema doesn't
// name — and must give back the pages the deletes freed.
func TestCompactInPlace(t *testing.T) {
	const deleted = 18_000
	dbDir := filepath.Join(t.TempDir(), "chaindata")
	writeTestDB(t, dbDir, deleted)

	dataFile := filepath.Join(dbDir, dataFileName)
	require.NoError(t, os.Chmod(dataFile, 0600))
	before := dataFileStat(t, dbDir)

	require.NoError(t, CompactInPlace(t.Context(), dbDir, dbcfg.ChainDB, log.New()))

	after := dataFileStat(t, dbDir)
	require.Less(t, after.Size(), before.Size())
	require.Equal(t, before.Mode().Perm(), after.Mode().Perm())

	db := openTestDB(dbDir)
	defer db.Close()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		n, err := tx.Count(testTable)
		require.NoError(t, err)
		require.Equal(t, uint64(testRows-deleted), n)

		nd, err := tx.Count(dupTestTable)
		require.NoError(t, err)
		require.Equal(t, uint64((testRows-deleted)*testDups), nd)

		v, err := tx.GetOne(testTable, u64Key(deleted))
		require.NoError(t, err)
		require.Len(t, v, len(testVal))

		d, err := tx.CursorDupSort(dupTestTable)
		require.NoError(t, err)
		defer d.Close()
		_, _, err = d.SeekExact(u64Key(deleted))
		require.NoError(t, err)
		nDups, err := d.CountDuplicates()
		require.NoError(t, err)
		require.Equal(t, uint64(testDups), nDups)
		for j := range testDups {
			got, err := d.SeekBothRange(u64Key(deleted), u64Key(uint64(deleted*testDups+j)))
			require.NoError(t, err)
			require.Equal(t, u64Key(uint64(deleted*testDups+j)), got)
		}
		return nil
	}))
}

// TestAutoCompactDatadir pins the threshold: only a db whose free pages exceed
// bloatRatio times its data is rewritten.
func TestAutoCompactDatadir(t *testing.T) {
	withAutoCompactMinFree(t, 0)
	dirs := datadir.New(t.TempDir())
	writeTestDB(t, dirs.Chaindata, 18_000)
	writeTestDB(t, dirs.TxPool, 2_000)
	bloated, healthy := dataFileStat(t, dirs.Chaindata), dataFileStat(t, dirs.TxPool)

	require.NoError(t, ApplyMigrations(t.Context(), dirs, log.New()))

	require.Less(t, dataFileStat(t, dirs.Chaindata).Size(), bloated.Size())
	require.True(t, os.SameFile(healthy, dataFileStat(t, dirs.TxPool)), "a healthy db must not be rewritten")
}

// TestAutoCompactDatadirSkipsLockedDatadir: integration opens a datadir while the
// node that holds its lock keeps running, so the lock owner's dbs must stay untouched.
func TestAutoCompactDatadirSkipsLockedDatadir(t *testing.T) {
	withAutoCompactMinFree(t, 0)
	dirs := datadir.New(t.TempDir())
	writeTestDB(t, dirs.Chaindata, 18_000)
	before := dataFileStat(t, dirs.Chaindata)

	unlock, err := dirs.TryFlock()
	require.NoError(t, err)
	defer unlock()
	require.NoError(t, ApplyMigrations(t.Context(), dirs, log.New()))

	require.True(t, os.SameFile(before, dataFileStat(t, dirs.Chaindata)))
}

func withAutoCompactMinFree(t *testing.T, v uint64) {
	t.Helper()
	prev := autoCompactMinFree
	autoCompactMinFree = v
	t.Cleanup(func() { autoCompactMinFree = prev })
}

// TestAutoCompactDatadirSkipsLittleFreeSpace: a small db crosses bloatRatio with
// a few free pages, and its rewrite gives back nothing the growth step keeps.
func TestAutoCompactDatadirSkipsLittleFreeSpace(t *testing.T) {
	withAutoCompactMinFree(t, 1<<30)
	dirs := datadir.New(t.TempDir())
	writeTestDB(t, dirs.Chaindata, 18_000)
	before := dataFileStat(t, dirs.Chaindata)

	require.NoError(t, ApplyMigrations(t.Context(), dirs, log.New()))

	require.True(t, os.SameFile(before, dataFileStat(t, dirs.Chaindata)))
}

// TestDatadirDBs pins the three rules of the datadir scan: a db is found by its
// mdbx.dat, the label comes from the root it sits under, and the walk reaches
// caplin/blobs/chaindata.
func TestDatadirDBs(t *testing.T) {
	root := t.TempDir()
	mkDB := func(parts ...string) string {
		p := filepath.Join(append([]string{root}, parts...)...)
		require.NoError(t, os.MkdirAll(p, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(p, dataFileName), nil, 0644))
		return p
	}
	chaindata := mkDB("chaindata")
	txpool := mkDB("txpool")
	nodes := mkDB("nodes", "eth68")
	blobs := mkDB("caplin", "blobs", "chaindata")
	indexing := mkDB("caplin", "indexing")

	// A staging dir lives inside a db whose own mdbx.dat stops the walk above it.
	mkDB("chaindata", compactDirName)

	found, err := datadirDBs(datadir.Open(root))
	require.NoError(t, err)

	got := map[string]kv.Label{}
	for _, db := range found {
		got[db.path] = db.label
	}
	require.Len(t, found, len(got), "a db must be reported once")
	require.Equal(t, map[string]kv.Label{
		chaindata: dbcfg.ChainDB,
		txpool:    dbcfg.TxPoolDB,
		nodes:     dbcfg.SentryDB,
		blobs:     dbcfg.CaplinDB,
		indexing:  dbcfg.CaplinDB,
	}, got)
}
