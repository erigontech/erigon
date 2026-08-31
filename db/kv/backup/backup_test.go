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

func mdbxFileSize(t *testing.T, dbDir string) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(dbDir, "mdbx.dat"))
	require.NoError(t, err)
	return st.Size()
}

// TestCompactInPlace pins the swap: compaction must leave the db openable at the
// same path with every row intact — including tables the label's schema doesn't
// name — and must give back the pages the deletes freed.
func TestCompactInPlace(t *testing.T) {
	const (
		rows    = 20_000
		deleted = 18_000
		batch   = 2_000
		dups    = 3 // >1, so a lost DupSort flag can't be copied into a plain table
	)
	dbDir := filepath.Join(t.TempDir(), "chaindata")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	open := func() kv.RwDB {
		return mdbx.New(dbcfg.ChainDB, log.New()).Path(dbDir).
			WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
				return kv.TableCfg{testTable: {}, dupTestTable: {Flags: kv.DupSort}}
			}).
			GrowthStep(4 * datasize.MB).MapSize(1 * datasize.GB).WriteMap(true).MustOpen()
	}

	db := open()
	val := make([]byte, 1024)
	for from := 0; from < rows; from += batch {
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			c, err := tx.RwCursor(testTable)
			require.NoError(t, err)
			defer c.Close()
			d, err := tx.RwCursorDupSort(dupTestTable)
			require.NoError(t, err)
			defer d.Close()
			for i := from; i < from+batch; i++ {
				require.NoError(t, c.Append(u64Key(uint64(i)), val))
				for j := range dups {
					require.NoError(t, d.AppendDup(u64Key(uint64(i)), u64Key(uint64(i*dups+j))))
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
	db.Close()

	before := mdbxFileSize(t, dbDir)
	require.NoError(t, CompactInPlace(t.Context(), dbDir, dbcfg.ChainDB, log.New()))
	require.Less(t, mdbxFileSize(t, dbDir), before)

	db = open()
	defer db.Close()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		n, err := tx.Count(testTable)
		require.NoError(t, err)
		require.Equal(t, uint64(rows-deleted), n)

		nd, err := tx.Count(dupTestTable)
		require.NoError(t, err)
		require.Equal(t, uint64((rows-deleted)*dups), nd)

		v, err := tx.GetOne(testTable, u64Key(deleted))
		require.NoError(t, err)
		require.Len(t, v, len(val))

		d, err := tx.CursorDupSort(dupTestTable)
		require.NoError(t, err)
		defer d.Close()
		_, _, err = d.SeekExact(u64Key(deleted))
		require.NoError(t, err)
		nDups, err := d.CountDuplicates()
		require.NoError(t, err)
		require.Equal(t, uint64(dups), nDups)
		for j := range dups {
			got, err := d.SeekBothRange(u64Key(deleted), u64Key(uint64(deleted*dups+j)))
			require.NoError(t, err)
			require.Equal(t, u64Key(uint64(deleted*dups+j)), got)
		}
		return nil
	}))
}
