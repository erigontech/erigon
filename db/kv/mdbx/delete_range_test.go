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

package mdbx

import (
	"bytes"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
)

const deleteRangeTable = "T"

// newFilledDB returns a WriteMap DB (the production default: freed pages are
// recycled in place, which is what makes stale bounds dangerous) filled with
// keys 0..n-1.
func newFilledDB(t *testing.T, n int) kv.RwDB {
	t.Helper()
	db := New(dbcfg.ChainDB, log.New()).InMem(t, t.TempDir()).WriteMap(true).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{deleteRangeTable: kv.TableCfgItem{}}
	}).MapSize(512 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(deleteRangeTable)
		require.NoError(t, err)
		defer c.Close()
		for i := 0; i < n; i++ {
			require.NoError(t, c.Append(u64tob(uint64(i)), []byte{1}))
		}
		return nil
	}))
	return db
}

func countTable(t *testing.T, db kv.RwDB) uint64 {
	t.Helper()
	var c uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		c, err = tx.Count(deleteRangeTable)
		return err
	}))
	return c
}

func TestMdbxDeleteRange(t *testing.T) {
	deleteRange := func(t *testing.T, db kv.RwDB, from, to []byte) uint64 {
		var n uint64
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			var err error
			n, err = tx.(kv.HasDeleteRange).DeleteRange(deleteRangeTable, from, to)
			return err
		}))
		return n
	}

	t.Run("half-open [from,to)", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.EqualValues(t, 500, deleteRange(t, db, u64tob(200), u64tob(700)))
		require.EqualValues(t, 500, countTable(t, db))
		require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
			has := func(i uint64) bool { v, _ := tx.GetOne(deleteRangeTable, u64tob(i)); return v != nil }
			require.True(t, has(199))  // just below the range: kept
			require.False(t, has(200)) // inclusive lower bound: gone
			require.False(t, has(699)) // just below upper bound: gone
			require.True(t, has(700))  // exclusive upper bound: kept
			return nil
		}))
	})

	t.Run("to==nil deletes through the last key", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.EqualValues(t, 100, deleteRange(t, db, u64tob(900), nil))
		require.EqualValues(t, 900, countTable(t, db))
	})

	t.Run("from==nil deletes from the first key", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.EqualValues(t, 300, deleteRange(t, db, nil, u64tob(300)))
		require.EqualValues(t, 700, countTable(t, db))
	})

	t.Run("from==nil,to==nil clears the whole table", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.EqualValues(t, 1000, deleteRange(t, db, nil, nil))
		require.Zero(t, countTable(t, db))
	})

	t.Run("reversed range deletes nothing", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.Zero(t, deleteRange(t, db, u64tob(700), u64tob(200)))
		require.EqualValues(t, 1000, countTable(t, db))
	})

	t.Run("from past the last key deletes nothing", func(t *testing.T) {
		db := newFilledDB(t, 1000)
		require.Zero(t, deleteRange(t, db, u64tob(5000), nil))
		require.EqualValues(t, 1000, countTable(t, db))
	})
}

// TestChunkedDeleteRangeCoversAllKeys reproduces backup.clearTable's loop:
// distribute the table into many count-balanced chunks, then range-delete each
// chunk in sequence on one write tx. The bounds must be cloned first — a same-tx
// DeleteRange rebalances the b-tree pages the raw mdbx keys point into, and
// under WriteMap those freed pages are recycled and rewritten, so reusing the
// raw bounds would silently skip chunks and leave residual rows.
func TestChunkedDeleteRangeCoversAllKeys(t *testing.T) {
	const n = 200_000
	db := newFilledDB(t, n)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		bounds, err := tx.(kv.DBWithDistributionSupport).DistributeCursors(deleteRangeTable, nil, 512)
		require.NoError(t, err)
		require.Greater(t, len(bounds), 2, "table must split into multiple chunks")
		for i := range bounds { // clone before any DeleteRange mutates the tree
			bounds[i] = bytes.Clone(bounds[i])
		}

		dr := tx.(kv.HasDeleteRange)
		var deleted uint64
		for i := 0; i+1 < len(bounds); i++ {
			m, err := dr.DeleteRange(deleteRangeTable, bounds[i], bounds[i+1])
			require.NoError(t, err)
			deleted += m
		}
		require.EqualValues(t, n, deleted)
		return nil
	}))
	require.Zero(t, countTable(t, db))
}

func newFilledDupSortDB(t *testing.T, keys, dupsPerKey int) kv.RwDB {
	t.Helper()
	db := New(dbcfg.ChainDB, log.New()).InMem(t, t.TempDir()).WriteMap(true).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{deleteRangeTable: kv.TableCfgItem{Flags: kv.DupSort}}
	}).MapSize(512 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursorDupSort(deleteRangeTable)
		require.NoError(t, err)
		defer c.Close()
		for i := 0; i < keys; i++ {
			for d := 0; d < dupsPerKey; d++ {
				require.NoError(t, c.AppendDup(u64tob(uint64(i)), u64tob(uint64(d))))
			}
		}
		return nil
	}))
	return db
}

// TestMdbxDeleteRangeDupSort pins that native range-delete removes every dup of
// each key in [from,to) and counts (key,value) pairs, not distinct keys.
func TestMdbxDeleteRangeDupSort(t *testing.T) {
	const keys, dups = 1000, 8
	db := newFilledDupSortDB(t, keys, dups)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		n, err := tx.(kv.HasDeleteRange).DeleteRange(deleteRangeTable, u64tob(200), u64tob(700))
		require.NoError(t, err)
		require.EqualValues(t, 500*dups, n) // every dup of keys 200..699
		return nil
	}))
	require.EqualValues(t, 500*dups, countTable(t, db))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		has := func(i uint64) bool { v, _ := tx.GetOne(deleteRangeTable, u64tob(i)); return v != nil }
		require.True(t, has(199))  // dups kept
		require.False(t, has(200)) // all dups gone
		require.False(t, has(699))
		require.True(t, has(700))
		return nil
	}))
}

// TestChunkedDeleteRangeDupSortCoversAllKeys is the DupSort analogue of
// TestChunkedDeleteRangeCoversAllKeys: chunked range-delete over count-balanced
// bounds must remove every (key,dup) with no gaps — the shape ResetExec clears.
func TestChunkedDeleteRangeDupSortCoversAllKeys(t *testing.T) {
	const keys, dups = 50_000, 8
	db := newFilledDupSortDB(t, keys, dups)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		bounds, err := tx.(kv.DBWithDistributionSupport).DistributeCursors(deleteRangeTable, nil, 256)
		require.NoError(t, err)
		require.Greater(t, len(bounds), 2, "dupsort table must split into multiple chunks")
		for i := range bounds {
			bounds[i] = bytes.Clone(bounds[i])
		}
		dr := tx.(kv.HasDeleteRange)
		var deleted uint64
		for i := 0; i+1 < len(bounds); i++ {
			m, err := dr.DeleteRange(deleteRangeTable, bounds[i], bounds[i+1])
			require.NoError(t, err)
			deleted += m
		}
		require.EqualValues(t, keys*dups, deleted)
		return nil
	}))
	require.Zero(t, countTable(t, db))
}
