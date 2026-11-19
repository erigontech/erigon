// Copyright 2026 The Erigon Authors
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

package membatchwithdb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

func cursorRows(tb testing.TB, c kv.Cursor, k, v []byte, err error) []string {
	tb.Helper()
	var rows []string
	for ; k != nil && err == nil; k, v, err = c.Next() {
		rows = append(rows, string(k)+"="+string(v))
	}
	require.NoError(tb, err)
	return rows
}

func newBatchWithTrailingTombstones(tb testing.TB, rows, tombstones int) *membatchwithdb.MemoryMutation {
	tb.Helper()
	_, tx := newTestTx(tb)
	for i := range rows {
		require.NoError(tb, tx.Put(kv.Headers, dbutils.HeaderKey(uint64(i+1), common.Hash{}), []byte("db")))
	}
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(tb, err)
	tb.Cleanup(batch.Close)
	for i := range tombstones {
		require.NoError(tb, batch.Delete(kv.Headers, dbutils.HeaderKey(uint64(rows+i+1), common.Hash{})))
	}
	return batch
}

// Tombstones that sort after the db rows being served must stay unvisited.
func TestCursorLeavesTrailingTombstonesUnvisited(t *testing.T) {
	const rows = 200
	batch := newBatchWithTrailingTombstones(t, rows, 10_000)
	for name, position := range map[string]func(kv.Cursor) ([]byte, []byte, error){
		"First": kv.Cursor.First,
		"Seek":  func(c kv.Cursor) ([]byte, []byte, error) { return c.Seek(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			c, err := batch.Cursor(kv.Headers)
			require.NoError(t, err)
			defer c.Close()
			k, _, err := position(c)
			for range rows - 1 {
				require.NoError(t, err)
				k, _, err = c.Next()
			}
			require.NoError(t, err)
			require.Equal(t, dbutils.HeaderKey(rows, common.Hash{}), k)
			memK, valid := membatchwithdb.MemCursorPosition(c)
			require.True(t, valid)
			require.Equal(t, dbutils.HeaderKey(rows+1, common.Hash{}), memK)
		})
	}
}

func TestCursorSeekBothRangeLeavesTombstonesOfLaterKeysUnvisited(t *testing.T) {
	_, tx := newTestTx(t)
	require.NoError(t, tx.Put(kv.TblAccountVals, []byte("k1"), []byte("v1")))
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	defer batch.Close()
	for i := range 1_000 {
		require.NoError(t, batch.Delete(kv.TblAccountVals, fmt.Appendf(nil, "k2-%04d", i)))
	}
	c, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	v, err := c.SeekBothRange([]byte("k1"), nil)
	require.NoError(t, err)
	require.Equal(t, "v1", string(v))
	memK, valid := membatchwithdb.MemCursorPosition(c)
	require.True(t, valid)
	require.Equal(t, "k2-0000", string(memK))
}

func TestCursorReachesLiveEntryAfterTombstones(t *testing.T) {
	const rows, tombstones = 200, 10_000
	batch := newBatchWithTrailingTombstones(t, rows, tombstones)
	last := dbutils.HeaderKey(rows+tombstones+1, common.Hash{})
	require.NoError(t, batch.Put(kv.Headers, last, []byte("mem")))
	c, err := batch.Cursor(kv.Headers)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.First()
	got := cursorRows(t, c, k, v, err)
	require.Len(t, got, rows+1)
	require.Equal(t, string(last)+"=mem", got[rows])
}

func newBatchWithPlainTombstones(t *testing.T) *membatchwithdb.MemoryMutation {
	t.Helper()
	_, tx := newTestTx(t)
	for _, k := range []string{"a", "c", "e", "g"} {
		require.NoError(t, tx.Put(kv.HeaderNumber, []byte(k), []byte("db")))
	}
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	t.Cleanup(batch.Close)
	for _, k := range []string{"b", "c", "d", "h", "j"} {
		require.NoError(t, batch.Delete(kv.HeaderNumber, []byte(k)))
	}
	for _, k := range []string{"f", "i"} {
		require.NoError(t, batch.Put(kv.HeaderNumber, []byte(k), []byte("mem")))
	}
	return batch
}

func TestCursorSkipsTombstonesPlain(t *testing.T) {
	batch := newBatchWithPlainTombstones(t)
	c, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer c.Close()

	k, v, err := c.First()
	require.Equal(t, []string{"a=db", "e=db", "f=mem", "g=db", "i=mem"}, cursorRows(t, c, k, v, err))
	for seek, want := range map[string][]string{
		"b": {"e=db", "f=mem", "g=db", "i=mem"},
		"c": {"e=db", "f=mem", "g=db", "i=mem"},
		"d": {"e=db", "f=mem", "g=db", "i=mem"},
		"h": {"i=mem"},
		"j": nil,
	} {
		k, v, err = c.Seek([]byte(seek))
		require.Equal(t, want, cursorRows(t, c, k, v, err), "seek %s", seek)
	}
}

func TestCursorMixesDirectionsOverTombstones(t *testing.T) {
	batch := newBatchWithPlainTombstones(t)
	c, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer c.Close()

	k, _, err := c.Seek([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, "e", string(k))
	k, _, err = c.Last()
	require.NoError(t, err)
	require.Equal(t, "i", string(k))
	k, _, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	k, v, err := c.First()
	require.Equal(t, []string{"a=db", "e=db", "f=mem", "g=db", "i=mem"}, cursorRows(t, c, k, v, err))
	k, _, err = c.Last()
	require.NoError(t, err)
	require.Equal(t, "i", string(k))
	k, v, err = c.Seek([]byte("d"))
	require.Equal(t, []string{"e=db", "f=mem", "g=db", "i=mem"}, cursorRows(t, c, k, v, err))
}

func newBatchWithDupSortTombstones(t *testing.T) *membatchwithdb.MemoryMutation {
	t.Helper()
	_, tx := newTestTx(t)
	for _, kv2 := range [][2]string{{"k1", "v1"}, {"k1", "v3"}, {"k2", "v1"}, {"k2", "v2"}, {"k3", "v1"}, {"k4", "v1"}} {
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte(kv2[0]), []byte(kv2[1])))
	}
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	t.Cleanup(batch.Close)
	c, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	for _, kv2 := range [][2]string{{"k1", "v1"}, {"k1", "v4"}, {"k3", "v0"}, {"k5", "v1"}} {
		require.NoError(t, c.DeleteExact([]byte(kv2[0]), []byte(kv2[1])))
	}
	require.NoError(t, batch.Delete(kv.TblAccountVals, []byte("k2")))
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("k1"), []byte("v2")))
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("k3"), []byte("v2")))
	return batch
}

func TestCursorSkipsTombstonesDupSort(t *testing.T) {
	batch := newBatchWithDupSortTombstones(t)
	c, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()

	all := []string{"k1=v2", "k1=v3", "k3=v1", "k3=v2", "k4=v1"}
	k, v, err := c.First()
	require.Equal(t, all, cursorRows(t, c, k, v, err))
	k, v, err = c.Seek([]byte("k2"))
	require.Equal(t, all[2:], cursorRows(t, c, k, v, err))

	_, _, err = c.First()
	require.NoError(t, err)
	k, v, err = c.NextDup()
	require.NoError(t, err)
	require.Equal(t, "k1=v3", string(k)+"="+string(v))
	k, _, err = c.NextDup()
	require.NoError(t, err)
	require.Nil(t, k)

	for _, tc := range []struct{ key, value, want string }{
		{"k1", "v0", "v2"},
		{"k1", "v4", ""},
		{"k2", "", ""},
		{"k3", "v0", "v1"},
	} {
		v, err = c.SeekBothRange([]byte(tc.key), []byte(tc.value))
		require.NoError(t, err)
		require.Equal(t, tc.want, string(v), "SeekBothRange(%s, %s)", tc.key, tc.value)
	}
	_, v, err = c.SeekBothExact([]byte("k1"), []byte("v1"))
	require.NoError(t, err)
	require.Nil(t, v)
	_, v, err = c.SeekBothExact([]byte("k3"), []byte("v2"))
	require.NoError(t, err)
	require.Equal(t, "v2", string(v))
}

func TestCursorNextNoDupSkipsTombstones(t *testing.T) {
	_, tx := newTestTx(t)
	for _, k := range []string{"k1", "k4"} {
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte(k), []byte("v1")))
	}
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	defer batch.Close()
	for _, k := range []string{"k0", "k3", "k5"} {
		require.NoError(t, batch.Delete(kv.TblAccountVals, []byte(k)))
	}
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("k2"), []byte("v1")))
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("k6"), []byte("v1")))
	c, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()

	k, _, err := c.First()
	require.NoError(t, err)
	require.Equal(t, "k1", string(k))
	for _, want := range []string{"k2", "k4", "k6", ""} {
		k, _, err = c.NextNoDup()
		require.NoError(t, err)
		require.Equal(t, want, string(k))
	}
}

func TestCursorNextDupStaysOnKey(t *testing.T) {
	t.Run("db side ends", func(t *testing.T) {
		batch := newBatchOverDbDupSort(t)
		require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key2"), []byte("value2.1")))
		c, err := batch.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		defer c.Close()
		k, v, err := c.SeekExact([]byte("key1"))
		require.NoError(t, err)
		require.Equal(t, "key1=value1.1", string(k)+"="+string(v))
		k, v, err = c.NextDup()
		require.NoError(t, err)
		require.Equal(t, "key1=value1.3", string(k)+"="+string(v))
		k, v, err = c.NextDup()
		require.NoError(t, err)
		require.Nil(t, k, "got %s=%s", k, v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, "key2=value2.1", string(k)+"="+string(v))
	})
	t.Run("mem side ends", func(t *testing.T) {
		batch := newBatchOverDbDupSort(t)
		require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key2"), []byte("value2.1")))
		c, err := batch.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		defer c.Close()
		k, v, err := c.SeekExact([]byte("key2"))
		require.NoError(t, err)
		require.Equal(t, "key2=value2.1", string(k)+"="+string(v))
		k, v, err = c.NextDup()
		require.NoError(t, err)
		require.Nil(t, k, "got %s=%s", k, v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, "key3=value3.1", string(k)+"="+string(v))
	})
	t.Run("both sides", func(t *testing.T) {
		batch := newBatchOverDbDupSort(t)
		require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.5")))
		c, err := batch.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		defer c.Close()
		var rows []string
		k, v, err := c.SeekExact([]byte("key1"))
		for ; k != nil; k, v, err = c.NextDup() {
			require.NoError(t, err)
			rows = append(rows, string(k)+"="+string(v))
		}
		require.NoError(t, err)
		require.Equal(t, []string{"key1=value1.1", "key1=value1.3", "key1=value1.5"}, rows)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, "key3=value3.1", string(k)+"="+string(v))
	})
}

func TestCursorNextNoDupLeavesKey(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.2")))
	c, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.First()
	require.NoError(t, err)
	require.Equal(t, "key1=value1.1", string(k)+"="+string(v))
	k, v, err = c.NextNoDup()
	require.NoError(t, err)
	require.Equal(t, "key3=value3.1", string(k)+"="+string(v))
}

func TestCursorNextAfterNextDupOverNestedOverlay(t *testing.T) {
	_, tx := newTestTx(t)
	require.NoError(t, tx.Put(kv.TblAccountVals, []byte("A"), []byte("1")))
	require.NoError(t, tx.Put(kv.TblAccountVals, []byte("C"), []byte("1")))
	parent, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, parent.Put(kv.TblAccountVals, []byte("B"), []byte("1")))
	child, err := membatchwithdb.NewMemoryBatch(parent)
	require.NoError(t, err)
	defer child.Close()

	c, err := child.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.First()
	require.NoError(t, err)
	require.Equal(t, "A=1", string(k)+"="+string(v))
	k, _, err = c.NextDup()
	require.NoError(t, err)
	require.Nil(t, k)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, "B=1", string(k)+"="+string(v))
}

func TestCursorNextNoDupKeepsLaterDupsOfNextKey(t *testing.T) {
	_, tx := newTestTx(t)
	for _, kv2 := range [][2]string{{"A", "1"}, {"B", "1"}, {"B", "2"}, {"C", "1"}} {
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte(kv2[0]), []byte(kv2[1])))
	}
	batch, err := membatchwithdb.NewMemoryBatch(tx)
	require.NoError(t, err)
	defer batch.Close()
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("A"), []byte("0")))
	c, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("B"), []byte("1")))

	k, v, err := c.First()
	require.NoError(t, err)
	require.Equal(t, "A=0", string(k)+"="+string(v))
	k, v, err = c.NextNoDup()
	require.NoError(t, err)
	require.Equal(t, "B=2", string(k)+"="+string(v))
}

func TestCursorNextDupAfterSeekBothExactStaysOnKey(t *testing.T) {
	t.Run("db hit", func(t *testing.T) {
		_, tx := newTestTx(t)
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte("A"), []byte("1")))
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte("C"), []byte("1")))
		batch, err := membatchwithdb.NewMemoryBatch(tx)
		require.NoError(t, err)
		defer batch.Close()
		c, err := batch.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		defer c.Close()
		k, _, err := c.SeekBothExact([]byte("A"), []byte("1"))
		require.NoError(t, err)
		require.Equal(t, "A", string(k))
		k, v, err := c.NextDup()
		require.NoError(t, err)
		require.Nil(t, k, "got %s=%s", k, v)
	})
	t.Run("mem hit", func(t *testing.T) {
		_, tx := newTestTx(t)
		require.NoError(t, tx.Put(kv.TblAccountVals, []byte("C"), []byte("1")))
		batch, err := membatchwithdb.NewMemoryBatch(tx)
		require.NoError(t, err)
		defer batch.Close()
		require.NoError(t, batch.Put(kv.TblAccountVals, []byte("A"), []byte("1")))
		require.NoError(t, batch.Put(kv.TblAccountVals, []byte("B"), []byte("1")))
		c, err := batch.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		defer c.Close()
		k, _, err := c.SeekBothExact([]byte("A"), []byte("1"))
		require.NoError(t, err)
		require.Equal(t, "A", string(k))
		k, v, err := c.NextDup()
		require.NoError(t, err)
		require.Nil(t, k, "got %s=%s", k, v)
	})
}
