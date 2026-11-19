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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

func TestDupSortPointReadsSeeOverlayOnlyValue(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key2"), []byte("value2.1")))

	v, err := batch.GetOne(kv.TblAccountVals, []byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2.1"), v)
	has, err := batch.Has(kv.TblAccountVals, []byte("key2"))
	require.NoError(t, err)
	require.True(t, has)
}

func TestDupSortPointReadsSkipDeletedDbValue(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	c, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1")))

	v, err := batch.GetOne(kv.TblAccountVals, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)

	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.3")))
	has, err := batch.Has(kv.TblAccountVals, []byte("key1"))
	require.NoError(t, err)
	require.False(t, has)
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

func TestRangeDescBoundsMatchDb(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("CABA"), []byte("mem1")))
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("CBBA"), []byte("mem2")))

	it, err := batch.Range(kv.HeaderNumber, []byte("CCAA"), []byte("CAAA"), order.Desc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)
	require.Equal(t, []string{"CCAA", "CBBA", "CBAA", "CABA"}, keys)

	it, err = batch.Range(kv.HeaderNumber, []byte("CB"), []byte("CA"), order.Desc, kv.Unlim)
	require.NoError(t, err)
	keys, _ = collectStream(t, it)
	require.Equal(t, []string{"CBBA", "CBAA", "CABA", "CAAA"}, keys)
}

func TestRangeDupSortDescBoundsMatchDb(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.2")))

	it, err := batch.RangeDupSort(kv.TblAccountVals, []byte("key1"), []byte("value1.3"), []byte("value1.1"), order.Desc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)
	require.Equal(t, []string{"value1.3", "value1.2"}, values)
}
