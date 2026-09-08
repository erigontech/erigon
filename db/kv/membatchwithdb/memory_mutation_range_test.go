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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

func newBatchOverDbNonDupSort(tb testing.TB) *membatchwithdb.MemoryMutation {
	tb.Helper()
	_, rwTx := newTestTx(tb)
	initializeDbNonDupSort(tb, rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(tb, err)
	tb.Cleanup(batch.Close)
	return batch
}

func collectStream(tb testing.TB, it stream.KV) (keys, values []string) {
	tb.Helper()
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(tb, err)
		keys = append(keys, string(k))
		values = append(values, string(v))
	}
	return keys, values
}

// The db side is exhausted on the same Next that still has greater mem keys to
// serve: the db key read in that call must not be swallowed by the mem key.
func TestRangeAscKeepsLastDbKey(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("DAAA"), []byte("value4")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, values := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CAAA", "CBAA", "CCAA", "DAAA"}, keys)
	require.Equal(t, []string{"value", "value1", "value2", "value3", "value4"}, values)
}

func TestRangeDescKeepsLastDbKey(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("0AAA"), []byte("value0")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Desc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"CCAA", "CBAA", "CAAA", "AAAA", "0AAA"}, keys)
}

func TestRangeDupSortKeepsLastDbValue(t *testing.T) {
	_, rwTx := newTestTx(t)
	initializeDbDupSort(t, rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.5")))

	it, err := batch.RangeDupSort(kv.TblAccountVals, []byte("key1"), nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Equal(t, []string{"value1.1", "value1.3", "value1.5"}, values)
}

func TestRangeSkipsDeletedDbEntry(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CBAA")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CAAA", "CCAA"}, keys)
}

func TestRangeServesDeletedThenRewrittenEntry(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CBAA")))
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("CBAA"), []byte("value2.new")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, values := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value", "value1", "value2.new", "value3"}, values)
}

func TestRangeHidesClearedTable(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.ClearTable(kv.HeaderNumber))
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("ZZZZ"), []byte("value9")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"ZZZZ"}, keys)
}

func TestPrefixSkipsDeletedDbEntry(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CBAA")))

	it, err := batch.Prefix(kv.HeaderNumber, []byte("C"))
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"CAAA", "CCAA"}, keys)
}

func TestRangeOnReadViewSkipsDeletedDbEntry(t *testing.T) {
	_, rwTx := newTestTx(t)
	initializeDbNonDupSort(t, rwTx)
	overlay, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer overlay.Close()
	require.NoError(t, overlay.Delete(kv.HeaderNumber, []byte("CBAA")))

	view := overlay.NewReadView(rwTx)
	it, err := view.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CAAA", "CCAA"}, keys)
}

func TestRangeDupSortSkipsDeletedDbKey(t *testing.T) {
	_, rwTx := newTestTx(t)
	initializeDbDupSort(t, rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	require.NoError(t, batch.Delete(kv.TblAccountVals, []byte("key1")))

	it, err := batch.RangeDupSort(kv.TblAccountVals, []byte("key1"), nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Empty(t, values)
}

// A bounded limit must count non-deleted rows: the db side has to look past
// the rows the overlay hides, otherwise the stream ends short.
func TestRangeWithLimitCountsNonDeletedRows(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CAAA")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Asc, 3)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CBAA", "CCAA"}, keys)
}

func TestRangeDupSortWithLimitCountsNonDeletedValues(t *testing.T) {
	_, rwTx := newTestTx(t)
	initializeDbDupSort(t, rwTx)
	require.NoError(t, rwTx.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.5")))
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	c, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1")))

	it, err := batch.RangeDupSort(kv.TblAccountVals, []byte("key1"), nil, nil, order.Asc, 2)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Equal(t, []string{"value1.3", "value1.5"}, values)
}
