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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
)

func newBatchOverDbNonDupSort(tb testing.TB) *membatchwithdb.MemoryMutation {
	tb.Helper()
	_, rwTx := newTestTx(tb)
	initializeDbNonDupSort(tb, rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx)
	require.NoError(tb, err)
	tb.Cleanup(batch.Close)
	return batch
}

func newBatchOverDbDupSort(tb testing.TB) *membatchwithdb.MemoryMutation {
	tb.Helper()
	_, rwTx := newTestTx(tb)
	initializeDbDupSort(tb, rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx)
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

func TestRangeDescWithLimitCountsNonDeletedRows(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CCAA")))

	it, err := batch.Range(kv.HeaderNumber, nil, nil, order.Desc, 2)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"CBAA", "CAAA"}, keys)
}

func TestRangeDupSortKeepsLastDbValue(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
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

func TestRangeOverlayOverridesDbValueOnSameKey(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
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
	overlay, err := membatchwithdb.NewMemoryBatch(rwTx)
	require.NoError(t, err)
	defer overlay.Close()
	require.NoError(t, overlay.Delete(kv.HeaderNumber, []byte("CBAA")))

	view := overlay.NewReadView(rwTx)
	it, err := view.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, _ := collectStream(t, it)

	require.Equal(t, []string{"AAAA", "CAAA", "CCAA"}, keys)
}

func TestRangeOnReadViewIsSafeAgainstConcurrentDelete(t *testing.T) {
	_, rwTx := newTestTx(t)
	initializeDbNonDupSort(t, rwTx)
	overlay, err := membatchwithdb.NewMemoryBatch(rwTx)
	require.NoError(t, err)
	defer overlay.Close()
	require.NoError(t, overlay.Delete(kv.HeaderNumber, []byte("CBAA")))

	view := overlay.NewReadView(rwTx)

	const rounds = 200
	var wg sync.WaitGroup
	var writeErr, readErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range rounds {
			if err := overlay.Delete(kv.HeaderNumber, []byte(fmt.Sprintf("E%03d", i))); err != nil {
				writeErr = err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range rounds {
			it, err := view.Range(kv.HeaderNumber, nil, nil, order.Asc, kv.Unlim)
			if err != nil {
				readErr = err
				return
			}
			for it.HasNext() {
				if _, _, err := it.Next(); err != nil {
					it.Close()
					readErr = err
					return
				}
			}
			it.Close()
		}
	}()
	wg.Wait()

	require.NoError(t, writeErr)
	require.NoError(t, readErr)
}

func TestRangeDupSortSkipsDeletedDbKey(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
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
	batch, err := membatchwithdb.NewMemoryBatch(rwTx)
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

func TestRangeOnDupSortHonorsDeletedDups(t *testing.T) {
	batch := newBatchOverDbDupSort(t)

	c, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.DeleteExact([]byte("key1"), []byte("value1.1")))

	it, err := batch.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Equal(t, []string{"value1.3", "value3.1", "value3.3"}, values)
}

func TestRangeOnDupSortTableKeepsDbValuesUnderSharedKey(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.2")))

	it, err := batch.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	keys, values := collectStream(t, it)

	require.Equal(t, []string{"key1", "key1", "key1", "key3", "key3"}, keys)
	require.Equal(t, []string{"value1.1", "value1.2", "value1.3", "value3.1", "value3.3"}, values)
}

func TestRangeDescOnDupSortTableKeepsDbValuesUnderSharedKey(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.2")))

	it, err := batch.Range(kv.TblAccountVals, nil, nil, order.Desc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Equal(t, []string{"value3.3", "value3.1", "value1.3", "value1.2", "value1.1"}, values)
}

func TestRangeOnDupSortTableCollapsesIdenticalValue(t *testing.T) {
	batch := newBatchOverDbDupSort(t)
	require.NoError(t, batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.1")))

	it, err := batch.Range(kv.TblAccountVals, nil, nil, order.Asc, kv.Unlim)
	require.NoError(t, err)
	_, values := collectStream(t, it)

	require.Equal(t, []string{"value1.1", "value1.3", "value3.1", "value3.3"}, values)
}

func TestCursorLastSkipsDeletedDbTail(t *testing.T) {
	batch := newBatchOverDbNonDupSort(t)
	require.NoError(t, batch.Delete(kv.HeaderNumber, []byte("CCAA")))

	c, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.Last()
	require.NoError(t, err)
	require.Equal(t, "CBAA", string(k))
	require.Equal(t, "value2", string(v))
}

func TestTxNumsLastAfterTruncateOnOverlay(t *testing.T) {
	_, rwTx := newTestTx(t)
	for blockNum := range uint64(5) {
		require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, blockNum*10+9))
	}
	batch, err := membatchwithdb.NewMemoryBatch(rwTx)
	require.NoError(t, err)
	t.Cleanup(batch.Close)

	require.NoError(t, rawdbv3.TxNums.Truncate(batch, 3))
	blockNum, txNum, err := rawdbv3.TxNums.Last(batch)
	require.NoError(t, err)
	require.Equal(t, uint64(2), blockNum)
	require.Equal(t, uint64(29), txNum)
}

func TestCursorLastSkipsDeletedTailOnNestedBatch(t *testing.T) {
	parent := newBatchOverDbNonDupSort(t)
	child, err := membatchwithdb.NewMemoryBatch(parent)
	require.NoError(t, err)
	t.Cleanup(child.Close)
	require.NoError(t, child.Delete(kv.HeaderNumber, []byte("CCAA")))

	c, err := child.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.Last()
	require.NoError(t, err)
	require.Equal(t, "CBAA", string(k))
	require.Equal(t, "value2", string(v))
}

func TestCursorLastKeepsEmptyValueOnNestedBatch(t *testing.T) {
	parent := newBatchOverDbNonDupSort(t)
	require.NoError(t, parent.Put(kv.HeaderNumber, []byte("AAAA"), []byte{}))
	child, err := membatchwithdb.NewMemoryBatch(parent)
	require.NoError(t, err)
	t.Cleanup(child.Close)
	for _, k := range []string{"CAAA", "CBAA", "CCAA"} {
		require.NoError(t, child.Delete(kv.HeaderNumber, []byte(k)))
	}

	c, err := child.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer c.Close()
	k, v, err := c.Last()
	require.NoError(t, err)
	require.Equal(t, "AAAA", string(k))
	require.NotNil(t, v)
	require.Empty(t, v)
}
