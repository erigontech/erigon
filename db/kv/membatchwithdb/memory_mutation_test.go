// Copyright 2022 The Erigon Authors
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

func initializeDbNonDupSort(rwTx kv.RwTx) {
	rwTx.Put(kv.HeaderNumber, []byte("AAAA"), []byte("value"))
	rwTx.Put(kv.HeaderNumber, []byte("CAAA"), []byte("value1"))
	rwTx.Put(kv.HeaderNumber, []byte("CBAA"), []byte("value2"))
	rwTx.Put(kv.HeaderNumber, []byte("CCAA"), []byte("value3"))
}

func TestPutAppendHas(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	require.NoError(t, batch.Append(kv.HeaderNumber, []byte("AAAA"), []byte("value1.5")))
	//MDBX's APPEND checking only keys, not values
	require.NoError(t, batch.Append(kv.HeaderNumber, []byte("AAAA"), []byte("value1.3")))

	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("AAAA"), []byte("value1.3")))
	require.NoError(t, batch.Append(kv.HeaderNumber, []byte("CBAA"), []byte("value3.5")))
	//MDBX's APPEND checking only keys, not values
	require.NoError(t, batch.Append(kv.HeaderNumber, []byte("CBAA"), []byte("value3.1")))
	require.NoError(t, batch.AppendDup(kv.HeaderNumber, []byte("CBAA"), []byte("value3.1")))
	// Pure Go backend allows out-of-order Append (no MDBX ordering check).
	require.NoError(t, batch.Append(kv.HeaderNumber, []byte("AAAA"), []byte("value1.3")))

	require.NoError(t, batch.Flush(t.Context(), rwTx))

	exist, err := batch.Has(kv.HeaderNumber, []byte("AAAA"))
	require.NoError(t, err)
	require.True(t, exist)

	val, err := batch.GetOne(kv.HeaderNumber, []byte("AAAA"))
	require.NoError(t, err)
	require.Equal(t, val, []byte("value1.3"))

	exist, err = batch.Has(kv.HeaderNumber, []byte("KKKK"))
	require.NoError(t, err)
	require.False(t, exist)
}

func TestLastMiningDB(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	batch.Put(kv.HeaderNumber, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HeaderNumber, []byte("BCAA"), []byte("value5"))

	cursor, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer cursor.Close()

	key, value, err := cursor.Last()
	require.NoError(t, err)

	require.Equal(t, key, []byte("CCAA"))
	require.Equal(t, value, []byte("value3"))

	key, value, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, key, []byte(nil))
	require.Equal(t, value, []byte(nil))
}

func TestLastMiningMem(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	batch.Put(kv.HeaderNumber, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HeaderNumber, []byte("DCAA"), []byte("value5"))

	cursor, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer cursor.Close()

	key, value, err := cursor.Last()
	require.NoError(t, err)

	require.Equal(t, key, []byte("DCAA"))
	require.Equal(t, value, []byte("value5"))

	key, value, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, key, []byte(nil))
	require.Equal(t, value, []byte(nil))
}

func TestDeleteMining(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	batch.Put(kv.HeaderNumber, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HeaderNumber, []byte("DCAA"), []byte("value5"))
	batch.Put(kv.HeaderNumber, []byte("FCAA"), []byte("value5"))

	batch.Delete(kv.HeaderNumber, []byte("BAAA"))
	batch.Delete(kv.HeaderNumber, []byte("CBAA"))

	cursor, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer cursor.Close()

	key, value, err := cursor.SeekExact([]byte("BAAA"))
	require.NoError(t, err)
	require.Equal(t, key, []byte(nil))
	require.Equal(t, value, []byte(nil))

	key, value, err = cursor.SeekExact([]byte("CBAA"))
	require.NoError(t, err)
	require.Equal(t, key, []byte(nil))
	require.Equal(t, value, []byte(nil))
}

func TestFlush(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)
	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	batch.Put(kv.HeaderNumber, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HeaderNumber, []byte("AAAA"), []byte("value5"))
	batch.Put(kv.HeaderNumber, []byte("FCAA"), []byte("value5"))

	require.NoError(t, batch.Flush(t.Context(), rwTx))

	value, err := rwTx.GetOne(kv.HeaderNumber, []byte("BAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value4"))

	value, err = rwTx.GetOne(kv.HeaderNumber, []byte("AAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value5"))
}

func TestForEach(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()
	batch.Put(kv.HeaderNumber, []byte("FCAA"), []byte("value5"))
	require.NoError(t, batch.Flush(t.Context(), rwTx))

	var keys []string
	var values []string
	err = batch.ForEach(kv.HeaderNumber, []byte("XYAZ"), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.NoError(t, err)
	require.Nil(t, keys)
	require.Nil(t, values)

	err = batch.ForEach(kv.HeaderNumber, []byte("CC"), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"CCAA", "FCAA"}, keys)
	require.Equal(t, []string{"value3", "value5"}, values)

	var keys1 []string
	var values1 []string

	err = batch.ForEach(kv.HeaderNumber, []byte("A"), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"AAAA", "CAAA", "CBAA", "CCAA", "FCAA"}, keys1)
	require.Equal(t, []string{"value", "value1", "value2", "value3", "value5"}, values1)
}

func newTestTx(tb testing.TB) (kv.TemporalRwDB, kv.TemporalRwTx) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	stepSize := uint64(16)
	db := temporaltest.NewTestDBWithStepSize(tb, dirs, stepSize)
	tx, err := db.BeginTemporalRw(tb.Context()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}

func TestPrefix(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	kvs1, err := rwTx.Prefix(kv.HeaderNumber, []byte("AB"))
	require.NoError(t, err)
	defer kvs1.Close()
	require.False(t, kvs1.HasNext())

	var keys1 []string
	var values1 []string
	kvs2, err := rwTx.Prefix(kv.HeaderNumber, []byte("AAAA"))
	require.NoError(t, err)
	defer kvs2.Close()
	for kvs2.HasNext() {
		k1, v1, err := kvs2.Next()
		require.NoError(t, err)
		keys1 = append(keys1, string(k1))
		values1 = append(values1, string(v1))
	}
	require.Equal(t, []string{"AAAA"}, keys1)
	require.Equal(t, []string{"value"}, values1)

	var keys []string
	var values []string
	kvs3, err := rwTx.Prefix(kv.HeaderNumber, []byte("C"))
	require.NoError(t, err)
	defer kvs3.Close()
	for kvs3.HasNext() {
		k1, v1, err := kvs3.Next()
		require.NoError(t, err)
		keys = append(keys, string(k1))
		values = append(values, string(v1))
	}
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value1", "value2", "value3"}, values)
}

func TestForAmount(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	var keys []string
	var values []string
	err = batch.ForAmount(kv.HeaderNumber, []byte("C"), uint32(3), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value1", "value2", "value3"}, values)

	var keys1 []string
	var values1 []string
	err = batch.ForAmount(kv.HeaderNumber, []byte("C"), uint32(10), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys1)
	require.Equal(t, []string{"value1", "value2", "value3"}, values1)
}

func TestGetOneAfterClearBucket(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	err = batch.ClearTable(kv.HeaderNumber)
	require.NoError(t, err)

	val, err := batch.GetOne(kv.HeaderNumber, []byte("A"))
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = batch.GetOne(kv.HeaderNumber, []byte("AAAA"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestSeekExactAfterClearBucket(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	err = batch.ClearTable(kv.HeaderNumber)
	require.NoError(t, err)

	cursor, err := batch.RwCursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer cursor.Close()

	key, val, err := cursor.SeekExact([]byte("AAAA"))
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)

	err = cursor.Put([]byte("AAAA"), []byte("valueX"))
	require.NoError(t, err)

	key, val, err = cursor.SeekExact([]byte("AAAA"))
	require.NoError(t, err)
	assert.Equal(t, []byte("AAAA"), key)
	assert.Equal(t, []byte("valueX"), val)

	key, val, err = cursor.SeekExact([]byte("BBBB"))
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)
}

func TestFirstAfterClearBucket(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	err = batch.ClearTable(kv.HeaderNumber)
	require.NoError(t, err)

	err = batch.Put(kv.HeaderNumber, []byte("BBBB"), []byte("value5"))
	require.NoError(t, err)

	cursor, err := batch.Cursor(kv.HeaderNumber)
	require.NoError(t, err)
	defer cursor.Close()

	key, val, err := cursor.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("BBBB"), key)
	assert.Equal(t, []byte("value5"), val)

	key, val, err = cursor.Next()
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)
}

func TestIncReadSequence(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	_, err = batch.IncrementSequence(kv.HeaderNumber, uint64(12))
	require.NoError(t, err)

	val, err := batch.ReadSequence(kv.HeaderNumber)
	require.NoError(t, err)
	require.Equal(t, uint64(12), val)
}

func initializeDbDupSort(rwTx kv.RwTx) {
	rwTx.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.1"))
	rwTx.Put(kv.TblAccountVals, []byte("key3"), []byte("value3.1"))
	rwTx.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.3"))
	rwTx.Put(kv.TblAccountVals, []byte("key3"), []byte("value3.3"))
}

func TestNext(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	batch.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.2"))

	cursor, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer cursor.Close()

	k, v, err := cursor.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.1"), v)

	k, v, err = cursor.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.2"), v)

	k, v, err = cursor.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.3"), v)

	k, v, err = cursor.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("key3"), k)
	assert.Equal(t, []byte("value3.1"), v)

	k, v, err = cursor.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("key3"), k)
	assert.Equal(t, []byte("value3.3"), v)

	k, v, err = cursor.Next()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)
}

func TestNextNoDup(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	batch.Put(kv.TblAccountVals, []byte("key2"), []byte("value2.1"))
	batch.Put(kv.TblAccountVals, []byte("key2"), []byte("value2.2"))

	cursor, err := batch.CursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer cursor.Close()

	k, _, err := cursor.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), k)

	k, _, err = cursor.NextNoDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("key2"), k)

	k, _, err = cursor.NextNoDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("key3"), k)
}

func TestDeleteCurrentDuplicates(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbDupSort(rwTx)

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	cursor, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer cursor.Close()

	require.NoError(t, cursor.Put([]byte("key3"), []byte("value3.2")))

	key, _, err := cursor.SeekExact([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), key)

	require.NoError(t, cursor.DeleteCurrentDuplicates())

	require.NoError(t, batch.Flush(t.Context(), rwTx))

	var keys []string
	var values []string
	err = rwTx.ForEach(kv.TblAccountVals, nil, func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{"key1", "key1"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3"}, values)
}

func TestSeekBothRange(t *testing.T) {
	_, rwTx := newTestTx(t)

	rwTx.Put(kv.TblAccountVals, []byte("key1"), []byte("value1.1"))
	rwTx.Put(kv.TblAccountVals, []byte("key3"), []byte("value3.3"))

	batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	cursor, err := batch.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer cursor.Close()

	require.NoError(t, cursor.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, cursor.Put([]byte("key1"), []byte("value1.3")))

	v, err := cursor.SeekBothRange([]byte("key2"), []byte("value1.2"))
	require.NoError(t, err)
	// SeekBothRange does exact match of the key, but range match of the value, so we get nil here
	require.Nil(t, v)

	v, err = cursor.SeekBothRange([]byte("key3"), []byte("value3.2"))
	require.NoError(t, err)
	require.Equal(t, "value3.3", string(v))
}

func initializeDbHeaders(rwTx kv.RwTx) {
	rwTx.Put(kv.Headers, []byte("A"), []byte("0"))
	rwTx.Put(kv.Headers, []byte("A..........................._______________________________A"), []byte("1"))
	rwTx.Put(kv.Headers, []byte("A..........................._______________________________C"), []byte("2"))
	rwTx.Put(kv.Headers, []byte("B"), []byte("8"))
	rwTx.Put(kv.Headers, []byte("C"), []byte("9"))
	rwTx.Put(kv.Headers, []byte("D..........................._______________________________A"), []byte("3"))
	rwTx.Put(kv.Headers, []byte("D..........................._______________________________C"), []byte("4"))
}

func TestGetOne(t *testing.T) {
	_, rwTx := newTestTx(t)

	initializeDbHeaders(rwTx)

	require.NoError(t, rwTx.Put(kv.Headers, []byte("A..........................."), []byte("?")))

	require.NoError(t, rwTx.Delete(kv.Headers, []byte("A..........................._______________________________A")))
	require.NoError(t, rwTx.Put(kv.Headers, []byte("B"), []byte("7")))
	require.NoError(t, rwTx.Delete(kv.Headers, []byte("C")))
	require.NoError(t, rwTx.Put(kv.Headers, []byte("D..........................._______________________________C"), []byte("6")))
	require.NoError(t, rwTx.Put(kv.Headers, []byte("D..........................._______________________________E"), []byte("5")))

	v, err := rwTx.GetOne(kv.Headers, []byte("A"))
	require.NoError(t, err)
	assert.Equal(t, []byte("0"), v)

	v, err = rwTx.GetOne(kv.Headers, []byte("A..........................._______________________________C"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), v)

	v, err = rwTx.GetOne(kv.Headers, []byte("B"))
	require.NoError(t, err)
	assert.Equal(t, []byte("7"), v)

	v, err = rwTx.GetOne(kv.Headers, []byte("D..........................._______________________________A"))
	require.NoError(t, err)
	assert.Equal(t, []byte("3"), v)

	v, err = rwTx.GetOne(kv.Headers, []byte("D..........................._______________________________C"))
	require.NoError(t, err)
	assert.Equal(t, []byte("6"), v)

	v, err = rwTx.GetOne(kv.Headers, []byte("D..........................._______________________________E"))
	require.NoError(t, err)
	assert.Equal(t, []byte("5"), v)
}

// TestMemoryMutationConcurrentReadWrite verifies that concurrent OverlayReadView
// reads and Put writes on a MemoryMutation don't race. This simulates the Engine
// API pattern where getters (getQuickPayloadStatusIfPossible) read via ReadViews
// concurrently while InsertBlocks writes to the overlay.
func TestMemoryMutationConcurrentReadWrite(t *testing.T) {
	db, rwTx := newTestTx(t)

	// Pre-populate DB with some data.
	require.NoError(t, rwTx.Put(kv.HeaderNumber, []byte("existing-key"), []byte("db-value")))
	require.NoError(t, rwTx.Commit())

	// Open a fresh RO tx as the overlay's backing tx (simulates InsertBlocks).
	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	batch, err := membatchwithdb.NewMemoryBatch(roTx.(kv.TemporalTx), "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	// Put some initial values into the overlay.
	require.NoError(t, batch.Put(kv.HeaderNumber, []byte("overlay-key"), []byte("overlay-value")))

	var wg sync.WaitGroup
	const readers = 4
	const iterations = 500

	// Concurrent readers — simulate engine server getters using OverlayReadView.
	// Each reader opens its own RO tx (just like the real getters do).
	for r := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			readerTx, err := db.BeginRo(t.Context())
			if err != nil {
				t.Errorf("reader %d: BeginRo: %v", id, err)
				return
			}
			defer readerTx.Rollback()

			view := batch.NewReadView(readerTx)

			for i := range iterations {
				_ = i
				// Read from overlay mem layer.
				v, err := view.GetOne(kv.HeaderNumber, []byte("overlay-key"))
				if err != nil {
					t.Errorf("reader %d: GetOne overlay-key: %v", id, err)
					return
				}
				if v != nil && string(v) != "overlay-value" {
					_ = v
				}

				// Read from DB fallback (via reader's own tx).
				v, err = view.GetOne(kv.HeaderNumber, []byte("existing-key"))
				if err != nil {
					t.Errorf("reader %d: GetOne existing-key: %v", id, err)
					return
				}
				if v != nil && string(v) != "db-value" {
					_ = v
				}

				// Has check.
				_, err = view.Has(kv.HeaderNumber, []byte("overlay-key"))
				if err != nil {
					t.Errorf("reader %d: Has: %v", id, err)
					return
				}
			}
		}(r)
	}

	// Concurrent writer — simulate InsertBlocks writing to the overlay.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			key := []byte(fmt.Sprintf("write-key-%04d", i))
			val := []byte(fmt.Sprintf("write-val-%04d", i))
			if err := batch.Put(kv.HeaderNumber, key, val); err != nil {
				t.Errorf("writer: Put: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	// Verify writes landed (using single-threaded GetOne on the batch directly).
	v, err := batch.GetOne(kv.HeaderNumber, []byte("write-key-0499"))
	require.NoError(t, err)
	assert.Equal(t, []byte("write-val-0499"), v)
}

// TestMemoryMutationConcurrentDeleteAndRead verifies that concurrent Delete
// and OverlayReadView reads don't race.
func TestMemoryMutationConcurrentDeleteAndRead(t *testing.T) {
	db, rwTx := newTestTx(t)

	// Pre-populate DB.
	for i := range 100 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, rwTx.Put(kv.HeaderNumber, key, []byte("db-val")))
	}
	require.NoError(t, rwTx.Commit())

	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	batch, err := membatchwithdb.NewMemoryBatch(roTx.(kv.TemporalTx), "", log.Root())
	require.NoError(t, err)
	defer batch.Close()

	var wg sync.WaitGroup

	// Reader goroutine using OverlayReadView with its own tx.
	wg.Add(1)
	go func() {
		defer wg.Done()
		readerTx, err := db.BeginRo(t.Context())
		if err != nil {
			t.Errorf("reader: BeginRo: %v", err)
			return
		}
		defer readerTx.Rollback()
		view := batch.NewReadView(readerTx)
		for i := range 100 {
			key := []byte(fmt.Sprintf("key-%03d", i))
			_, _ = view.GetOne(kv.HeaderNumber, key)
			_, _ = view.Has(kv.HeaderNumber, key)
		}
	}()

	// Deleter goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			key := []byte(fmt.Sprintf("key-%03d", i))
			_ = batch.Delete(kv.HeaderNumber, key)
		}
	}()

	wg.Wait()
}
