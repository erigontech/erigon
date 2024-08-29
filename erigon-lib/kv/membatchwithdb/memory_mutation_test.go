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

package membatchwithdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	stateLib "github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon-lib/kv"
)

func initializeDbNonDupSort(rwTx kv.RwTx) {
	rwTx.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value"))
	rwTx.Put(kv.HashedAccounts, []byte("CAAA"), []byte("value1"))
	rwTx.Put(kv.HashedAccounts, []byte("CBAA"), []byte("value2"))
	rwTx.Put(kv.HashedAccounts, []byte("CCAA"), []byte("value3"))
}

func TestPutAppendHas(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("AAAA"), []byte("value1.5")))
	//MDBX's APPEND checking only keys, not values
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("AAAA"), []byte("value1.3")))

	require.NoError(t, batch.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value1.3")))
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("CBAA"), []byte("value3.5")))
	//MDBX's APPEND checking only keys, not values
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("CBAA"), []byte("value3.1")))
	require.NoError(t, batch.AppendDup(kv.HashedAccounts, []byte("CBAA"), []byte("value3.1")))
	require.Error(t, batch.Append(kv.HashedAccounts, []byte("AAAA"), []byte("value1.3")))

	require.Nil(t, batch.Flush(context.Background(), rwTx))

	exist, err := batch.Has(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	require.Equal(t, exist, true)

	val, err := batch.GetOne(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	require.Equal(t, val, []byte("value1.3"))

	exist, err = batch.Has(kv.HashedAccounts, []byte("KKKK"))
	require.Nil(t, err)
	require.Equal(t, exist, false)
}

func TestLastMiningDB(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("BCAA"), []byte("value5"))

	cursor, err := batch.Cursor(kv.HashedAccounts)
	require.NoError(t, err)

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
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("DCAA"), []byte("value5"))

	cursor, err := batch.Cursor(kv.HashedAccounts)
	require.NoError(t, err)

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
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)
	batch := NewMemoryBatch(rwTx, "", log.Root())
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("DCAA"), []byte("value5"))
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))

	batch.Delete(kv.HashedAccounts, []byte("BAAA"))
	batch.Delete(kv.HashedAccounts, []byte("CBAA"))

	cursor, err := batch.Cursor(kv.HashedAccounts)
	require.NoError(t, err)

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
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)
	batch := NewMemoryBatch(rwTx, "", log.Root())
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value5"))
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))

	require.NoError(t, batch.Flush(context.Background(), rwTx))

	value, err := rwTx.GetOne(kv.HashedAccounts, []byte("BAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value4"))

	value, err = rwTx.GetOne(kv.HashedAccounts, []byte("AAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value5"))
}

func TestForEach(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))
	require.NoError(t, batch.Flush(context.Background(), rwTx))

	var keys []string
	var values []string
	err := batch.ForEach(kv.HashedAccounts, []byte("XYAZ"), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Nil(t, keys)
	require.Nil(t, values)

	err = batch.ForEach(kv.HashedAccounts, []byte("CC"), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, []string{"CCAA", "FCAA"}, keys)
	require.Equal(t, []string{"value3", "value5"}, values)

	var keys1 []string
	var values1 []string

	err = batch.ForEach(kv.HashedAccounts, []byte("A"), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, []string{"AAAA", "CAAA", "CBAA", "CCAA", "FCAA"}, keys1)
	require.Equal(t, []string{"value", "value1", "value2", "value3", "value5"}, values1)
}

func NewTestTemporalDb(tb testing.TB) (kv.RwDB, kv.RwTx, *stateLib.Aggregator) {
	tb.Helper()
	db := memdb.NewStateDB(tb.TempDir())
	tb.Cleanup(db.Close)

	agg, err := stateLib.NewAggregator(context.Background(), datadir.New(tb.TempDir()), 16, db, nil, log.New())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(agg.Close)

	_db, err := temporal.New(db, agg)
	if err != nil {
		tb.Fatal(err)
	}
	tx, err := _db.BeginTemporalRw(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return _db, tx, agg
}

func TestPrefix(t *testing.T) {
	_, rwTx, _ := NewTestTemporalDb(t)

	initializeDbNonDupSort(rwTx)

	kvs1, err := rwTx.Prefix(kv.HashedAccounts, []byte("AB"))
	require.Nil(t, err)
	defer kvs1.Close()
	require.False(t, kvs1.HasNext())

	var keys1 []string
	var values1 []string
	kvs2, err := rwTx.Prefix(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	defer kvs2.Close()
	for kvs2.HasNext() {
		k1, v1, err := kvs2.Next()
		require.Nil(t, err)
		keys1 = append(keys1, string(k1))
		values1 = append(values1, string(v1))
	}
	require.Equal(t, []string{"AAAA"}, keys1)
	require.Equal(t, []string{"value"}, values1)

	var keys []string
	var values []string
	kvs3, err := rwTx.Prefix(kv.HashedAccounts, []byte("C"))
	require.Nil(t, err)
	defer kvs3.Close()
	for kvs3.HasNext() {
		k1, v1, err := kvs3.Next()
		require.Nil(t, err)
		keys = append(keys, string(k1))
		values = append(values, string(v1))
	}
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value1", "value2", "value3"}, values)
}

func TestForAmount(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	var keys []string
	var values []string
	err := batch.ForAmount(kv.HashedAccounts, []byte("C"), uint32(3), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})

	require.Nil(t, err)
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value1", "value2", "value3"}, values)

	var keys1 []string
	var values1 []string
	err = batch.ForAmount(kv.HashedAccounts, []byte("C"), uint32(10), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})

	require.Nil(t, err)
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys1)
	require.Equal(t, []string{"value1", "value2", "value3"}, values1)
}

func TestGetOneAfterClearBucket(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	err := batch.ClearBucket(kv.HashedAccounts)
	require.Nil(t, err)

	cond := batch.isTableCleared(kv.HashedAccounts)
	require.True(t, cond)

	val, err := batch.GetOne(kv.HashedAccounts, []byte("A"))
	require.Nil(t, err)
	require.Nil(t, val)

	val, err = batch.GetOne(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	require.Nil(t, val)
}

func TestSeekExactAfterClearBucket(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	err := batch.ClearBucket(kv.HashedAccounts)
	require.Nil(t, err)

	cond := batch.isTableCleared(kv.HashedAccounts)
	require.True(t, cond)

	cursor, err := batch.RwCursor(kv.HashedAccounts)
	require.NoError(t, err)

	key, val, err := cursor.SeekExact([]byte("AAAA"))
	require.Nil(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)

	err = cursor.Put([]byte("AAAA"), []byte("valueX"))
	require.Nil(t, err)

	key, val, err = cursor.SeekExact([]byte("AAAA"))
	require.Nil(t, err)
	assert.Equal(t, []byte("AAAA"), key)
	assert.Equal(t, []byte("valueX"), val)

	key, val, err = cursor.SeekExact([]byte("BBBB"))
	require.Nil(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)
}

func TestFirstAfterClearBucket(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	err := batch.ClearBucket(kv.HashedAccounts)
	require.Nil(t, err)

	err = batch.Put(kv.HashedAccounts, []byte("BBBB"), []byte("value5"))
	require.Nil(t, err)

	cursor, err := batch.Cursor(kv.HashedAccounts)
	require.NoError(t, err)

	key, val, err := cursor.First()
	require.Nil(t, err)
	assert.Equal(t, []byte("BBBB"), key)
	assert.Equal(t, []byte("value5"), val)

	key, val, err = cursor.Next()
	require.Nil(t, err)
	assert.Nil(t, key)
	assert.Nil(t, val)
}

func TestIncReadSequence(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbNonDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	_, err := batch.IncrementSequence(kv.HashedAccounts, uint64(12))
	require.Nil(t, err)

	val, err := batch.ReadSequence(kv.HashedAccounts)
	require.Nil(t, err)
	require.Equal(t, val, uint64(12))
}

func initializeDbDupSort(rwTx kv.RwTx) {
	rwTx.Put(kv.AccountChangeSet, []byte("key1"), []byte("value1.1"))
	rwTx.Put(kv.AccountChangeSet, []byte("key3"), []byte("value3.1"))
	rwTx.Put(kv.AccountChangeSet, []byte("key1"), []byte("value1.3"))
	rwTx.Put(kv.AccountChangeSet, []byte("key3"), []byte("value3.3"))
}

func TestNext(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	batch.Put(kv.AccountChangeSet, []byte("key1"), []byte("value1.2"))

	cursor, err := batch.CursorDupSort(kv.AccountChangeSet)
	require.NoError(t, err)

	k, v, err := cursor.First()
	require.Nil(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.1"), v)

	k, v, err = cursor.Next()
	require.Nil(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.2"), v)

	k, v, err = cursor.Next()
	require.Nil(t, err)
	assert.Equal(t, []byte("key1"), k)
	assert.Equal(t, []byte("value1.3"), v)

	k, v, err = cursor.Next()
	require.Nil(t, err)
	assert.Equal(t, []byte("key3"), k)
	assert.Equal(t, []byte("value3.1"), v)

	k, v, err = cursor.Next()
	require.Nil(t, err)
	assert.Equal(t, []byte("key3"), k)
	assert.Equal(t, []byte("value3.3"), v)

	k, v, err = cursor.Next()
	require.Nil(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)
}

func TestNextNoDup(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	batch.Put(kv.AccountChangeSet, []byte("key2"), []byte("value2.1"))
	batch.Put(kv.AccountChangeSet, []byte("key2"), []byte("value2.2"))

	cursor, err := batch.CursorDupSort(kv.AccountChangeSet)
	require.NoError(t, err)

	k, _, err := cursor.First()
	require.Nil(t, err)
	assert.Equal(t, []byte("key1"), k)

	k, _, err = cursor.NextNoDup()
	require.Nil(t, err)
	assert.Equal(t, []byte("key2"), k)

	k, _, err = cursor.NextNoDup()
	require.Nil(t, err)
	assert.Equal(t, []byte("key3"), k)
}

func TestDeleteCurrentDuplicates(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	initializeDbDupSort(rwTx)

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	cursor, err := batch.RwCursorDupSort(kv.AccountChangeSet)
	require.NoError(t, err)

	require.NoError(t, cursor.Put([]byte("key3"), []byte("value3.2")))

	key, _, err := cursor.SeekExact([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), key)

	require.NoError(t, cursor.DeleteCurrentDuplicates())

	require.NoError(t, batch.Flush(context.Background(), rwTx))

	var keys []string
	var values []string
	err = rwTx.ForEach(kv.AccountChangeSet, nil, func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{"key1", "key1"}, keys)
	require.Equal(t, []string{"value1.1", "value1.3"}, values)
}

func TestSeekBothRange(t *testing.T) {
	_, rwTx := memdb.NewTestTx(t)

	rwTx.Put(kv.AccountChangeSet, []byte("key1"), []byte("value1.1"))
	rwTx.Put(kv.AccountChangeSet, []byte("key3"), []byte("value3.3"))

	batch := NewMemoryBatch(rwTx, "", log.Root())
	defer batch.Close()

	cursor, err := batch.RwCursorDupSort(kv.AccountChangeSet)
	require.NoError(t, err)

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
	_, rwTx := memdb.NewTestTx(t)

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
