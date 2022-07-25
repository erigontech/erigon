/*
   Copyright 2022 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package memdb

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"
)

func initializeDB(rwTx kv.RwTx) {
	rwTx.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value"))
	rwTx.Put(kv.HashedAccounts, []byte("CAAA"), []byte("value1"))
	rwTx.Put(kv.HashedAccounts, []byte("CBAA"), []byte("value2"))
	rwTx.Put(kv.HashedAccounts, []byte("CCAA"), []byte("value3"))
}

func TestPutAppendHas(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.Nil(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("AAAA"), []byte("value1.5")))
	require.Error(t, batch.Append(kv.HashedAccounts, []byte("AAAA"), []byte("value1.3")))
	require.NoError(t, batch.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value1.3")))
	require.NoError(t, batch.Append(kv.HashedAccounts, []byte("CBAA"), []byte("value3.5")))
	require.Error(t, batch.Append(kv.HashedAccounts, []byte("CBAA"), []byte("value3.1")))
	require.NoError(t, batch.AppendDup(kv.HashedAccounts, []byte("CBAA"), []byte("value3.1")))

	require.Nil(t, batch.Flush(rwTx))

	exist, err := batch.Has(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	require.Equal(t, exist, true)

	val, err := batch.Get(kv.HashedAccounts, []byte("AAAA"))
	require.Nil(t, err)
	require.Equal(t, val, []byte("value1.3"))

	exist, err = batch.Has(kv.HashedAccounts, []byte("KKKK"))
	require.Nil(t, err)
	require.Equal(t, exist, false)
}

func TestLastMiningDB(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
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
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
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
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)
	batch := NewMemoryBatch(rwTx)
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("DCAA"), []byte("value5"))
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))

	batch.Delete(kv.HashedAccounts, []byte("BAAA"), nil)
	batch.Delete(kv.HashedAccounts, []byte("CBAA"), nil)

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
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)
	batch := NewMemoryBatch(rwTx)
	batch.Put(kv.HashedAccounts, []byte("BAAA"), []byte("value4"))
	batch.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value5"))
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))

	require.NoError(t, batch.Flush(rwTx))

	value, err := rwTx.GetOne(kv.HashedAccounts, []byte("BAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value4"))

	value, err = rwTx.GetOne(kv.HashedAccounts, []byte("AAAA"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("value5"))
}

func TestForEach(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	batch.Put(kv.HashedAccounts, []byte("FCAA"), []byte("value5"))
	require.NoError(t, batch.Flush(rwTx))

	var keys []string
	var values []string
	err = batch.ForEach(kv.HashedAccounts, []byte("XYAZ"), func(k, v []byte) error {
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

func TestForPrefix(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	var keys1 []string
	var values1 []string

	err = batch.ForPrefix(kv.HashedAccounts, []byte("AB"), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Nil(t, keys1)
	require.Nil(t, values1)

	err = batch.ForPrefix(kv.HashedAccounts, []byte("AAAA"), func(k, v []byte) error {
		keys1 = append(keys1, string(k))
		values1 = append(values1, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, []string{"AAAA"}, keys1)
	require.Equal(t, []string{"value"}, values1)

	var keys []string
	var values []string
	err = batch.ForPrefix(kv.HashedAccounts, []byte("C"), func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, []string{"CAAA", "CBAA", "CCAA"}, keys)
	require.Equal(t, []string{"value1", "value2", "value3"}, values)
}

func TestForAmount(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	defer batch.Close()

	var keys []string
	var values []string
	err = batch.ForAmount(kv.HashedAccounts, []byte("C"), uint32(3), func(k, v []byte) error {
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

func TestGetClearBucket(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	defer batch.Close()

	err = batch.ClearBucket(kv.HashedAccounts)
	require.Nil(t, err)

	cond := batch.isTableCleared(kv.HashedAccounts)
	require.Equal(t, cond, true)

	val, err := batch.GetOne(kv.HashedAccounts, []byte("A"))
	require.Nil(t, err)
	require.Nil(t, val)
}

func TestIncReadSequence(t *testing.T) {
	rwTx, err := New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMemoryBatch(rwTx)
	defer batch.Close()

	_, err = batch.IncrementSequence(kv.HashedAccounts, uint64(12))
	require.Nil(t, err)

	val, err := batch.ReadSequence(kv.HashedAccounts)
	require.Nil(t, err)
	require.Equal(t, val, uint64(12))
}
