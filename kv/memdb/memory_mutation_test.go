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
