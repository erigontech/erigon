// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build !js

package olddb

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

func initializeDB(rwTx kv.RwTx) {
	rwTx.Put(kv.HashedAccounts, []byte("AAAA"), []byte("value"))
	rwTx.Put(kv.HashedAccounts, []byte("CAAA"), []byte("value1"))
	rwTx.Put(kv.HashedAccounts, []byte("CBAA"), []byte("value2"))
	rwTx.Put(kv.HashedAccounts, []byte("CCAA"), []byte("value3"))
}

func TestLastMiningDB(t *testing.T) {
	rwTx, err := memdb.New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMiningBatch(rwTx)
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
	rwTx, err := memdb.New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)

	batch := NewMiningBatch(rwTx)
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
	rwTx, err := memdb.New().BeginRw(context.Background())
	require.NoError(t, err)

	initializeDB(rwTx)
	batch := NewMiningBatch(rwTx)
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
