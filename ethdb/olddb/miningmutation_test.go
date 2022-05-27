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
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

var testBucketA = kv.HashedAccounts
var testBucketDup = kv.HashedStorage

func TestIterateWithNextAndCurrentMixed(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	for i := 0; i < 30; i += 2 {
		err := tx.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	mut := NewMiningBatch(tx)

	for i := 1; i < 30; i += 2 {
		err := mut.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}

	// check for repetions
	for i := 0; i < 30; i += 2 {
		err := mut.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	for i := 1; i < 30; i += 2 {
		err := mut.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}

	c, err := mut.Cursor(testBucketA)

	require.NoError(t, err)
	i := 0
	for k, v, _ := c.First(); k != nil; k, v, _ = c.Next() {
		require.True(t, bytes.Compare(k, []byte{byte(i)}) == 0 && bytes.Compare(v, []byte{byte(i)}) == 0)
		currK, currV, err := c.Current()
		require.NoError(t, err)
		require.True(t, bytes.Compare(k, currK) == 0 && bytes.Compare(v, currV) == 0)
		i++
	}
}

func TestIterateWithNextAndCurrentMixedDup(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	cu, _ := tx.RwCursorDupSort(testBucketDup)
	for i := 1; i < 30; i += 2 {
		err := cu.AppendDup([]byte{byte(i / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	cu.Close()

	mut := NewMiningBatch(tx)

	for i := 0; i < 30; i += 2 {
		err := mut.Put(testBucketDup, []byte{byte(i / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}

	// check for repetions
	for i := 0; i < 30; i += 2 {
		err := mut.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	for i := 1; i < 30; i += 2 {
		err := mut.Put(testBucketA, []byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}

	c, err := mut.Cursor(testBucketDup)

	require.NoError(t, err)
	i := 0
	var currK, currV []byte
	for k, v, _ := c.First(); k != nil; k, v, _ = c.Next() {
		require.True(t, bytes.Compare(k, []byte{byte(i / 5)}) == 0 && bytes.Compare(v, []byte{byte(i)}) == 0)
		currK, currV, err = c.Current()
		require.NoError(t, err)
		require.True(t, bytes.Compare(k, currK) == 0 && bytes.Compare(v, currV) == 0)
		i++
	}
	require.True(t, currK != nil && currV != nil)
	require.Equal(t, i, 30)
}

func TestIterateWithNextDupAndCurrentMixed(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	cu, _ := tx.RwCursorDupSort(testBucketDup)
	for i := 1; i < 30; i += 2 {
		err := cu.AppendDup([]byte{byte((i) / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	cu.Close()

	mut := NewMiningBatch(tx)

	for i := 0; i < 30; i += 2 {
		err := mut.Put(testBucketDup, []byte{byte(i / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	// Let us account for repeated entries
	for i := 0; i < 30; i += 2 {
		err := mut.Put(testBucketDup, []byte{byte(i / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	for i := 1; i < 30; i += 2 {
		err := mut.Put(testBucketDup, []byte{byte(i / 5)}, []byte{byte(i)})
		require.NoError(t, err)
	}

	c, err := mut.Cursor(testBucketDup)

	require.NoError(t, err)
	i := 0
	for k, v, _ := c.First(); k != nil; k, v, _ = c.NextDup() {
		require.True(t, bytes.Compare(k, []byte{byte(i / 5)}) == 0 && bytes.Compare(v, []byte{byte(i)}) == 0)
		currK, currV, err := c.Current()
		require.NoError(t, err)
		require.True(t, bytes.Compare(k, currK) == 0 && bytes.Compare(v, currV) == 0)
		i++
	}
	require.Equal(t, i, 5)
}
