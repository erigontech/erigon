// Copyright 2024 The Erigon Authors
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

package trie

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// requireValue asserts that key resolves to want; nil want means the key must be absent.
func requireValue(t *testing.T, tr *Trie, key, want []byte) {
	t.Helper()
	v, ok := tr.Get(key)
	if want == nil {
		require.Nil(t, v)
		return
	}
	require.True(t, ok)
	require.Equal(t, want, v)
}

func TestTrieDeleteSubtree_ShortNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val)
	requireValue(t, trie, key, val)

	// remove unknown
	trie.DeleteSubtree([]byte{uint8(2)})
	requireValue(t, trie, key, val)

	// remove by key
	trie.DeleteSubtree(key)
	requireValue(t, trie, key, nil)
}

func TestTrieDeleteSubtree_ShortNode_Debug(t *testing.T) {
	trie := newEmpty()
	addr1 := common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f")
	addr2 := common.HexToAddress("0xfc597da4849c0d854629216d9e297bbca7bb4616")

	key := []byte{uint8(1)}
	val := []byte{uint8(1)}

	keyHash, err := common.HashData(key)
	require.NoError(t, err)

	addrHash1, err := common.HashData(addr1[:])
	require.NoError(t, err)

	addrHash2, err := common.HashData(addr2[:])
	require.NoError(t, err)

	key1 := dbutils.GenerateCompositeTrieKey(addrHash1, keyHash)
	key2 := dbutils.GenerateCompositeTrieKey(addrHash2, keyHash)
	trie.Update(key1, val)
	trie.Update(key2, val)

	trie.DeleteSubtree(addrHash1[:])

	requireValue(t, trie, key2, val)
	requireValue(t, trie, key1, nil)
}

func TestTrieDeleteSubtree_ShortNode_LongPrefix(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1), uint8(1)}
	prefix := []byte{uint8(1)}
	val := []byte("1")

	trie.Update(key, val)
	requireValue(t, trie, key, val)

	// remove unknown
	trie.Delete([]byte{uint8(2)})
	requireValue(t, trie, key, val)

	// remove by prefix
	trie.DeleteSubtree(prefix)
	requireValue(t, trie, key, nil)
}

func TestTrieDeleteSubtree_DuoNode(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	keyExist := []byte{uint8(2)}
	valExist := []byte{uint8(2)}

	trie.Update(key, val)
	trie.Update(keyExist, valExist)
	requireValue(t, trie, key, val)

	trie.DeleteSubtree(key)

	requireValue(t, trie, key, nil)
	requireValue(t, trie, keyExist, valExist)
}

func TestTrieDeleteSubtree_TwoDuoNode_FullMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(2), uint8(3)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)

	trie.DeleteSubtree(key1)
	trie.DeleteSubtree(key2)

	requireValue(t, trie, key1, nil)
	requireValue(t, trie, key2, nil)
	requireValue(t, trie, key3, val3)
}

func TestTrieDeleteSubtree_DuoNode_PartialMatch(t *testing.T) {
	trie := newEmpty()

	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(2), uint8(3)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)

	trie.DeleteSubtree(partialKey)

	requireValue(t, trie, key1, nil)
	requireValue(t, trie, key2, nil)
	requireValue(t, trie, key3, val3)
}

func TestTrieDeleteSubtree_FromFullNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(1), uint8(3)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(partialKey)

	requireValue(t, trie, key1, nil)
	requireValue(t, trie, key2, nil)
	requireValue(t, trie, key3, nil)
	requireValue(t, trie, key4, val4)
}

func TestTrieDeleteSubtree_RemoveFullNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2), uint8(1)}
	key3 := []byte{uint8(1), uint8(1), uint8(3), uint8(1)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}
	partialKey := []byte{uint8(1), uint8(1)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(partialKey)

	requireValue(t, trie, key1, nil)
	requireValue(t, trie, key2, nil)
	requireValue(t, trie, key3, nil)
	requireValue(t, trie, key4, val4)
}

func TestTrieDeleteSubtree_FullNode_FullMatch(t *testing.T) {
	trie := newEmpty()
	key1 := []byte{uint8(1), uint8(1), uint8(1)}
	key2 := []byte{uint8(1), uint8(1), uint8(2)}
	key3 := []byte{uint8(1), uint8(1), uint8(3)}
	key4 := []byte{uint8(1), uint8(2), uint8(4)}

	val1 := []byte{uint8(1)}
	val2 := []byte{uint8(2)}
	val3 := []byte{uint8(3)}
	val4 := []byte{uint8(4)}

	trie.Update(key1, val1)
	trie.Update(key2, val2)
	trie.Update(key3, val3)
	trie.Update(key4, val4)

	trie.DeleteSubtree(key1)
	trie.DeleteSubtree(key2)

	requireValue(t, trie, key1, nil)
	requireValue(t, trie, key2, nil)
	requireValue(t, trie, key3, val3)
}

func TestTrieDeleteSubtree_ValueNode_PartialMatch(t *testing.T) {
	trie := newEmpty()
	key := []byte{uint8(1)}
	val := []byte{uint8(1)}
	keyExist := []byte{uint8(2)}
	valExist := []byte{uint8(2)}

	trie.Update(key, val)
	trie.Update(keyExist, valExist)
	requireValue(t, trie, key, val)

	trie.DeleteSubtree(key)

	requireValue(t, trie, key, nil)
	requireValue(t, trie, keyExist, valExist)
}

func TestAccountNotRemovedAfterRemovingSubtrieAfterAccount(t *testing.T) {
	acc := &accounts.Account{
		Nonce:       2,
		Incarnation: 2,
		Balance:     u256.U64(200),
		Root:        EmptyRoot,
		CodeHash:    accounts.InternCodeHash(emptyState),
	}

	trie := newEmpty()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubAddr := crypto.PubkeyToAddress(key.PublicKey)
	addrHash, err := common.HashData(pubAddr[:])
	require.NoError(t, err)
	trie.UpdateAccount(addrHash[:], acc)

	accRes1, _ := trie.GetAccount(addrHash[:])
	require.Equal(t, acc, accRes1)

	val1 := []byte("1")
	dataKey1, err := common.HashData([]byte("1"))
	require.NoError(t, err)

	val2 := []byte("2")
	dataKey2, err := common.HashData([]byte("2"))
	require.NoError(t, err)

	trie.Update(dbutils.GenerateCompositeTrieKey(addrHash, dataKey1), val1)
	trie.Update(dbutils.GenerateCompositeTrieKey(addrHash, dataKey2), val2)

	trie.DeleteSubtree(addrHash[:])

	accRes2, _ := trie.GetAccount(addrHash[:])
	require.Equal(t, acc, accRes2, "account must survive deletion of its storage subtrie")
}
