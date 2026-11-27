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
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv/dbutils"
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

func TestTrieDeleteSubtree_PrefixBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prefix  []byte
		removed []string
		kept    []string
	}{
		{name: "empty trie"},
		{name: "short root", removed: []string{"\x10"}},
		{name: "duo root", removed: []string{"\x10", "\x20"}},
		{name: "full root", removed: []string{"\x10", "\x20", "\x30"}},
		{name: "duo with terminal value", removed: []string{"\x10", ""}},
		{name: "full with terminal value", removed: []string{"\x10", "\x20", ""}},
		{name: "duo after extension", prefix: []byte("a"), removed: []string{"a\x10", "a\x20"}},
		{name: "full after extension", prefix: []byte("a"), removed: []string{"a\x10", "a\x20", "a\x30"}},
		{name: "duo after odd nibble path", prefix: []byte("a"), removed: []string{"a\x10", "a\x20"}, kept: []string{"b"}},
		{name: "full after odd nibble path", prefix: []byte("a"), removed: []string{"a\x10", "a\x20", "a\x30"}, kept: []string{"b"}},
		{name: "inside extension", prefix: []byte("a"), removed: []string{"ab\x10", "ab\x20"}},
		{name: "inside leaf", prefix: []byte("a"), removed: []string{"ab"}, kept: []string{"b"}},
		{name: "missing prefix", prefix: []byte("a"), kept: []string{"b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newEmpty()
			value := []byte{1}
			for _, key := range tc.removed {
				tr.Update([]byte(key), value)
			}
			for _, key := range tc.kept {
				tr.Update([]byte(key), value)
			}

			tr.Hash()
			require.NotPanics(t, func() { tr.DeleteSubtree(tc.prefix) })

			for _, key := range tc.removed {
				got, _ := tr.Get([]byte(key))
				require.Nil(t, got, "value at key %x", key)
			}
			for _, key := range tc.kept {
				requireValue(t, tr, []byte(key), value)
			}
			want := newEmpty()
			for _, key := range tc.kept {
				want.Update([]byte(key), value)
			}
			require.Equal(t, want.Hash(), tr.Hash())
		})
	}
}

func TestTrieDeleteSubtree_UnresolvedHash(t *testing.T) {
	for _, tc := range []struct {
		name   string
		root   Node
		prefix []byte
		panics bool
	}{
		{name: "root", root: &HashNode{hash: make([]byte, 32)}},
		{name: "extension boundary", root: NewShortNode([]byte{6, 1}, &HashNode{hash: make([]byte, 32)}), prefix: []byte("a")},
		{name: "prefix continues", root: NewShortNode([]byte{6, 1}, &HashNode{hash: make([]byte, 32)}), prefix: []byte("ab"), panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewInMemoryTrie(tc.root)
			before := tr.Hash()
			if tc.panics {
				require.Panics(t, func() { tr.DeleteSubtree(tc.prefix) })
				require.Equal(t, before, tr.Hash())
			} else {
				require.NotPanics(t, func() { tr.DeleteSubtree(tc.prefix) })
				require.Equal(t, newEmpty().Hash(), tr.Hash())
			}
		})
	}
}

func TestTrieDeleteSubtree_AccountRoot(t *testing.T) {
	account := &AccountNode{Storage: ValueNode{1}, Code: CodeNode{1}}
	tr := NewInMemoryTrie(account)
	tr.DeleteSubtree(nil)
	require.Same(t, account, tr.RootNode)
	require.Nil(t, account.Storage)
	require.Nil(t, account.Code)
	require.Equal(t, empty.RootHash, account.Root)
	require.True(t, account.RootCorrect)
}

func TestTrieDeleteSubtree_AccountAtPrefix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prefix  []byte
		removed []string
	}{
		{name: "empty key"},
		{name: "nonempty key", prefix: []byte("a")},
		{name: "empty key at duo", removed: []string{"\x10"}},
		{name: "empty key at full", removed: []string{"\x10", "\x20"}},
		{name: "nonempty key at duo", prefix: []byte("a"), removed: []string{"a\x10"}},
		{name: "nonempty key at full", prefix: []byte("a"), removed: []string{"a\x10", "a\x20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := CodeNode{1}
			account := testAccount(2, u256.U64(200), withCodeHash(crypto.Keccak256Hash(code)))
			account.Root = common.Hash{1}
			tr := newEmpty()
			for _, key := range tc.removed {
				tr.Update([]byte(key), []byte{1})
			}
			tr.UpdateAccount(tc.prefix, account)
			require.NoError(t, tr.UpdateAccountCode(tc.prefix, code))
			tr.Hash()

			tr.DeleteSubtree(tc.prefix)

			account.Root = empty.RootHash
			got, ok := tr.GetAccount(tc.prefix)
			require.True(t, ok)
			require.Equal(t, account, got)
			cachedCode, _ := tr.GetAccountCode(tc.prefix)
			require.Empty(t, cachedCode)
			want := newEmpty()
			want.UpdateAccount(tc.prefix, account)
			require.Equal(t, want.Hash(), tr.Hash())
		})
	}
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

	keyHash := crypto.Keccak256Hash(key)
	addrHash1 := crypto.Keccak256Hash(addr1[:])
	addrHash2 := crypto.Keccak256Hash(addr2[:])

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

func TestAccountNotRemovedAfterRemovingSubtrieAfterAccount(t *testing.T) {
	acc := testAccount(2, u256.U64(200), withIncarnation(2))

	trie := newEmpty()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubAddr := crypto.PubkeyToAddress(key.PublicKey)
	addrHash := crypto.Keccak256Hash(pubAddr[:])
	trie.UpdateAccount(addrHash[:], acc)

	accRes1, _ := trie.GetAccount(addrHash[:])
	require.Equal(t, acc, accRes1)

	val1 := []byte("1")
	dataKey1 := crypto.Keccak256Hash([]byte("1"))

	val2 := []byte("2")
	dataKey2 := crypto.Keccak256Hash([]byte("2"))

	trie.Update(dbutils.GenerateCompositeTrieKey(addrHash, dataKey1), val1)
	trie.Update(dbutils.GenerateCompositeTrieKey(addrHash, dataKey2), val2)

	trie.DeleteSubtree(addrHash[:])

	accRes2, _ := trie.GetAccount(addrHash[:])
	require.Equal(t, acc, accRes2, "account must survive deletion of its storage subtrie")
}
