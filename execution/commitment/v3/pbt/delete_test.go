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

package pbt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieZeroValueDeletesAndCollapses(t *testing.T) {
	ctx := newTrieTestContext()
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	valueA := testTrieValue(1)
	valueB := testTrieValue(2)
	entries := []Op{{Key: keyA, Value: valueA}, {Key: keyB, Value: valueB}}
	trie := NewTrie(ctx)
	_, err := trie.Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)

	delTrie := NewTrie(ctx)
	root, err := delTrie.Process([]Op{{Key: keyB, Value: [eip8297.ValueLength]byte{}}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: keyA, Value: valueA[:]}}), root)
	require.NoError(t, NewTrie(ctx).Verify())
	entries = []Op{{Key: keyA, Value: valueA}}
	assertPersistedTrie(t, ctx, entries)

	root, err = NewTrie(ctx).Process([]Op{{Key: keyA, Value: [eip8297.ValueLength]byte{}}})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	require.Empty(t, ctx.records)
	assertPersistedTrie(t, ctx, nil)
}

func TestTrieDropStoragePrefix(t *testing.T) {
	address := bytes.Repeat([]byte{0x51}, 20)
	keyA := eip8297.TreeKeyStorage(address, storageSlot(64))
	keyB := eip8297.TreeKeyStorage(address, storageSlot(65))
	account := accountKey(0, eip8297.BasicDataLeafKey)
	ctx := newTrieTestContext()
	entries := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: keyA, Value: testTrieValue(2)},
		{Key: keyB, Value: testTrieValue(3)},
	}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)

	prefix := bytes.Clone(keyA[:33])
	root, err := NewTrie(ctx).Process([]Op{Drop(prefix)})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: account, Value: testTrieValueBytes(1)}}), root)
	require.NoError(t, NewTrie(ctx).Verify())
	for key := range ctx.records {
		require.False(t, bytes.HasPrefix([]byte(key), prefix))
	}
	assertPersistedTrie(t, ctx, []Op{{Key: account, Value: testTrieValue(1)}})
}

func TestTrieDropAndRewriteSameBatch(t *testing.T) {
	address := bytes.Repeat([]byte{0x52}, 20)
	account := accountKey(0, eip8297.BasicDataLeafKey)
	keyA := eip8297.TreeKeyStorage(address, storageSlot(64))
	keyB := eip8297.TreeKeyStorage(address, storageSlot(65))
	prefix := bytes.Clone(keyA[:33])
	initial := []Op{{Key: account, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(2)}, {Key: keyB, Value: testTrieValue(3)}}
	for _, tc := range []struct {
		name    string
		rewrite []Op
		want    []Op
	}{
		{name: "single row", rewrite: []Op{{Key: keyA, Value: testTrieValue(4)}, {Key: keyB, Value: testTrieValue(5)}}, want: []Op{{Key: account, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(4)}, {Key: keyB, Value: testTrieValue(5)}}},
		{name: "partial row", rewrite: []Op{{Key: keyA, Value: testTrieValue(4)}}, want: []Op{{Key: account, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(4)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTrieTestContext()
			requireProcess(t, ctx, initial)
			ops := append([]Op{{Drop: prefix}}, tc.rewrite...)
			root, err := NewTrie(ctx).Process(ops)
			require.NoError(t, err)
			require.Equal(t, eip8297.StateRoot(entriesFromOps(tc.want)), root)
			assertPersistedTrie(t, ctx, tc.want)
		})
	}

	address = bytes.Repeat([]byte{0x63}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	k0 := storageKeyWithSuffix(stem, 0x20, 0x40)
	k1 := storageKeyWithSuffix(stem, 0xc6, 0x40)
	k2 := storageKeyWithSuffix(stem, 0xc6, 0x41)
	initial = []Op{{Key: account, Value: testTrieValue(1)}, {Key: k0, Value: testTrieValue(2)}, {Key: k1, Value: testTrieValue(3)}, {Key: k2, Value: testTrieValue(4)}}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	root, err := NewTrie(ctx).Process(append([]Op{{Drop: bytes.Clone(k0[:33])}}, initial[1:]...))
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entriesFromOps(initial)), root)
	assertPersistedTrie(t, ctx, initial)
}

func TestTrieDeleteAndInsertKeepsTopRowsAttached(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys [][]byte
	}{
		{name: "three leaves", keys: [][]byte{trieCodeKey(0, 0, 1), trieCodeKey(0, 2, 2), trieCodeKey(0, 8, 3)}},
		{name: "two leaves", keys: [][]byte{trieCodeKey(0, 0, 1), trieCodeKey(0, 2, 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTrieTestContext()
			initial := make([]Op, 0, len(tc.keys))
			for index, key := range tc.keys {
				initial = append(initial, Op{Key: key, Value: testTrieValue(byte(index + 1))})
			}
			requireProcess(t, ctx, initial)
			newKey := trieCodeKey(0x10, 0, 4)
			batch := []Op{{Key: tc.keys[0], Value: [32]byte{}}, {Key: newKey, Value: testTrieValue(4)}}
			root, err := NewTrie(ctx).Process(batch)
			require.NoError(t, err)
			want := append([]Op(nil), initial[1:]...)
			want = append(want, Op{Key: newKey, Value: testTrieValue(4)})
			require.Equal(t, eip8297.StateRoot(entriesFromOps(want)), root)
			assertPersistedTrie(t, ctx, want)
		})
	}
}

func TestTrieCollapseMovesStorageSplitToLastBit(t *testing.T) {
	address := bytes.Repeat([]byte{0x62}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	k0 := storageKeyWithSuffix(stem, 0x20, 0x40)
	k1 := storageKeyWithSuffix(stem, 0xc6, 0x40)
	k2 := storageKeyWithSuffix(stem, 0xc6, 0x41)
	account := accountKey(0, eip8297.BasicDataLeafKey)
	value := testTrieValue(1)
	ctx := newTrieTestContext()
	entries := []Op{
		{Key: account, Value: testTrieValue(4)},
		{Key: k0, Value: value},
		{Key: k1, Value: testTrieValue(2)},
		{Key: k2, Value: testTrieValue(3)},
	}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
	ctx.reads = nil
	root, err := NewTrie(ctx).Process([]Op{{Key: k0, Value: [32]byte{}}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(4)},
		{Key: k1, Value: testTrieValueBytes(2)},
		{Key: k2, Value: testTrieValueBytes(3)},
	}), root)
	require.Equal(t, [][]byte{GlobalRootKey(), rowKeyForTestPath(k1, 264)}, ctx.reads)
	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, RowRoot, record.Form)
	require.Equal(t, int16(523), record.Cells[15].Prefix.BitLen)
	require.NoError(t, NewTrie(ctx).Verify())
	assertPersistedTrie(t, ctx, []Op{{Key: account, Value: testTrieValue(4)}, {Key: k1, Value: testTrieValue(2)}, {Key: k2, Value: testTrieValue(3)}})
}

func TestTrieCollapseThenInsertInOneBatch(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	keyC := trieCodeKey(0, 8, 3)
	ctx := newTrieTestContext()
	entries := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
	root, err := NewTrie(ctx).Process([]Op{
		{Key: keyB, Value: [32]byte{}},
		{Key: keyC, Value: testTrieValue(3)},
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: keyA, Value: testTrieValueBytes(1)},
		{Key: keyC, Value: testTrieValueBytes(3)},
	}), root)
	require.NoError(t, NewTrie(ctx).Verify())
	assertPersistedTrie(t, ctx, []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyC, Value: testTrieValue(3)}})
}

func TestTrieDeletesAllCellsInOneBatch(t *testing.T) {
	account := accountKey(0, eip8297.BasicDataLeafKey)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{0x71}, 20), storageSlot(64))
	ctx := newTrieTestContext()
	entries := []Op{{Key: account, Value: testTrieValue(1)}, {Key: storage, Value: testTrieValue(2)}}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
	root, err := NewTrie(ctx).Process([]Op{
		{Key: account, Value: [32]byte{}},
		{Key: storage, Value: [32]byte{}},
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	require.Empty(t, ctx.records)
	assertPersistedTrie(t, ctx, nil)
}

func storageSlot(value byte) []byte {
	slot := make([]byte, 32)
	slot[31] = value
	return slot
}

func storageKeyWithSuffix(prefix []byte, first, last byte) []byte {
	key := append(bytes.Clone(prefix), first)
	key = append(key, make([]byte, 31)...)
	key = append(key, last)
	return key
}

func entriesFromOps(ops []Op) []eip8297.Entry {
	entries := make([]eip8297.Entry, 0, len(ops))
	for _, op := range ops {
		entries = append(entries, eip8297.Entry{Key: op.Key, Value: op.Value[:]})
	}
	return entries
}

func rowKeyForTestPath(key []byte, bitLen int16) []byte {
	path, err := keyPath(key)
	if err != nil {
		panic(err)
	}
	path.Truncate(bitLen)
	rowKey, err := EncodeRowKey(&path)
	if err != nil {
		panic(err)
	}
	return rowKey
}
