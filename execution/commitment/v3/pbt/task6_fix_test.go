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

func TestTrieRejectsInvalidOperationOrder(t *testing.T) {
	address := bytes.Repeat([]byte{0x91}, 20)
	keyA := trieCodeKey(0, 0, 8)
	keyB := trieCodeKey(0, 0, 9)
	storage := eip8297.TreeKeyStorage(address, storageSlot(64))
	prefix := bytes.Clone(storage[:33])
	tests := []struct {
		name string
		ops  []Op
	}{
		{name: "duplicate", ops: []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(2)}}},
		{name: "unsorted", ops: []Op{{Key: keyB, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(2)}}},
		{name: "drop follows write", ops: []Op{{Key: storage, Value: testTrieValue(1)}, Drop(prefix)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTrie(newTrieTestContext()).Process(tt.ops)
			require.ErrorContains(t, err, "operation list")
		})
	}
}

func TestTrieRefreshesRoutingWithinBatch(t *testing.T) {
	tests := []struct {
		name    string
		initial []Op
		batch   []Op
		want    []Op
	}{
		{
			name:    "code",
			initial: []Op{{Key: trieCodeKey(0, 0, 8), Value: testTrieValue(1)}, {Key: trieCodeKey(0, 0, 9), Value: testTrieValue(2)}},
			batch:   []Op{{Key: trieCodeKey(0, 0, 2), Value: testTrieValue(3)}, {Key: trieCodeKey(0, 0, 9)}},
			want:    []Op{{Key: trieCodeKey(0, 0, 2), Value: testTrieValue(3)}, {Key: trieCodeKey(0, 0, 8), Value: testTrieValue(1)}},
		},
		{
			name: "storage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "storage" {
				address := bytes.Repeat([]byte{0x92}, 20)
				stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
				key8 := storageKeyWithSuffix(stem, 0x20, 8)
				key9 := storageKeyWithSuffix(stem, 0x20, 9)
				key2 := storageKeyWithSuffix(stem, 0x20, 2)
				account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
				tt.initial = []Op{{Key: account, Value: testTrieValue(4)}, {Key: key8, Value: testTrieValue(1)}, {Key: key9, Value: testTrieValue(2)}}
				tt.batch = []Op{{Key: key2, Value: testTrieValue(3)}, {Key: key9}}
				tt.want = []Op{{Key: account, Value: testTrieValue(4)}, {Key: key2, Value: testTrieValue(3)}, {Key: key8, Value: testTrieValue(1)}}
			}
			ctx := newTrieTestContext()
			requireProcess(t, ctx, tt.initial)
			root, err := NewTrie(ctx).Process(tt.batch)
			require.NoError(t, err)
			require.Equal(t, eip8297.StateRoot(entriesFromOps(tt.want)), root)
			assertPersistedTrie(t, ctx, tt.want)
		})
	}
}

func TestTrieRefreshesRootRoutingAfterChildSplit(t *testing.T) {
	keyA := trieCodeKey(0, 0, 8)
	keyB := trieCodeKey(0, 0, 9)
	keyC := trieCodeKey(0, 0, 2)
	ctx := newTrieTestContext()
	requireProcess(t, ctx, []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})

	trie := NewTrie(ctx)
	root, err := trie.loadRoot()
	require.NoError(t, err)
	row, err := trie.extTopRow(root)
	require.NoError(t, err)
	require.Equal(t, int16(271), root.self.BitLen)
	require.NoError(t, trie.insert(keyC, testTrieValue(3)))
	require.Equal(t, int16(271), root.self.BitLen)
	require.NoError(t, trie.refreshRouting())
	require.Equal(t, int16(268), root.self.BitLen)
	require.Equal(t, int16(268), row.path.BitLen)
}

func TestTrieNestedSplitKeepsLoadedChild(t *testing.T) {
	address := bytes.Repeat([]byte{0x93}, 20)
	storage := eip8297.TreeKeyStorage(address, storageSlot(64))
	initial := []Op{
		{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)},
		{Key: trieCodeKey(0, 2, 2), Value: testTrieValue(2)},
		{Key: trieCodeKey(0, 8, 3), Value: testTrieValue(3)},
		{Key: storage, Value: testTrieValue(4)},
	}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	newKey := trieCodeKey(0x10, 0, 4)
	probe := NewTrie(ctx)
	probe.roundPrev = make(map[string][]byte)
	probeRoot, err := probe.loadRoot()
	require.NoError(t, err)
	probeRow := probeRoot.row
	oldChild, err := probe.loadBranchChild(probeRow, 0)
	require.NoError(t, err)
	require.NoError(t, probe.remove(initial[0].Key))
	require.NoError(t, probe.refreshRouting())
	require.NoError(t, probe.insert(newKey, testTrieValue(4)))
	newChild := probeRow.cell(0).child
	require.Same(t, oldChild, newChild.cell(0).child)
	batch := []Op{{Key: initial[0].Key, Value: [eip8297.ValueLength]byte{}}, {Key: newKey, Value: testTrieValue(4)}}
	want := []Op{
		{Key: initial[1].Key, Value: testTrieValue(2)},
		{Key: initial[2].Key, Value: testTrieValue(3)},
		{Key: newKey, Value: testTrieValue(4)},
		{Key: storage, Value: testTrieValue(4)},
	}
	got, err := NewTrie(ctx).Process(batch)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entriesFromOps(want)), got)
	require.NoError(t, NewTrie(ctx).Verify())
	assertPersistedTrie(t, ctx, want)
}
