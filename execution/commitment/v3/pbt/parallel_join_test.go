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
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestParallelGlobalJoinDoesNotEncodeChainRowsTwice(t *testing.T) {
	stemA := append([]byte{eip8297.StorageZone, 0x10}, make([]byte, 31)...)
	stemB := append([]byte{eip8297.StorageZone, 0x11}, make([]byte, 31)...)
	initial := []Op{
		{Key: storageKeyWithSuffix(stemA, 0, 1), Value: testTrieValue(1)},
		{Key: storageKeyWithSuffix(stemA, 0, 2), Value: testTrieValue(2)},
		{Key: storageKeyWithSuffix(stemB, 0, 1), Value: testTrieValue(3)},
		{Key: storageKeyWithSuffix(stemB, 0, 2), Value: testTrieValue(4)},
	}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	counts := make(map[string]int)
	trie := NewTrie(ctx)
	trie.SetCoreHooks(nil, func(_ phaseTask, key []byte) error {
		counts[string(key)]++
		return nil
	})
	updates := make([]Op, len(initial))
	for i, op := range initial {
		updates[i] = Op{Key: op.Key, Value: testTrieValue(byte(i + 11))}
	}
	_, err := trie.ProcessParallel(updates, 2)
	require.NoError(t, err)
	chain := chainPrefix(eip8297.StorageZone, 1)
	for _, key := range []string{
		string(eip8297.AppendBitPath(nil, &chain)),
		string(rowKeyForTestPath(initial[0].Key, 524)),
		string(rowKeyForTestPath(initial[2].Key, 524)),
	} {
		require.Equal(t, 1, counts[key], "%x", []byte(key))
	}
}

func TestParallelWhaleJoinUsesChildDescriptor(t *testing.T) {
	address := bytes.Repeat([]byte{0x46}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	keyA := storageKeyWithSuffix(stem, 0x80, 1)
	keyB := storageKeyWithSuffix(stem, 0x80, 2)
	absent := storageKeyWithSuffix(stem, 0x20, 1)
	initial := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	batch := []Op{{Key: absent}, {Key: keyA}}
	root, err := NewTrie(ctx).ProcessParallelWithThreshold(batch, 2, 2)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: keyB, Value: testTrieValueBytes(2)}}), root)
	require.NoError(t, NewTrie(ctx).Verify())
	bucketKey, err := bucketKeyForStorage(keyA)
	require.NoError(t, err)
	prefix, err := bucketPathForKey(bucketKey)
	require.NoError(t, err)
	probe := &Trie{upperOnly: true, upperStops: []eip8297.Bitpath{prefix}}
	require.True(t, probe.stopsUpperPath(&prefix))
}

func TestSubtreeTaskDoesNotReadAbovePrefix(t *testing.T) {
	address := bytes.Repeat([]byte{0x53}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	other := eip8297.TreeKeyAccount(bytes.Repeat([]byte{0x5a}, 20), eip8297.BasicDataLeafKey)
	ctx := newTrieTestContext()
	initial := []Op{{Key: account, Value: testTrieValue(1)}, {Key: other, Value: testTrieValue(2)}}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	requireProcess(t, ctx, initial)
	prefix := chainPrefix(eip8297.AccountZone, account[1]>>4)
	parent := NewTrie(ctx)
	descriptor, present, err := parent.descriptorAtPrefix(&prefix)
	require.NoError(t, err)
	ctx.reads = nil
	task := phaseTask{kind: phaseChain, prefix: prefix, ops: []Op{{Key: account, Value: testTrieValue(3)}}, initial: phaseBucketResult{prefix: prefix, descriptor: descriptor, present: present}, hasInitial: true}
	trie := NewTrie(ctx)
	_, err = trie.runSubtreeTask(context.Background(), ctx, task)
	require.NoError(t, err)
	for _, key := range ctx.reads {
		require.NotEqual(t, GlobalRootKey(), key)
		path, err := eip8297.DecodeBitPath(key)
		require.NoError(t, err, "%x", key)
		require.True(t, pathHasPrefix(&path, &prefix), "%x is above %x", key, []byte(eip8297.AppendBitPath(nil, &prefix)))
	}
	childContext := &phaseContext{base: ctx, records: make(map[string][]byte), prefix: &prefix}
	_, _, err = childContext.Branch(GlobalRootKey())
	require.ErrorContains(t, err, "above subtree prefix")
}
