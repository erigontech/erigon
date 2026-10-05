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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func requireBucketRecord(t *testing.T, ctx *trieTestContext, address []byte) Record {
	t.Helper()
	key, err := BucketRootKey(address)
	require.NoError(t, err)
	data, ok := ctx.records[string(key)]
	require.True(t, ok)
	record, err := DecodeRecord(key, data)
	require.NoError(t, err)
	return record
}

func TestTrieBucketTransitions(t *testing.T) {
	address := bytes.Repeat([]byte{0x81}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	keyA := eip8297.TreeKeyStorage(address, storageSlot(64))
	keyB := eip8297.TreeKeyStorage(address, storageSlot(65))
	valueA := testTrieValue(1)
	valueB := testTrieValue(2)
	ctx := newTrieTestContext()

	_, err := NewTrie(ctx).Process([]Op{{Key: account, Value: testTrieValue(3)}})
	require.NoError(t, err)
	require.NoError(t, NewTrie(ctx).Verify())
	key, err := BucketRootKey(address)
	require.NoError(t, err)
	_, exists := ctx.records[string(key)]
	require.False(t, exists)

	root, err := NewTrie(ctx).Process([]Op{{Key: keyA, Value: valueA}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(3)},
		{Key: keyA, Value: valueA[:]},
	}), root)
	require.Equal(t, LeafRoot, requireBucketRecord(t, ctx, address).Form)
	require.NoError(t, NewTrie(ctx).Verify())

	root, err = NewTrie(ctx).Process([]Op{{Key: keyB, Value: valueB}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(3)},
		{Key: keyA, Value: valueA[:]},
		{Key: keyB, Value: valueB[:]},
	}), root)
	require.NoError(t, NewTrie(ctx).Verify())

	root, err = NewTrie(ctx).Process([]Op{{Key: keyB}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(3)},
		{Key: keyA, Value: valueA[:]},
	}), root)
	require.Equal(t, LeafRoot, requireBucketRecord(t, ctx, address).Form)
	require.NoError(t, NewTrie(ctx).Verify())

	root, err = NewTrie(ctx).Process([]Op{{Key: keyA}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: account, Value: testTrieValueBytes(3)}}), root)
	_, exists = ctx.records[string(key)]
	require.False(t, exists)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestTrieBucketCollapseMovesPrefixToLastBit(t *testing.T) {
	address := bytes.Repeat([]byte{0x82}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	k0 := storageKeyWithSuffix(stem, 0x20, 0x40)
	k1 := storageKeyWithSuffix(stem, 0xc6, 0x40)
	k2 := storageKeyWithSuffix(stem, 0xc6, 0x41)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	initial := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: k0, Value: testTrieValue(2)},
		{Key: k1, Value: testTrieValue(3)},
		{Key: k2, Value: testTrieValue(4)},
	}
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process(initial)
	require.NoError(t, err)
	require.NoError(t, NewTrie(ctx).Verify())

	root, err := NewTrie(ctx).Process([]Op{{Key: k0}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(1)},
		{Key: k1, Value: testTrieValueBytes(3)},
		{Key: k2, Value: testTrieValueBytes(4)},
	}), root)
	record := requireBucketRecord(t, ctx, address)
	require.Equal(t, ExtRoot, record.Form)
	require.Equal(t, int16(263), record.SelfExt.BitLen)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestTrieBucketEarlySplitUsesRowRecord(t *testing.T) {
	address := bytes.Repeat([]byte{0x85}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	keyA := storageKeyWithSuffix(stem, 0x20, 0)
	keyB := storageKeyWithSuffix(stem, 0x40, 0)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: account, Value: testTrieValue(3)}, {Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	record := requireBucketRecord(t, ctx, address)
	require.Equal(t, RowRoot, record.Form)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestTrieBucketEarlyRootSplitUsesRowRecord(t *testing.T) {
	address := bytes.Repeat([]byte{0x86}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	keyA := storageKeyWithSuffix(stem, 0x20, 0)
	keyB := storageKeyWithSuffix(stem, 0x40, 0)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	record := requireBucketRecord(t, ctx, address)
	require.Equal(t, RowRoot, record.Form)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestTrieHeaderAndOverflowShareOneBatch(t *testing.T) {
	address := bytes.Repeat([]byte{0x83}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	header := eip8297.TreeKeyAccount(address, eip8297.HeaderStorageSlots+63)
	overflow := eip8297.TreeKeyStorage(address, storageSlot(64))
	entries := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: header, Value: testTrieValue(2)},
		{Key: overflow, Value: testTrieValue(3)},
	}
	ctx := newTrieTestContext()
	root, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	want := eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(1)},
		{Key: header, Value: testTrieValueBytes(2)},
		{Key: overflow, Value: testTrieValueBytes(3)},
	})
	require.Equal(t, want, root)
	require.Equal(t, LeafRoot, requireBucketRecord(t, ctx, address).Form)
	require.NoError(t, NewTrie(ctx).Verify())
	assertPersistedTrie(t, ctx, entries)
}

func TestTrieVerifyRejectsStaleBucketRecord(t *testing.T) {
	address := bytes.Repeat([]byte{0x84}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	keyA := eip8297.TreeKeyStorage(address, storageSlot(64))
	keyB := eip8297.TreeKeyStorage(address, storageSlot(65))
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: account, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(2)}})
	require.NoError(t, err)
	bucketKey, err := BucketRootKey(address)
	require.NoError(t, err)
	stale := bytes.Clone(ctx.records[string(bucketKey)])
	_, err = NewTrie(ctx).Process([]Op{{Key: keyB, Value: testTrieValue(3)}})
	require.NoError(t, err)
	ctx.records[string(bucketKey)] = stale
	require.Error(t, NewTrie(ctx).Verify())
}

func TestTrieBucketRecordPrevAcrossReopenAndFormChanges(t *testing.T) {
	address := bytes.Repeat([]byte{0x01}, 20)
	key64 := eip8297.TreeKeyStorage(address, storageSlot(64))
	key256 := eip8297.TreeKeyStorage(address, storageSlot256())
	ctx := newTrieTestContext()
	ctx.rejectNilPrev = true
	value64 := testTrieValue(1)
	value256 := testTrieValue(2)
	requireProcess(t, ctx, []Op{{Key: key64, Value: value64}})
	start := cloneRecords(ctx.records)

	trie := NewTrie(ctx)
	_, err := trie.Process([]Op{{Key: key256, Value: value256}})
	require.NoError(t, err)
	deltas := trie.TakeDeltas()
	for _, delta := range deltas {
		require.Equal(t, start[string(delta.Key)], delta.Prev, "delta %x", delta.Key)
	}
	for _, delta := range slices.Backward(deltas) {
		require.NoError(t, ctx.PutBranch(delta.Key, delta.Prev, delta.Data))
	}

	require.Equal(t, start, ctx.records)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestTrieRepeatedAddressesOverflowForms(t *testing.T) {
	for _, byteValue := range []byte{0x06, 0x03, 0x0d} {
		t.Run(fmt.Sprintf("%02x", byteValue), func(t *testing.T) {
			address := bytes.Repeat([]byte{byteValue}, 20)
			key64 := eip8297.TreeKeyStorage(address, storageSlot(64))
			key256 := eip8297.TreeKeyStorage(address, storageSlot256())
			ctx := newTrieTestContext()
			_, err := NewTrie(ctx).Process([]Op{{Key: key64, Value: testTrieValue(1)}})
			require.NoError(t, err)
			_, err = NewTrie(ctx).Process([]Op{{Key: key256, Value: testTrieValue(2)}})
			require.NoError(t, err)
			require.NoError(t, NewTrie(ctx).Verify())
		})
	}
}

func storageSlot256() []byte {
	slot := make([]byte, 32)
	slot[30] = 1
	return slot
}
