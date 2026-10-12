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

func TestTrieVerifyRejectsStaleBranchHash(t *testing.T) {
	ctx := newTrieTestContext()
	a := trieCodeKey(0, 0, 1)
	b := trieCodeKey(0, 2, 2)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	trie := NewTrie(ctx)
	_, err := trie.Process([]Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}})
	require.NoError(t, err)
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: storage, Value: eip8297.EncodeStorageValue([]byte{9})}})
	require.NoError(t, err)

	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	record.Cells[0].Left[0]++
	data, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	ctx.records[string(GlobalRootKey())] = data

	require.Error(t, NewTrie(ctx).Verify())
}

func TestTrieVerifyReleasesVerifiedRows(t *testing.T) {
	ctx := newTrieTestContext()
	entries := make([]Op, 512)
	for i := range entries {
		entries[i] = Op{Key: trieCodeKey(byte(i>>8), byte(i), byte(i+1)), Value: testTrieValue(byte(i + 1))}
	}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)

	verifier := NewTrie(ctx).newVerifier()
	require.NoError(t, verifier.verify())
	require.LessOrEqual(t, len(verifier.rows), 2)
}

func TestTrieVerifyReportsProgress(t *testing.T) {
	ctx := newTrieTestContext()
	entries := make([]Op, 2)
	entries[0] = Op{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)}
	entries[1] = Op{Key: trieCodeKey(0, 2, 2), Value: testTrieValue(2)}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	var calls int
	trie := NewTrie(ctx)
	trie.SetVerifyProgress(func([]byte) { calls++ })
	require.NoError(t, trie.Verify())
	require.Positive(t, calls)
}

func TestTrieVerifyUnlinksVerifiedChildren(t *testing.T) {
	ctx := newTrieTestContext()
	entries := make([]Op, 128)
	for i := range entries {
		entries[i] = Op{Key: trieCodeKey(byte(i>>8), byte(i), byte(i+1)), Value: testTrieValue(byte(i + 1))}
	}
	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)

	verifier := NewTrie(ctx).newVerifier()
	root, err := verifier.loadRoot()
	require.NoError(t, err)
	row := root.row
	if row == nil {
		row, err = verifier.extTopRow(root)
		require.NoError(t, err)
	}
	var children []*rowNode
	for slot := range row.cells {
		if row.cell(slot).Kind == BranchCell {
			child, loadErr := verifier.loadBranchChild(row, slot)
			require.NoError(t, loadErr)
			children = append(children, child)
		}
	}
	_, err = verifier.verifyRow(row)
	require.NoError(t, err)
	check := func(row *rowNode) {
		branches := 0
		for slot := range row.cells {
			if row.cell(slot).Kind == BranchCell {
				branches++
				require.Nil(t, row.cell(slot).child)
			}
		}
		require.Positive(t, branches)
	}
	if verifier.root != nil {
		check(row)
	}
	for _, child := range children {
		require.Nil(t, child.parent)
	}
}

func TestTrieVerifyRejectsWrongRootSelfExtensionBits(t *testing.T) {
	ctx := newTrieTestContext()
	a := trieCodeKey(0, 0, 1)
	b := trieCodeKey(0, 1, 2)
	_, err := NewTrie(ctx).Process([]Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}})
	require.NoError(t, err)

	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, ExtRoot, record.Form)
	record.SelfExt.SetBitAt(20, record.SelfExt.Bit(20)^1)
	data, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	ctx.records[string(GlobalRootKey())] = data

	require.Error(t, NewTrie(ctx).Verify())
}

func TestVerifyRejectsOrphanBucketRecordInEmptyTrie(t *testing.T) {
	address := bytes.Repeat([]byte{0x44}, 20)
	key := eip8297.TreeKeyStorage(address, storageSlot(64))
	value := eip8297.EncodeStorageValue([]byte{1})
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: key, Value: value}})
	require.NoError(t, err)
	bucketKey, err := bucketKeyForStorage(key)
	require.NoError(t, err)
	bucketRecord := bytes.Clone(ctx.records[string(bucketKey)])
	ctx.records = map[string][]byte{string(bucketKey): bucketRecord}

	err = NewTrie(ctx).Verify()
	require.Error(t, err)
	require.ErrorContains(t, err, "orphan bucket record")
}

func TestVerifyRejectsOrphanBucketRecordBesideLiveState(t *testing.T) {
	address := bytes.Repeat([]byte{0x55}, 20)
	key := accountKey(0, eip8297.BasicDataLeafKey)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process([]Op{{Key: key, Value: testTrieValue(1)}})
	require.NoError(t, err)

	orphanContext := newTrieTestContext()
	orphanKey := eip8297.TreeKeyStorage(address, storageSlot(64))
	_, err = NewTrie(orphanContext).Process([]Op{{Key: orphanKey, Value: eip8297.EncodeStorageValue([]byte{2})}})
	require.NoError(t, err)
	orphanBucket, err := bucketKeyForStorage(orphanKey)
	require.NoError(t, err)
	ctx.records[string(orphanBucket)] = bytes.Clone(orphanContext.records[string(orphanBucket)])

	err = NewTrie(ctx).Verify()
	require.Error(t, err)
	require.ErrorContains(t, err, "orphan bucket record")
}

func TestVerifyIgnoresTombstoneBucketRecord(t *testing.T) {
	address := bytes.Repeat([]byte{0x66}, 20)
	key := eip8297.TreeKeyStorage(address, storageSlot(64))
	ctx := newTrieTestContext()
	ctx.keepTombstones = true
	_, err := NewTrie(ctx).Process([]Op{{Key: key, Value: eip8297.EncodeStorageValue([]byte{1})}})
	require.NoError(t, err)
	_, err = NewTrie(ctx).Process([]Op{{Key: key}})
	require.NoError(t, err)
	require.NoError(t, NewTrie(ctx).Verify())
}
