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

package witness

import (
	"bytes"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinDriverBuildsEmbeddingInOrder(t *testing.T) {
	pbinUseBlake3(t)
	addressA := []byte{8}
	addressB := []byte{9}
	addressC := []byte{10}
	slot0 := make([]byte, 32)
	slot64 := pbinSlot(64)
	storage0 := pbinValueBytes(1)
	storage64 := pbinValueBytes(2)
	code := bytes.Repeat([]byte{0x60}, 40)
	codeHash := common.Hash(keccak.Sum256(code))
	codeHashValue := eip8297.CodeHashValue(codeHash)
	delegation := append([]byte{0xef, 0x01, 0x00}, make([]byte, eip8297.DelegationCodeLength-3)...)
	delegationValue := eip8297.EncodeDelegation(delegation)
	tree := NewPBinEmptyTree()
	root, resolved, err := tree.Apply(PBinDriverInput{
		Storage: []PBinStorageWrite{
			{Address: addressB, Slot: slot64, Value: storage64},
			{Address: addressA, Slot: slot0, Value: storage0},
		},
		Accounts: []PBinAccountUpdate{
			{Address: addressC, Values: map[byte][]byte{eip8297.BasicDataLeafKey: pbinValueBytes(5)}, Code: code},
			{Address: addressA, Values: map[byte][]byte{eip8297.BasicDataLeafKey: pbinValueBytes(3)}, Delegation: delegation},
			{Address: addressB, Values: map[byte][]byte{eip8297.BasicDataLeafKey: pbinValueBytes(4)}, Code: code},
		},
	})
	require.NoError(t, err)
	require.Empty(t, resolved)
	want := []eip8297.Entry{
		{Key: eip8297.TreeKeyStorage(addressA, slot0), Value: storage0},
		{Key: eip8297.TreeKeyStorage(addressB, slot64), Value: storage64},
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: pbinValueBytes(3)},
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.DelegationLeafKey), Value: delegationValue[:]},
		{Key: eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey), Value: pbinValueBytes(4)},
		{Key: eip8297.TreeKeyAccount(addressB, eip8297.CodeHashLeafKey), Value: codeHashValue[:]},
		{Key: eip8297.TreeKeyAccount(addressC, eip8297.BasicDataLeafKey), Value: pbinValueBytes(5)},
		{Key: eip8297.TreeKeyAccount(addressC, eip8297.CodeHashLeafKey), Value: codeHashValue[:]},
	}
	for chunkID, chunk := range eip8297.ChunkifyCode(code) {
		want = append(want, eip8297.Entry{Key: eip8297.TreeKeyCodeChunk(codeHash, chunkID), Value: chunk[:]})
	}
	require.Equal(t, pbinReferenceRoot(want), root)

	root, _, err = tree.Apply(PBinDriverInput{
		Storage:  []PBinStorageWrite{{Address: addressA, Slot: slot0, Value: make([]byte, eip8297.ValueLength)}},
		Accounts: []PBinAccountUpdate{{Address: addressA, Code: code}},
	})
	require.NoError(t, err)
	want = pbinRemoveEntry(want, eip8297.TreeKeyStorage(addressA, slot0))
	want = pbinRemoveEntry(want, eip8297.TreeKeyAccount(addressA, eip8297.DelegationLeafKey))
	want = append(want, eip8297.Entry{Key: eip8297.TreeKeyAccount(addressA, eip8297.CodeHashLeafKey), Value: codeHashValue[:]})
	for chunkID, chunk := range eip8297.ChunkifyCode(code) {
		want = append(want, eip8297.Entry{Key: eip8297.TreeKeyCodeChunk(codeHash, chunkID), Value: chunk[:]})
	}
	require.Equal(t, pbinReferenceRoot(pbinUniqueEntries(want)), root)
}

func TestPBinDriverReadsBeforeWrites(t *testing.T) {
	pbinUseBlake3(t)
	key := eip8297.TreeKeyAccount([]byte{11}, 0)
	value := pbinValueBytes(1)
	tree, calls := pbinLazyTree(t, []eip8297.Entry{{Key: key, Value: value}})
	root, resolved, err := tree.Apply(PBinDriverInput{Reads: [][]byte{key}, Accounts: []PBinAccountUpdate{{Address: []byte{11}, Values: map[byte][]byte{0: pbinValueBytes(2)}}}})
	require.NoError(t, err)
	require.NotEmpty(t, resolved)
	require.GreaterOrEqual(t, len(calls), 1)
	require.NotEqual(t, pbinReferenceRoot([]eip8297.Entry{{Key: key, Value: value}}), root)
}

func TestPBinDriverDeletesZeroAccountValue(t *testing.T) {
	pbinUseBlake3(t)
	key := eip8297.TreeKeyAccount([]byte{12}, eip8297.BasicDataLeafKey)
	tree := NewPBinEmptyTree()
	_, _, err := tree.Apply(PBinDriverInput{Accounts: []PBinAccountUpdate{{Address: []byte{12}, Values: map[byte][]byte{eip8297.BasicDataLeafKey: pbinValueBytes(1)}}}})
	require.NoError(t, err)
	root, _, err := tree.Apply(PBinDriverInput{Accounts: []PBinAccountUpdate{{Address: []byte{12}, Values: map[byte][]byte{eip8297.BasicDataLeafKey: make([]byte, eip8297.ValueLength)}}}})
	require.NoError(t, err)
	want := NewPBinEmptyTree().RootHash()
	require.Equal(t, want, root)
	_, present, err := tree.Read(key)
	require.NoError(t, err)
	require.False(t, present)
}

func pbinRemoveEntry(entries []eip8297.Entry, key []byte) []eip8297.Entry {
	result := entries[:0]
	for _, entry := range entries {
		if !bytes.Equal(entry.Key, key) {
			result = append(result, entry)
		}
	}
	return result
}

func pbinUniqueEntries(entries []eip8297.Entry) []eip8297.Entry {
	seen := make(map[string]struct{}, len(entries))
	result := make([]eip8297.Entry, 0, len(entries))
	for _, entry := range entries {
		if _, ok := seen[string(entry.Key)]; ok {
			continue
		}
		seen[string(entry.Key)] = struct{}{}
		result = append(result, entry)
	}
	return result
}
