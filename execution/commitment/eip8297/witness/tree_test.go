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
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinTreeRandomLeafSets(t *testing.T) {
	pbinUseBlake3(t)
	for _, entries := range pbinTreeTestEntries() {
		tree := NewPBinEmptyTree()
		for index, entry := range entries {
			require.NoError(t, tree.Put(entry.Key, entry.Value))
			wantAfter := eip8297.StateRootWithHash(entries[:index+1], func(preimage []byte) common.Hash {
				sum := blake3.Sum256(preimage)
				return common.Hash(sum)
			})
			require.Equal(t, wantAfter, tree.RootHash(), "incremental root comparison at %d", index)
		}
		want := eip8297.StateRootWithHash(entries, func(preimage []byte) common.Hash {
			sum := blake3.Sum256(preimage)
			return common.Hash(sum)
		})
		require.Equal(t, want, tree.RootHash(), "root comparison for %d leaves", len(entries))
	}
}

func TestPBinTreeLazyReadsAndOperations(t *testing.T) {
	pbinUseBlake3(t)
	address := []byte{1}
	other := []byte{2}
	key0 := eip8297.TreeKeyAccount(address, 0)
	key1 := eip8297.TreeKeyAccount(address, 1)
	storage := eip8297.TreeKeyStorage(address, pbinSlot(64))
	otherKey := eip8297.TreeKeyAccount(other, 0)
	entries := []eip8297.Entry{
		{Key: key0, Value: pbinValueBytes(1)},
		{Key: key1, Value: pbinValueBytes(2)},
		{Key: storage, Value: pbinValueBytes(3)},
	}
	tree, calls := pbinLazyTree(t, entries)
	value, ok, err := tree.Read(key0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, entries[0].Value, value)
	firstCalls := len(calls)
	_, ok, err = tree.Read(key0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, calls, firstCalls)
	require.NoError(t, tree.Put(key0, pbinValueBytes(4)))
	require.NoError(t, tree.Put(otherKey, pbinValueBytes(5)))
	require.NoError(t, tree.Delete(storage))
	require.NoError(t, tree.Delete(key1))
	wantEntries := []eip8297.Entry{{Key: key0, Value: pbinValueBytes(4)}, {Key: otherKey, Value: pbinValueBytes(5)}}
	require.Equal(t, pbinReferenceRoot(wantEntries), tree.RootHash())
}

func TestPBinTreeGroupShapeTransitions(t *testing.T) {
	pbinUseBlake3(t)
	key0 := eip8297.TreeKeyAccount([]byte{3}, 0)
	key1 := eip8297.TreeKeyAccount([]byte{3}, 1)
	tree := NewPBinEmptyTree()
	require.NoError(t, tree.Put(key0, pbinValueBytes(1)))
	require.NoError(t, tree.Put(key1, pbinValueBytes(2)))
	require.NoError(t, tree.Delete(key0))
	require.Equal(t, pbinReferenceRoot([]eip8297.Entry{{Key: key1, Value: pbinValueBytes(2)}}), tree.RootHash())
}

func TestPBinTreeCollapseRehashesGroup(t *testing.T) {
	pbinUseBlake3(t)
	key0 := eip8297.TreeKeyAccount([]byte{4}, 0)
	key1 := eip8297.TreeKeyAccount([]byte{5}, 0)
	key2 := eip8297.TreeKeyAccount([]byte{5}, 1)
	tree, _ := pbinLazyTree(t, []eip8297.Entry{{Key: key0, Value: pbinValueBytes(1)}, {Key: key1, Value: pbinValueBytes(2)}, {Key: key2, Value: pbinValueBytes(3)}})
	require.NoError(t, tree.Delete(key0))
	require.Equal(t, pbinReferenceRoot([]eip8297.Entry{{Key: key1, Value: pbinValueBytes(2)}, {Key: key2, Value: pbinValueBytes(3)}}), tree.RootHash())
}

func TestPBinTreeAccountDeletionStopsAtOverflowCutPoint(t *testing.T) {
	pbinUseBlake3(t)
	address := []byte{6}
	other := []byte{7}
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address, 0), Value: pbinValueBytes(1)},
		{Key: eip8297.TreeKeyAccount(address, 1), Value: pbinValueBytes(2)},
		{Key: eip8297.TreeKeyStorage(address, pbinSlot(64)), Value: pbinValueBytes(3)},
		{Key: eip8297.TreeKeyStorage(address, pbinSlot(65)), Value: pbinValueBytes(4)},
		{Key: eip8297.TreeKeyStorage(address, pbinSlotNumber(320)), Value: pbinValueBytes(6)},
		{Key: eip8297.TreeKeyAccount(other, 0), Value: pbinValueBytes(5)},
	}
	tree, calls := pbinLazyTree(t, entries)
	require.NoError(t, tree.DeleteAccount(address))
	require.Equal(t, pbinReferenceRoot([]eip8297.Entry{{Key: eip8297.TreeKeyAccount(other, 0), Value: pbinValueBytes(5)}}), tree.RootHash())
	var cache eip8297.DigestCache
	cut := eip8297.PathFromBytes(cache.AccountStoragePrefix(address))
	for encoded := range calls {
		if len(encoded) == 0 {
			continue
		}
		bitLength := int16(binary.BigEndian.Uint16([]byte(encoded)))
		walk := eip8297.PathFromBits([]byte(encoded)[2:], bitLength)
		require.False(t, walk.HasPrefix(&cut), "resolved below overflow cut point")
	}
}

func TestPBinTreeCreatedSurvivorIsNotResolvedAgain(t *testing.T) {
	pbinUseBlake3(t)
	keyA := pbinSyntheticAccountKey(0x00)
	keyB := pbinSyntheticAccountKey(0x80)
	keyC := pbinSyntheticAccountKey(0xc0)
	tree, calls := pbinLazyTree(t, []eip8297.Entry{{Key: keyA, Value: pbinValueBytes(1)}, {Key: keyB, Value: pbinValueBytes(2)}})
	_, ok, err := tree.Read(keyA)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, tree.Put(keyC, pbinValueBytes(3)))
	resolved := len(calls)
	resolvedNodes := len(tree.Resolved())
	require.NoError(t, tree.Delete(keyA))
	require.Len(t, calls, resolved)
	require.Len(t, tree.Resolved(), resolvedNodes)
}

func TestPBinTreeChecksPointersAndGroupPositions(t *testing.T) {
	pbinUseBlake3(t)
	key0 := eip8297.TreeKeyAccount([]byte{12}, 0)
	key1 := eip8297.TreeKeyAccount([]byte{13}, 0)
	eager := NewPBinEmptyTree()
	require.NoError(t, eager.Put(key0, pbinValueBytes(1)))
	require.NoError(t, eager.Put(key1, pbinValueBytes(2)))
	store := pbinStoreTree(t, eager)
	corruptTree, err := NewPBinTree(eager.RootHash(), func(path []byte) ([]byte, error) {
		blob := slices.Clone(store[string(path)])
		if len(path) > 0 {
			blob[len(blob)-1] ^= 1
		}
		return blob, nil
	})
	require.NoError(t, err)
	_, _, err = corruptTree.Read(key0)
	require.ErrorContains(t, err, "hashes to")

	sameStem := eip8297.TreeKeyAccount([]byte{14}, 0)
	sameStem1 := eip8297.TreeKeyAccount([]byte{14}, 1)
	groupTree := NewPBinEmptyTree()
	require.NoError(t, groupTree.Put(sameStem, pbinValueBytes(3)))
	require.NoError(t, groupTree.Put(sameStem1, pbinValueBytes(4)))
	groupStore := pbinStoreTree(t, groupTree)
	groupBlob := slices.Clone(groupStore[""])
	groupBlob[2] = 1
	groupHash := PBinHashBlobMust(groupBlob)
	_, err = NewPBinTree(groupHash, func(path []byte) ([]byte, error) { return groupBlob, nil })
	require.ErrorContains(t, err, "group position")
}

func pbinLazyTree(t *testing.T, entries []eip8297.Entry) (*PBinTree, map[string]int) {
	t.Helper()
	eager := NewPBinEmptyTree()
	for _, entry := range entries {
		require.NoError(t, eager.Put(entry.Key, entry.Value))
	}
	store := pbinStoreTree(t, eager)
	calls := make(map[string]int)
	lazy, err := NewPBinTree(eager.RootHash(), func(path []byte) ([]byte, error) {
		calls[string(path)]++
		blob, ok := store[string(path)]
		if !ok {
			t.Fatalf("missing path %x", path)
		}
		return blob, nil
	})
	require.NoError(t, err)
	return lazy, calls
}

func pbinStoreTree(t *testing.T, tree *PBinTree) map[string][]byte {
	t.Helper()
	store := make(map[string][]byte)
	var visit func(*pbinChild) common.Hash
	visit = func(child *pbinChild) common.Hash {
		if child == nil {
			return common.Hash{}
		}
		node := child.node
		if node.group != nil {
			var blob []byte
			var err error
			if len(node.group.Subs) == 1 {
				key := append(slices.Clone(node.group.Stem), node.group.Subs[0])
				blob, err = PBinEncodeLeaf(key, node.group.Values[0])
			} else {
				blob, err = PBinEncodeGroup(*node.group)
			}
			require.NoError(t, err)
			store[string(PBinPath(&node.walk))] = blob
			return PBinHashBlobMust(blob)
		}
		left := visit(node.branch.left)
		right := visit(node.branch.right)
		blob, err := PBinEncodeBranch(&node.branch.prefix, &left, &right)
		require.NoError(t, err)
		store[string(PBinPath(&node.walk))] = blob
		return PBinHashBlobMust(blob)
	}
	visit(tree.root)
	return store
}

func PBinHashBlobMust(blob []byte) common.Hash {
	hash, err := PBinHashBlob(blob)
	if err != nil {
		panic(err)
	}
	return hash
}

func pbinReferenceRoot(entries []eip8297.Entry) common.Hash {
	return eip8297.StateRootWithHash(entries, func(preimage []byte) common.Hash {
		sum := blake3.Sum256(preimage)
		return common.Hash(sum)
	})
}

func pbinValueBytes(seed byte) []byte {
	value := make([]byte, eip8297.ValueLength)
	value[31] = seed
	return value
}

func pbinSyntheticAccountKey(seed byte) []byte {
	key := make([]byte, eip8297.AccountKeyLength)
	key[1] = seed
	return key
}

func pbinTreeTestEntries() [][]eip8297.Entry {
	rng := rand.New(rand.NewSource(0x8297))
	entries := make([][]eip8297.Entry, 0, 3)
	for range 3 {
		var current []eip8297.Entry
		for group, count := range []int{1, 2, 17, 64, 128, 256} {
			address := make([]byte, 20)
			rng.Read(address)
			for sub := range count {
				value := make([]byte, eip8297.ValueLength)
				rng.Read(value)
				current = append(current, eip8297.Entry{
					Key:   eip8297.TreeKeyAccount(address, byte(sub)),
					Value: value,
				})
			}
			if group == 0 {
				current = append(current,
					eip8297.Entry{Key: eip8297.TreeKeyStorage(address, pbinSlot(64)), Value: pbinValue(rng)},
					eip8297.Entry{Key: eip8297.TreeKeyCodeChunk(pbinHash(1), 0), Value: pbinValue(rng)},
				)
			}
		}
		entries = append(entries, current)
	}
	return entries
}

func pbinValue(rng *rand.Rand) []byte {
	value := make([]byte, eip8297.ValueLength)
	_, _ = rng.Read(value)
	return value
}

func pbinSlot(value byte) []byte {
	return pbinSlotNumber(uint64(value))
}

func pbinSlotNumber(value uint64) []byte {
	slot := make([]byte, 32)
	binary.BigEndian.PutUint64(slot[24:], value)
	return slot
}

func pbinHash(value byte) common.Hash {
	var hash common.Hash
	hash[31] = value
	return hash
}
