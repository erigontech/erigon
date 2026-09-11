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

package commitment

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func v3RecordKey(path []byte, child byte) []byte {
	return nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), child)
}

// The trunk indexes a legacy compact key, whose first byte is a flags byte. A v3 key carries path
// nibbles there instead, so two nodes that differ only in their first two nibbles must not land in
// the same slot and read back each other's records.
func TestBranchCacheKeepsV3RecordKeysDistinct(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()

	first := v3RecordKey([]byte{2, 9}, 5)
	second := v3RecordKey([]byte{8, 5}, 5)
	require.NotEqual(t, first, second)

	c.Put(first, []byte("edge-29-5"), 1, 1)
	c.Put(second, []byte("edge-85-5"), 1, 1)

	for _, tc := range []struct {
		key  []byte
		want string
	}{{first, "edge-29-5"}, {second, "edge-85-5"}} {
		nodeKey, nibble, ok := v3NodeKeyOf(tc.key)
		require.True(t, ok)
		var records [16][]byte
		present, _, ok := c.GetNode(nodeKey, ^uint16(0), &records)
		require.Truef(t, ok, "record %x was evicted", tc.key)
		require.NotZerof(t, present&(uint16(1)<<nibble), "record %x was evicted", tc.key)
		require.Equalf(t, []byte(tc.want), records[nibble], "record %x reads back another node's value", tc.key)
	}
}

// A node's children share one entry, so they must still come back individually addressable.
func TestBranchCacheNodeEntryKeepsChildrenDistinct(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()

	path := []byte{4, 1, 7}
	for child := range 16 {
		c.Put(v3RecordKey(path, byte(child)), []byte{byte(child), 0xaa}, uint64(child), uint64(child))
	}
	var records [16][]byte
	present, step, ok := c.GetNode(nibbles.EncodeKeyV3(path), ^uint16(0), &records)
	require.True(t, ok)
	require.Equal(t, ^uint16(0), present, "every child written must be present")
	require.Equal(t, uint64(15), step, "the node reports the newest child's step")
	for child := range 16 {
		require.Equalf(t, []byte{byte(child), 0xaa}, records[child], "child %d", child)
	}

	// Dropping one child must leave the rest in place.
	c.Invalidate(v3RecordKey(path, 7))
	present, _, ok = c.GetNode(nibbles.EncodeKeyV3(path), ^uint16(0), &records)
	require.True(t, ok)
	require.Zero(t, present&(1<<7), "the invalidated child must be gone")
	require.Equal(t, ^uint16(0)&^uint16(1<<7), present, "its siblings must survive")
}

// Every edge down to depth 4 owns a trunk slot of its own, so no pair of them can alias.
func TestBranchCacheV3EdgeKeysRouteToDistinctSlots(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()
	// A parallel package run leaves enough caches alive to shrink the trunk; depth 4 is the case
	// under test, so pin it rather than let the adaptive depth decide.
	c.maxDepth = trunkDepthFull
	c.accountTrunk = newAccountTrunk(trunkDepthFull)
	c.edgeTrunk = newAccountTrunk(trunkDepthFull)

	// Compared by address: two empty slots are deep-equal, only identity distinguishes them.
	seen := make(map[string][]byte, 16+256+4096+65536)
	for _, path := range v3PathsUpTo(3) {
		for child := range byte(16) {
			key := v3RecordKey(path, child)
			slot := c.v3TrunkSlot(key, true)
			require.NotNilf(t, slot, "record %x has no trunk slot", key)
			addr := fmt.Sprintf("%p", slot)
			require.NotContainsf(t, seen, addr, "record %x shares a slot with %x", key, seen[addr])
			seen[addr] = key
		}
	}
}

// A storage record has to name the contract it belongs to: the pin controller budgets by that hash,
// and a wrong one both misattributes misses and lets two contracts share a pinned trunk.
func TestBranchCacheV3StorageRecordNamesItsContract(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()

	account := make([]byte, 64)
	for i := range account {
		account[i] = byte(i%15) + 1
	}
	path := append(append([]byte{}, account...), 7, 2)
	got, ok := c.ContractHash(v3RecordKey(path, 5))
	require.True(t, ok)

	var want [32]byte
	for i := range want {
		want[i] = account[2*i]<<4 | account[2*i+1]
	}
	require.Equal(t, want, got)

	_, ok = c.ContractHash(v3RecordKey(account[:60], 5))
	require.False(t, ok, "an account-trie record is not storage")
}

func v3PathsUpTo(maxLen int) [][]byte {
	paths := [][]byte{{}}
	for length := 1; length <= maxLen; length++ {
		prev := paths
		for _, path := range prev {
			if len(path) != length-1 {
				continue
			}
			for nibble := range byte(16) {
				next := make([]byte, len(path), len(path)+1)
				copy(next, path)
				paths = append(paths, append(next, nibble))
			}
		}
	}
	return paths
}

func TestBranchCacheV3CachesOnlyWrittenChildren(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()

	path := []byte{6, 2}
	nodeKey := nibbles.EncodeKeyV3(path)

	c.Put(v3RecordKey(path, 3), []byte("child-3"), 4, 40)

	var records [16][]byte
	present, _, ok := c.GetNode(nodeKey, ^uint16(0), &records)
	require.True(t, ok)
	require.Equal(t, uint16(1)<<3, present,
		"a node must cache the one child written, not its unwritten siblings")

	c.Put(v3RecordKey(path, 9), []byte("child-9"), 5, 50)

	present, step, ok := c.GetNode(nodeKey, ^uint16(0), &records)
	require.True(t, ok)
	require.Equal(t, uint16(1)<<3|uint16(1)<<9, present,
		"a later write must not drop the sibling written before it")
	require.Equal(t, []byte("child-3"), records[3])
	require.Equal(t, []byte("child-9"), records[9])
	require.Equal(t, uint64(5), step, "the node reports the newest child's step")

	var narrow [16][]byte
	present, _, ok = c.GetNode(nodeKey, uint16(1)<<9, &narrow)
	require.True(t, ok)
	require.Equal(t, uint16(1)<<9, present, "an unfold must get only the nibble it asked for")
	require.Nil(t, narrow[3], "a masked-out sibling must not be materialised")
	require.Equal(t, []byte("child-9"), narrow[9])
}
