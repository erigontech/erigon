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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestBranchCachePutChildrenRoundTrips(t *testing.T) {
	nodeKey := []byte{0x2a, 0xf0}
	var records [16][]byte
	records[0] = []byte{1}
	records[3] = []byte{2, 2, 2, 2, 2, 2, 2}
	records[9] = []byte{3, 3}
	records[15] = []byte{4, 4, 4}
	present := uint16(1)<<0 | uint16(1)<<3 | uint16(1)<<9 | uint16(1)<<15

	var steps, txNums [16]uint64
	for nibble := range 16 {
		steps[nibble] = uint64(nibble) + 1
		txNums[nibble] = uint64(nibble)*100 + 7
	}

	c := NewBranchCache(1024)
	c.PutChildren(nodeKey, present, &records, &steps, &txNums)

	var got [16][]byte
	gotPresent, step, ok := c.GetNode(nodeKey, ^uint16(0), &got)
	require.True(t, ok)
	require.Equal(t, present, gotPresent)
	require.Equal(t, uint64(16), step, "the node reports its newest child's step")
	for nibble := range 16 {
		require.Equalf(t, records[nibble], got[nibble], "nibble %d", nibble)
	}

	// Records share one buffer, so appending to one must allocate rather than write into the next.
	_ = append(got[0], 0xff, 0xff, 0xff, 0xff)
	_, _, ok = c.GetNode(nodeKey, ^uint16(0), &got)
	require.True(t, ok)
	require.Equal(t, records[3], got[3], "a neighbour record was overwritten through the shared buffer")
}

// A publish carries only the records that changed. Overwriting the entry with just those would
// send every later read of the node's other children back to the db.
func TestBranchCachePutChildrenMergesWithExisting(t *testing.T) {
	nodeKey := []byte{0x2a, 0xf0}
	var steps, txNums [16]uint64
	c := NewBranchCache(1024)

	var first [16][]byte
	first[1] = []byte{0xa1}
	first[4] = []byte{0xa4}
	c.PutChildren(nodeKey, 1<<1|1<<4, &first, &steps, &txNums)

	var second [16][]byte
	second[4] = []byte{0xb4}
	second[7] = []byte{0xb7}
	c.PutChildren(nodeKey, 1<<4|1<<7, &second, &steps, &txNums)

	var got [16][]byte
	present, _, ok := c.GetNode(nodeKey, ^uint16(0), &got)
	require.True(t, ok)
	require.Equal(t, uint16(1<<1|1<<4|1<<7), present, "the untouched sibling must survive the second put")
	require.Equal(t, []byte{0xa1}, got[1], "untouched sibling")
	require.Equal(t, []byte{0xb4}, got[4], "rewritten child takes the newer value")
	require.Equal(t, []byte{0xb7}, got[7], "new child")
}

// An empty record is a tombstone the cache must not serve as a value.
func TestBranchCachePutChildrenSkipsEmptyRecords(t *testing.T) {
	nodeKey := []byte{0x11, 0xf0}
	var records [16][]byte
	records[2] = []byte{}
	records[5] = []byte{9}
	var steps, txNums [16]uint64

	c := NewBranchCache(1024)
	c.PutChildren(nodeKey, 1<<2|1<<5, &records, &steps, &txNums)

	var got [16][]byte
	present, _, ok := c.GetNode(nodeKey, ^uint16(0), &got)
	require.True(t, ok)
	require.Zero(t, present&(1<<2), "an empty record must not be cached")
	require.Equal(t, []byte{9}, got[5])
}

// An edge record for P->n and the node P||n index the same nibble path, so they land in the same
// trunk slot. Get must never hand back the node's blob as if it were that record's value.
func TestBranchCacheGetRejectsNodeEntries(t *testing.T) {
	c := NewBranchCache(1024, true)
	defer c.Close()
	c.maxDepth = trunkDepthFull
	c.accountTrunk = newAccountTrunk(trunkDepthFull)

	parent := []byte{4, 1}
	child := byte(6)
	recordKey := v3RecordKey(parent, child)
	nodeKey := nibbles.EncodeKeyV3(append(append([]byte{}, parent...), child))
	require.Equal(t, c.v3TrunkSlot(recordKey, true), c.v3NodeTrunkSlot(nodeKey, true),
		"this test is only meaningful while the two share a slot")

	var records [16][]byte
	records[3] = []byte{7, 7, 7}
	var steps, txNums [16]uint64
	c.PutChildren(nodeKey, 1<<3, &records, &steps, &txNums)

	_, _, ok := c.Get(recordKey)
	require.False(t, ok, "a node entry must not be served as a single record's value")
}
