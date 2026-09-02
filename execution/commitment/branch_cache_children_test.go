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
)

func childKeyOf(nodeKey []byte, nibble int) []byte {
	key := make([]byte, len(nodeKey)+1)
	copy(key, nodeKey)
	key[len(nodeKey)] = 0x80 | byte(nibble)
	return key
}

// PutChildren shares one buffer across a node's records, so it has to be indistinguishable from a
// Put per child -- including that a record handed back by Get cannot grow into its neighbour.
func TestBranchCachePutChildrenMatchesPerChildPut(t *testing.T) {
	nodeKey := []byte{0x2a, 0xf0}
	records := [16][]byte{}
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

	batched := NewBranchCache(1024)
	batched.PutChildren(nodeKey, present, &records, &steps, &txNums)

	perChild := NewBranchCache(1024)
	for nibble := range 16 {
		if present&(uint16(1)<<nibble) == 0 {
			continue
		}
		perChild.Put(childKeyOf(nodeKey, nibble), records[nibble], steps[nibble], txNums[nibble])
	}

	for nibble := range 16 {
		key := childKeyOf(nodeKey, nibble)
		wantData, wantStep, wantOK := perChild.Get(key)
		gotData, gotStep, gotOK := batched.Get(key)
		require.Equalf(t, wantOK, gotOK, "presence for nibble %d", nibble)
		require.Equalf(t, wantData, gotData, "data for nibble %d", nibble)
		require.Equalf(t, wantStep, gotStep, "step for nibble %d", nibble)
		if present&(uint16(1)<<nibble) != 0 {
			require.Truef(t, gotOK, "nibble %d must be cached", nibble)
		}
	}

	// Appending to one record must allocate rather than write into the next record's bytes.
	first, _, ok := batched.Get(childKeyOf(nodeKey, 0))
	require.True(t, ok)
	_ = append(first, 0xff, 0xff, 0xff, 0xff)
	next, _, ok := batched.Get(childKeyOf(nodeKey, 3))
	require.True(t, ok)
	require.Equal(t, records[3], next, "a neighbour record was overwritten through the shared buffer")
}

// An empty record is a tombstone the cache must not serve as a value, matching Put's contract.
func TestBranchCachePutChildrenSkipsEmptyRecords(t *testing.T) {
	nodeKey := []byte{0x11, 0xf0}
	records := [16][]byte{}
	records[2] = []byte{}
	records[5] = []byte{9}
	present := uint16(1)<<2 | uint16(1)<<5

	var steps, txNums [16]uint64
	cache := NewBranchCache(1024)
	cache.PutChildren(nodeKey, present, &records, &steps, &txNums)

	_, _, ok := cache.Get(childKeyOf(nodeKey, 2))
	require.False(t, ok, "an empty record must not be cached")
	data, _, ok := cache.Get(childKeyOf(nodeKey, 5))
	require.True(t, ok)
	require.Equal(t, []byte{9}, data)
}
