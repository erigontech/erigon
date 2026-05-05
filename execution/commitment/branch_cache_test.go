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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBranchCache_RootPinning verifies the root branch lands in the pinned
// slot (counted as root-hit) and tail entries land in the LRU tier
// (counted as tail-hit).
func TestBranchCache_RootPinning(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00} // compact-encoded empty nibble path = root branch
	deepKey := []byte{0x12, 0x34, 0x56}
	c.Put(rootKey, []byte("root-data"))
	c.Put(deepKey, []byte("deep-data"))

	// Root reads should increment rootHits, not tailHits
	got, ok := c.Get(rootKey)
	require.True(t, ok)
	require.Equal(t, []byte("root-data"), got)
	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())

	// Deep reads should increment tailHits, not rootHits
	got, ok = c.Get(deepKey)
	require.True(t, ok)
	require.Equal(t, []byte("deep-data"), got)
	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())
}

// TestBranchCache_RootSurvivesEvictionPressure verifies that pinned root
// entry is not subject to LRU eviction even if the tail fills past
// capacity many times over.
func TestBranchCache_RootSurvivesEvictionPressure(t *testing.T) {
	c := NewBranchCache(10) // very small tail
	rootKey := []byte{0x00}
	c.Put(rootKey, []byte("ROOT-PERSISTS"))

	// Stuff the tail well past capacity
	for i := 0; i < 100; i++ {
		c.Put([]byte{byte(i), byte(i)}, []byte{byte(i)})
	}

	// Root must still be there
	got, ok := c.Get(rootKey)
	require.True(t, ok, "root should never be evicted from pinned slot")
	require.Equal(t, []byte("ROOT-PERSISTS"), got)

	// Tail at capacity (10), not 100
	require.LessOrEqual(t, c.tail.Len(), 10, "tail should respect LRU capacity")
}

// TestBranchCache_DirtyFlag verifies PutIfClean refuses overwrite of a
// dirty entry, while Put unconditionally replaces (and the new entry
// starts clean).
func TestBranchCache_DirtyFlag(t *testing.T) {
	c := NewBranchCache(100)
	key := []byte{0x12, 0x34}

	require.True(t, c.PutIfClean(key, []byte("v1")))
	c.MarkDirty(key)

	require.False(t, c.PutIfClean(key, []byte("v2")), "PutIfClean must refuse dirty entry")
	got, _ := c.Get(key)
	require.Equal(t, []byte("v1"), got, "dirty entry's data preserved")

	// Unconditional Put replaces
	c.Put(key, []byte("v3"))
	got, _ = c.Get(key)
	require.Equal(t, []byte("v3"), got)

	// New entry is clean
	require.True(t, c.PutIfClean(key, []byte("v4")))
}

// TestBranchCache_GetDecoded verifies the lazy-decode read path for a
// real encoded branch (round-trip with BranchEncoder).
func TestBranchCache_GetDecoded(t *testing.T) {
	c := NewBranchCache(100)

	row, bm := generateCellRow(t, 16)
	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)

	prefix := []byte{0x12, 0x34}
	c.Put(prefix, enc)

	// First decoded-read decodes lazily
	bitmap, cells, ok := c.GetDecoded(prefix)
	require.True(t, ok)
	require.Equal(t, bm, bitmap)
	require.NotNil(t, cells)

	// Second call returns same cells pointer
	_, cells2, ok := c.GetDecoded(prefix)
	require.True(t, ok)
	require.Same(t, cells, cells2, "decoded form cached and reused")

	// Encoded form unchanged after decoded reads
	encGot, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte(enc), encGot)
}

// TestBranchCache_Invalidate removes entries from both tiers.
func TestBranchCache_Invalidate(t *testing.T) {
	c := NewBranchCache(100)
	rootKey := []byte{0x00}
	deepKey := []byte{0x12, 0x34}
	c.Put(rootKey, []byte("r"))
	c.Put(deepKey, []byte("d"))

	c.Invalidate(rootKey)
	_, ok := c.Get(rootKey)
	require.False(t, ok, "root invalidated")

	c.Invalidate(deepKey)
	_, ok = c.Get(deepKey)
	require.False(t, ok, "deep invalidated")
}

// TestBranchCache_Clear empties everything and resets stats.
func TestBranchCache_Clear(t *testing.T) {
	c := NewBranchCache(100)
	c.Put([]byte{0x00}, []byte("r"))
	c.Put([]byte{0x12}, []byte("d"))
	c.Get([]byte{0x00})
	c.Get([]byte{0x12})

	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())

	c.Clear()
	require.Equal(t, uint64(0), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())
	_, ok := c.Get([]byte{0x00})
	require.False(t, ok)
	_, ok = c.Get([]byte{0x12})
	require.False(t, ok)
}

// TestBranchCache_Stats verifies the format of the stats string is
// deterministic and contains the expected per-tier counts.
func TestBranchCache_Stats(t *testing.T) {
	c := NewBranchCache(100)
	c.Put([]byte{0x00}, []byte("rrr"))
	c.Put([]byte{0x12, 0x34}, []byte("ddd"))
	c.Get([]byte{0x00})
	c.Get([]byte{0x12, 0x34})
	c.Get([]byte{0xff}) // tail miss

	s := c.Stats()
	for _, want := range []string{
		"root hit=1 miss=0",
		"tail hit=1 miss=1",
		"tail entries=1", // we put 1 deep entry; root not counted in tail
	} {
		require.Contains(t, s, want, "Stats output: %s", s)
	}
	// Sanity: format doesn't blow up if we read it
	require.True(t, strings.HasPrefix(s, "branch-cache "))
}
