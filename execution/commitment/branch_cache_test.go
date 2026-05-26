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
	c.Put(rootKey, []byte("root-data"), 0, 0, "test")
	c.Put(deepKey, []byte("deep-data"), 0, 0, "test")

	// Root reads should increment rootHits, not tailHits
	got, _, ok := c.Get(rootKey)
	require.True(t, ok)
	require.Equal(t, []byte("root-data"), got)
	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())

	// Deep reads should increment tailHits, not rootHits
	got, _, ok = c.Get(deepKey)
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
	c.Put(rootKey, []byte("ROOT-PERSISTS"), 0, 0, "test")

	// Stuff the tail well past capacity
	for i := 0; i < 100; i++ {
		c.Put([]byte{byte(i), byte(i)}, []byte{byte(i)}, 0, 0, "test")
	}

	// Root must still be there
	got, _, ok := c.Get(rootKey)
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

	require.True(t, c.PutIfClean(key, []byte("v1"), 0, 0, "test"))
	c.MarkDirty(key)

	require.False(t, c.PutIfClean(key, []byte("v2"), 0, 0, "test"), "PutIfClean must refuse dirty entry")
	got, _, _ := c.Get(key)
	require.Equal(t, []byte("v1"), got, "dirty entry's data preserved")

	// Unconditional Put replaces
	c.Put(key, []byte("v3"), 0, 0, "test")
	got, _, _ = c.Get(key)
	require.Equal(t, []byte("v3"), got)

	// New entry is clean
	require.True(t, c.PutIfClean(key, []byte("v4"), 0, 0, "test"))
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
	c.Put(prefix, enc, 0, 0, "test")

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
	encGot, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte(enc), encGot)
}

// TestBranchCache_Invalidate removes entries from both tiers.
func TestBranchCache_Invalidate(t *testing.T) {
	c := NewBranchCache(100)
	rootKey := []byte{0x00}
	deepKey := []byte{0x12, 0x34}
	c.Put(rootKey, []byte("r"), 0, 0, "test")
	c.Put(deepKey, []byte("d"), 0, 0, "test")

	c.Invalidate(rootKey)
	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root invalidated")

	c.Invalidate(deepKey)
	_, _, ok = c.Get(deepKey)
	require.False(t, ok, "deep invalidated")
}

// TestBranchCache_Clear empties everything and resets stats.
func TestBranchCache_Clear(t *testing.T) {
	c := NewBranchCache(100)
	c.Put([]byte{0x00}, []byte("r"), 0, 0, "test")
	c.Put([]byte{0x12}, []byte("d"), 0, 0, "test")
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get([]byte{0x12})

	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())

	c.Clear()
	require.Equal(t, uint64(0), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())
	_, _, ok := c.Get([]byte{0x00})
	require.False(t, ok)
	_, _, ok = c.Get([]byte{0x12})
	require.False(t, ok)
}

// TestBranchCache_Stats verifies the format of the stats string is
// deterministic and contains the expected per-tier counts.
func TestBranchCache_Stats(t *testing.T) {
	c := NewBranchCache(100)
	c.Put([]byte{0x00}, []byte("rrr"), 0, 0, "test")
	c.Put([]byte{0x12, 0x34}, []byte("ddd"), 0, 0, "test")
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get([]byte{0x12, 0x34})
	_, _, _ = c.Get([]byte{0xff}) // tail miss

	s := c.Stats()
	for _, want := range []string{
		"root hit=1 miss=0",
		"tail hit=1 miss=1",
		// Pinned tier added; tail still has 1 entry (the deep Put),
		// pinned tier remains empty in this test.
		"tail hit=1 miss=1 (50.0%) entries=1",
	} {
		require.Contains(t, s, want, "Stats output: %s", s)
	}
	// Sanity: format doesn't blow up if we read it
	require.True(t, strings.HasPrefix(s, "branch-cache "))
}

// TestBranchCache_UnwindTo_EvictsByTxNWatermark verifies that UnwindTo
// evicts every entry whose txN > watermark across root + pinned + tail
// tiers, and that entries with txN <= watermark survive untouched.
func TestBranchCache_UnwindTo_EvictsByTxNWatermark(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	pinnedKey := []byte{0x12, 0x34, 0x56}
	tailKeyKeep := []byte{0xa0, 0xb0}
	tailKeyEvict := []byte{0xa0, 0xb1}

	// txN=50 entries in every tier — these should survive a watermark of 60.
	c.Put(rootKey, []byte("root-keep"), 0, 50, "test")
	c.PinEntry(pinnedKey, []byte("pinned-keep"), 0, 50, "test")
	c.Put(tailKeyKeep, []byte("tail-keep"), 0, 50, "test")

	// txN=100 tail entry — should be evicted at watermark=60.
	c.Put(tailKeyEvict, []byte("tail-evict"), 0, 100, "test")

	evicted := c.UnwindTo(60)
	require.Equal(t, 1, evicted, "only the txN=100 tail entry should be evicted")

	_, _, ok := c.Get(rootKey)
	require.True(t, ok, "root entry with txN=50 must survive watermark=60")
	_, _, ok = c.Get(pinnedKey)
	require.True(t, ok, "pinned entry with txN=50 must survive watermark=60")
	_, _, ok = c.Get(tailKeyKeep)
	require.True(t, ok, "tail entry with txN=50 must survive watermark=60")
	_, _, ok = c.Get(tailKeyEvict)
	require.False(t, ok, "tail entry with txN=100 must be evicted by watermark=60")
}

// TestBranchCache_UnwindTo_EvictsAcrossAllTiers verifies eviction reaches
// the root + pinned tiers, not just the LRU tail.
func TestBranchCache_UnwindTo_EvictsAcrossAllTiers(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	pinnedKey := []byte{0x12, 0x34, 0x56}
	tailKey := []byte{0xa0, 0xb0}

	c.Put(rootKey, []byte("root"), 0, 100, "test")
	c.PinEntry(pinnedKey, []byte("pinned"), 0, 100, "test")
	c.Put(tailKey, []byte("tail"), 0, 100, "test")

	evicted := c.UnwindTo(50)
	require.Equal(t, 3, evicted, "every entry with txN > watermark must be evicted")

	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root entry must be evicted")
	_, _, ok = c.Get(pinnedKey)
	require.False(t, ok, "pinned entry must be evicted (pinning protects capacity, not correctness)")
	_, _, ok = c.Get(tailKey)
	require.False(t, ok, "tail entry must be evicted")
}

// TestBranchCache_UnwindTo_TxNZeroIsImmortal verifies that entries
// tagged with txN=0 ("not tracked") are NEVER evicted by any watermark.
// Preserves back-compat with callers that haven't been migrated to
// pass real txN values.
func TestBranchCache_UnwindTo_TxNZeroIsImmortal(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	pinnedKey := []byte{0x12, 0x34, 0x56}
	tailKey := []byte{0xa0, 0xb0}

	c.Put(rootKey, []byte("root"), 0, 0, "test")
	c.PinEntry(pinnedKey, []byte("pinned"), 0, 0, "test")
	c.Put(tailKey, []byte("tail"), 0, 0, "test")

	evicted := c.UnwindTo(0)
	require.Equal(t, 0, evicted, "no entry with txN=0 should be evicted")
	evicted = c.UnwindTo(1_000_000)
	require.Equal(t, 0, evicted, "no entry with txN=0 should be evicted even at huge watermark")

	_, _, ok := c.Get(rootKey)
	require.True(t, ok, "txN=0 root entry must survive any watermark")
	_, _, ok = c.Get(pinnedKey)
	require.True(t, ok, "txN=0 pinned entry must survive any watermark")
	_, _, ok = c.Get(tailKey)
	require.True(t, ok, "txN=0 tail entry must survive any watermark")
}

// TestBranchCache_UnwindTo_BoundaryEqualsWatermark verifies the
// inequality at the watermark itself: entries at exactly txN==watermark
// are NOT evicted (the rule is txN > watermark).
func TestBranchCache_UnwindTo_BoundaryEqualsWatermark(t *testing.T) {
	c := NewBranchCache(100)

	atKey := []byte{0xa0, 0xb0}
	aboveKey := []byte{0xa0, 0xb1}
	c.Put(atKey, []byte("at"), 0, 100, "test")
	c.Put(aboveKey, []byte("above"), 0, 101, "test")

	evicted := c.UnwindTo(100)
	require.Equal(t, 1, evicted, "only the txN=101 entry must be evicted; txN=100 stays")

	_, _, ok := c.Get(atKey)
	require.True(t, ok, "entry at txN==watermark must survive")
	_, _, ok = c.Get(aboveKey)
	require.False(t, ok, "entry at txN>watermark must be evicted")
}
