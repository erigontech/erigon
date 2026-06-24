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
	c.Put(rootKey, []byte("root-data"), 0, 0)
	c.Put(deepKey, []byte("deep-data"), 0, 0)

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
	c.Put(rootKey, []byte("ROOT-PERSISTS"), 0, 0)

	// Stuff the tail well past capacity
	for i := 0; i < 100; i++ {
		c.Put([]byte{byte(i), byte(i)}, []byte{byte(i)}, 0, 0)
	}

	// Root must still be there
	got, _, ok := c.Get(rootKey)
	require.True(t, ok, "root should never be evicted from pinned slot")
	require.Equal(t, []byte("ROOT-PERSISTS"), got)

	// Tail at capacity (10), not 100
	require.LessOrEqual(t, c.tail.Len(), 10, "tail should respect LRU capacity")
}

// TestBranchCache_Invalidate removes entries from both tiers.
func TestBranchCache_Invalidate(t *testing.T) {
	c := NewBranchCache(100)
	rootKey := []byte{0x00}
	deepKey := []byte{0x12, 0x34}
	c.Put(rootKey, []byte("r"), 0, 0)
	c.Put(deepKey, []byte("d"), 0, 0)

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
	c.Put([]byte{0x00}, []byte("r"), 0, 0)
	c.Put([]byte{0x12}, []byte("d"), 0, 0)
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
	c.Put([]byte{0x00}, []byte("rrr"), 0, 0)
	c.Put([]byte{0x12, 0x34}, []byte("ddd"), 0, 0)
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get([]byte{0x12, 0x34})
	_, _, _ = c.Get([]byte{0xff}) // tail miss

	s := c.Stats()
	for _, want := range []string{
		"root hit=1 miss=0",
		"tail hit=1 miss=1 (50.0%) entries=1",
	} {
		require.Contains(t, s, want, "Stats output: %s", s)
	}
	// Sanity: format doesn't blow up if we read it
	require.True(t, strings.HasPrefix(s, "branch-cache "))
}

// TestBranchCache_UnwindTo_EvictsByTxNWatermark verifies that UnwindTo
// evicts every entry whose txN > watermark across the root slot and tail
// tier, and that entries with txN <= watermark survive untouched.
func TestBranchCache_UnwindTo_EvictsByTxNWatermark(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	tailKeyKeep := []byte{0xa0, 0xb0}
	tailKeyEvict := []byte{0xa0, 0xb1}

	// txN=50 entries — these should survive a watermark of 60.
	c.Put(rootKey, []byte("root-keep"), 0, 50)
	c.Put(tailKeyKeep, []byte("tail-keep"), 0, 50)

	// txN=100 tail entry — should be evicted at watermark=60.
	c.Put(tailKeyEvict, []byte("tail-evict"), 0, 100)

	evicted := c.UnwindTo(60)
	require.Equal(t, 1, evicted, "only the txN=100 tail entry should be evicted")

	_, _, ok := c.Get(rootKey)
	require.True(t, ok, "root entry with txN=50 must survive watermark=60")
	_, _, ok = c.Get(tailKeyKeep)
	require.True(t, ok, "tail entry with txN=50 must survive watermark=60")
	_, _, ok = c.Get(tailKeyEvict)
	require.False(t, ok, "tail entry with txN=100 must be evicted by watermark=60")
}

// TestBranchCache_UnwindTo_LatePutCaughtByNextUnwind documents the invariant
// the concurrency contract relies on: a Put that lands after an UnwindTo's scan
// has already passed (the acknowledged Put-vs-scan race) is not lost — a
// subsequent UnwindTo with the same watermark still evicts it. So repeated
// UnwindTo converges regardless of scan/Put interleaving. If a future change
// breaks this (e.g. UnwindTo stops re-scanning), this test fails.
func TestBranchCache_UnwindTo_LatePutCaughtByNextUnwind(t *testing.T) {
	c := NewBranchCache(100)
	key := []byte{0xa0, 0xb1}

	// First UnwindTo evicts the stale entry.
	c.Put(key, []byte("v1"), 0, 100)
	require.Equal(t, 1, c.UnwindTo(60))
	_, _, ok := c.Get(key)
	require.False(t, ok)

	// A Put that races in after that scan (txN still above the watermark) is
	// caught by the next UnwindTo at the same watermark — no permanent stale entry.
	c.Put(key, []byte("v2"), 0, 100)
	require.Equal(t, 1, c.UnwindTo(60))
	_, _, ok = c.Get(key)
	require.False(t, ok, "a late Put above the watermark must be evicted by the next UnwindTo")
}

// TestBranchCache_UnwindTo_EvictsAcrossAllTiers verifies eviction reaches
// the root slot, not just the LRU tail.
func TestBranchCache_UnwindTo_EvictsAcrossAllTiers(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	tailKey := []byte{0xa0, 0xb0}

	c.Put(rootKey, []byte("root"), 0, 100)
	c.Put(tailKey, []byte("tail"), 0, 100)

	evicted := c.UnwindTo(50)
	require.Equal(t, 2, evicted, "every entry with txN > watermark must be evicted")

	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root entry must be evicted")
	_, _, ok = c.Get(tailKey)
	require.False(t, ok, "tail entry must be evicted")
}

// TestBranchCache_UnwindTo_BoundaryEqualsWatermark verifies the
// inequality at the watermark itself: entries at exactly txN==watermark
// are NOT evicted (the rule is txN > watermark).
func TestBranchCache_UnwindTo_BoundaryEqualsWatermark(t *testing.T) {
	c := NewBranchCache(100)

	atKey := []byte{0xa0, 0xb0}
	aboveKey := []byte{0xa0, 0xb1}
	c.Put(atKey, []byte("at"), 0, 100)
	c.Put(aboveKey, []byte("above"), 0, 101)

	evicted := c.UnwindTo(100)
	require.Equal(t, 1, evicted, "only the txN=101 entry must be evicted; txN=100 stays")

	_, _, ok := c.Get(atKey)
	require.True(t, ok, "entry at txN==watermark must survive")
	_, _, ok = c.Get(aboveKey)
	require.False(t, ok, "entry at txN>watermark must be evicted")
}

// TestBranchCache_ShardedTailUnwindAcrossShards exercises UnwindTo at the
// production tail granularity: a capacity >= branchCacheTailShards spreads the
// tail across all 256 shards, whereas the other tests here use small capacities
// that collapse it to one shard. It pins that the Range→DeleteByHash pairing
// routes correctly across shards — every stale entry is evicted and every fresh
// one survives, no matter which shard it landed in.
func TestBranchCache_ShardedTailUnwindAcrossShards(t *testing.T) {
	c := NewBranchCache(DefaultBranchCacheTailCapacity)

	const n = 2000
	const watermark = 1000
	for i := 0; i < n; i++ {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		c.Put(prefix, []byte{byte(i)}, 0, uint64(i))
	}

	evicted := c.UnwindTo(watermark)
	require.Equal(t, n-watermark-1, evicted, "every tail entry with txN > watermark must be evicted")

	for i := 0; i < n; i++ {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		_, _, ok := c.Get(prefix)
		if uint64(i) > watermark {
			require.False(t, ok, "entry txN=%d must be evicted by watermark=%d", i, watermark)
		} else {
			require.True(t, ok, "entry txN=%d must survive watermark=%d", i, watermark)
		}
	}
}
