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
	"runtime"
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

// TestBranchCache_StateKeyNeverCached pins the invariant that the commitment
// state checkpoint key bypasses every tier: Put is a no-op, Get always misses,
// and Invalidate neither panics nor disturbs real entries.
func TestBranchCache_StateKeyNeverCached(t *testing.T) {
	c := NewBranchCache(100)

	c.Put(KeyCommitmentState, []byte("checkpoint"), 1, 1)
	_, _, ok := c.Get(KeyCommitmentState)
	require.False(t, ok, "state key must never be served from the cache")
	require.Equal(t, 0, c.tail.Len(), "state key must not occupy a tail slot")

	deepKey := []byte{0x12, 0x34}
	c.Put(deepKey, []byte("d"), 0, 0)
	c.Invalidate(KeyCommitmentState)
	got, _, ok := c.Get(deepKey)
	require.True(t, ok, "invalidating the state key must not evict real entries")
	require.Equal(t, []byte("d"), got)
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

// TestBranchCache_Unwind_DropsStaleAboveFloorLazily verifies Unwind drops
// (lazily, on the next Get) every superseded-epoch entry whose txN is at or
// above the unwind floor, while entries below the floor survive untouched.
func TestBranchCache_Unwind_DropsStaleAboveFloorLazily(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	tailKeyKeep := []byte{0xa0, 0xb0}
	tailKeyDrop := []byte{0xa0, 0xb1}

	// txN=50 entries — below an unwind floor of 60, so they survive.
	c.Put(rootKey, []byte("root-keep"), 0, 50)
	c.Put(tailKeyKeep, []byte("tail-keep"), 0, 50)
	// txN=100 entry — at/above floor 60, so it drops on its next Get.
	c.Put(tailKeyDrop, []byte("tail-drop"), 0, 100)

	c.Unwind(60)

	_, _, ok := c.Get(rootKey)
	require.True(t, ok, "root entry with txN=50 must survive floor=60")
	_, _, ok = c.Get(tailKeyKeep)
	require.True(t, ok, "tail entry with txN=50 must survive floor=60")
	_, _, ok = c.Get(tailKeyDrop)
	require.False(t, ok, "tail entry with txN=100 must drop at floor=60")
}

// TestBranchCache_Unwind_AcrossAllTiers verifies lazy invalidation reaches the
// root slot, not just the LRU tail.
func TestBranchCache_Unwind_AcrossAllTiers(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	tailKey := []byte{0xa0, 0xb0}

	c.Put(rootKey, []byte("root"), 0, 100)
	c.Put(tailKey, []byte("tail"), 0, 100)

	c.Unwind(50)

	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root entry at txN>=floor must drop")
	_, _, ok = c.Get(tailKey)
	require.False(t, ok, "tail entry at txN>=floor must drop")
}

// TestBranchCache_Unwind_FloorBoundary verifies the >= rule at the floor: an
// entry stamped exactly at the unwind floor belongs to a rolled-back block and
// drops; an entry one txN below the floor survives.
func TestBranchCache_Unwind_FloorBoundary(t *testing.T) {
	c := NewBranchCache(100)

	belowKey := []byte{0xa0, 0xb0}
	atKey := []byte{0xa0, 0xb1}
	c.Put(belowKey, []byte("below"), 0, 99)
	c.Put(atKey, []byte("at"), 0, 100)

	c.Unwind(100)

	_, _, ok := c.Get(belowKey)
	require.True(t, ok, "entry at txN=floor-1 must survive")
	_, _, ok = c.Get(atKey)
	require.False(t, ok, "entry at txN==floor must drop (rolled-back block)")
}

// TestBranchCache_Unwind_CurrentEpochSurvives verifies the epoch disambiguates a
// txN reused across forks: an entry rewritten AFTER the unwind (current epoch)
// survives even when its txN is at/above the floor — only superseded-epoch
// entries are stale.
func TestBranchCache_Unwind_CurrentEpochSurvives(t *testing.T) {
	c := NewBranchCache(100)

	key := []byte{0xa0, 0xb0}
	c.Put(key, []byte("old-fork"), 0, 100) // pre-unwind, old epoch
	c.Unwind(50)
	c.Put(key, []byte("new-fork"), 0, 100) // re-executed on the live fork, new epoch, same txN

	v, _, ok := c.Get(key)
	require.True(t, ok, "current-epoch entry must survive even with txN>=floor")
	require.Equal(t, "new-fork", string(v), "must serve the re-executed value, not the dead-fork one")
}

// TestBranchCache_Unwind_FrozenSurvives verifies a frozen (txN=0) entry — e.g. a
// preloaded trunk branch — is never dropped by an unwind to a positive txN.
func TestBranchCache_Unwind_FrozenSurvives(t *testing.T) {
	c := NewBranchCache(100)
	key := []byte{0xa0, 0xb0}
	c.Put(key, []byte("frozen"), 0, 0)
	c.Unwind(50)
	_, _, ok := c.Get(key)
	require.True(t, ok, "frozen txN=0 entry must survive any positive-txN unwind")
}

// TestBranchCache_ShardedTailUnwindAcrossShards drives a lazy Unwind over many
// tail entries spread across the sharded tail, pinning that invalidation by txN
// floor works at scale: every stale entry (txN >= floor) drops on its next Get
// and every fresh one survives, regardless of which shard it landed in.
func TestBranchCache_ShardedTailUnwindAcrossShards(t *testing.T) {
	c := NewBranchCache(DefaultBranchCacheTailCapacity)

	const n = 2000
	const watermark = 1000
	for i := 0; i < n; i++ {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		c.Put(prefix, []byte{byte(i)}, 0, uint64(i))
	}

	// Lazy unwind: bump the epoch and lower the floor to watermark (the first
	// unwound txN). Stale entries (old epoch, txN >= floor) are dropped on their
	// next Get, across all tail shards — no eager scan.
	c.Unwind(watermark)

	for i := 0; i < n; i++ {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		_, _, ok := c.Get(prefix)
		if uint64(i) >= watermark {
			require.False(t, ok, "entry txN=%d must be dropped by floor=%d", i, watermark)
		} else {
			require.True(t, ok, "entry txN=%d must survive floor=%d", i, watermark)
		}
	}
}

// TestBranchCache_SmallTailKeepsCollidingKeys pins the shard-capacity floor: a
// small tail (as used by unit tests) must never let two distinct keys evict
// each other while it is far from full. The pre-cap 64-shard split of a
// 100-entry tail left single-entry shards, so two keys hashing to one dropped a
// valid entry via LRU overflow instead of by design. Scanning many pairs trips
// a reverted cap regardless of the process hash seed; the fix makes it green
// deterministically (every shard holds >= 2).
func TestBranchCache_SmallTailKeepsCollidingKeys(t *testing.T) {
	for i := 0; i < 4096; i++ {
		c := NewBranchCache(100)
		c.Put([]byte{0xa0, byte(i), byte(i >> 8)}, []byte("v1"), 0, 1)
		c.Put([]byte{0xb1, byte(i), byte(i >> 8)}, []byte("v2"), 0, 1)
		require.Equal(t, 2, c.tail.Len(),
			"both keys must survive in a 100-entry tail (i=%d): a single-entry shard evicted one", i)
	}
}

// TestBranchCache_BaselineFootprint pins that a freshly constructed cache is
// cheap. One is allocated per aggregator and may linger after Close, so an
// empty cache must not carry a multi-megabyte fixed backing for its LRU tail.
func TestBranchCache_BaselineFootprint(t *testing.T) {
	const (
		n                = 128
		maxBytesPerCache = 256 * 1024
	)
	caches := make([]*BranchCache, n)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range caches {
		caches[i] = NewBranchCache(DefaultBranchCacheTailCapacity)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(caches)

	perCache := (after.HeapAlloc - before.HeapAlloc) / n
	require.Less(t, perCache, uint64(maxBytesPerCache),
		"fresh BranchCache baseline is %d KiB/cache, want < %d KiB", perCache/1024, maxBytesPerCache/1024)
}
