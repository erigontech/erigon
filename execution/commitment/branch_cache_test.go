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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBranchCache_AccountTrunkRouting verifies account-trie branches at nibble
// depths 1-4 land in the resident fixed-array trunk (counted as trunk hits),
// survive LRU tail-eviction pressure, and are invalidated lazily by an unwind
// (the trunk honors the same (txN, epoch) model as the tail).
func TestBranchCache_AccountTrunkRouting(t *testing.T) {
	c := NewBranchCache(10) // small tail

	trunkKey := []byte{0xa0, 0xb0} // 2 nibbles (even flag) → accountTrunk.d2
	c.Put(trunkKey, []byte("trunk-data"), 0, 100)

	got, _, ok := c.Get(trunkKey)
	require.True(t, ok)
	require.Equal(t, []byte("trunk-data"), got)
	require.Equal(t, uint64(1), c.trunkHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load(), "depth-2 account branch must not land in the tail")

	// Flood the tail well past capacity with deep (5-nibble) keys; the resident
	// trunk entry must not be evicted.
	for i := 0; i < 100; i++ {
		c.Put([]byte{0x10, byte(i), byte(i)}, []byte{byte(i)}, 0, 100) // odd flag, 5 nibbles → tail
	}
	got, _, ok = c.Get(trunkKey)
	require.True(t, ok, "resident trunk entry must survive tail eviction pressure")
	require.Equal(t, []byte("trunk-data"), got)

	// An unwind below the entry's txN invalidates it lazily on next Get.
	c.Unwind(60)
	_, _, ok = c.Get(trunkKey)
	require.False(t, ok, "trunk entry with txN=100 must drop at unwind floor 60")
}

// TestBranchCache_StorageTrunkPin verifies PinEntry routes a storage-trunk
// prefix (>= 64 nibbles) into its per-contract storage trunk, is served from
// the pinned tier, counts toward PinnedCount, and honors the unwind model.
func TestBranchCache_StorageTrunkPin(t *testing.T) {
	c := NewBranchCache(100)

	// 33-byte compact prefix: even flag (0x00) + 32-byte account hash = 64
	// nibbles exactly → the storage trunk's depth-0 slot for that contract.
	prefix := make([]byte, 33)
	for i := 1; i < 33; i++ {
		prefix[i] = byte(i)
	}
	c.PinEntry(prefix, []byte("storage-root"), 0, 100)
	require.Equal(t, 1, c.PinnedCount())

	got, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte("storage-root"), got)
	require.Equal(t, uint64(1), c.pinnedHits.Load())

	c.Unwind(60)
	_, _, ok = c.Get(prefix)
	require.False(t, ok, "pinned storage-trunk entry with txN=100 must drop at unwind floor 60")
}

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
	require.LessOrEqual(t, c.tailLen(), 10, "tail should respect LRU capacity")
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
	deepKey := []byte{0x12, 0x34, 0x56} // 5 nibbles → LRU tail
	c.Put([]byte{0x00}, []byte("r"), 0, 0)
	c.Put(deepKey, []byte("d"), 0, 0)
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get(deepKey)

	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())

	c.Clear()
	require.Equal(t, uint64(0), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())
	_, _, ok := c.Get([]byte{0x00})
	require.False(t, ok)
	_, _, ok = c.Get(deepKey)
	require.False(t, ok)
}

// TestBranchCache_Stats verifies the format of the stats string is
// deterministic and contains the expected per-tier counts.
func TestBranchCache_Stats(t *testing.T) {
	c := NewBranchCache(100)
	// 3-byte odd-flag prefixes are 5 nibbles deep → LRU tail (the account
	// trunk only holds depths 1-4).
	tailHit := []byte{0x12, 0x34, 0x56}
	tailMiss := []byte{0x12, 0x34, 0x57}
	c.Put([]byte{0x00}, []byte("rrr"), 0, 0)
	c.Put(tailHit, []byte("ddd"), 0, 0)
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get(tailHit)
	_, _, _ = c.Get(tailMiss) // tail miss

	s := c.Stats()
	for _, want := range []string{
		"root hit=1 miss=0",
		"tail hit=1 miss=1 (50.0%) entries=1",
	} {
		require.Contains(t, s, want, "Stats output: %s", s)
	}
	// New format carries the trunk and pin tiers.
	require.Contains(t, s, "trunk hit=", "Stats output: %s", s)
	require.Contains(t, s, "pin hit=", "Stats output: %s", s)
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

// TestBranchCache_ConcurrentTailGrow drives concurrent tail Puts well past the
// 512-entry start capacity so maybeGrow runs under contention. It regresses the
// data race where Add read tailLRU.curCap unsynchronized while maybeGrow/reset
// wrote it under resizeMu. Must be run under -race to be meaningful.
func TestBranchCache_ConcurrentTailGrow(t *testing.T) {
	c := NewBranchCache(4096) // max >> 512 start, so the tail actually grows

	const (
		workers   = 8
		perWorker = 2000 // 16k distinct deep keys >> 512 → forces maybeGrow
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				// odd flag (0x10) + 3 bytes → 7 nibbles → tail; unique per (w,i).
				key := []byte{0x10, byte(w), byte(i), byte(i >> 8)}
				c.Put(key, []byte{byte(i)}, 0, 100)
				c.Get(key)
			}
		}(w)
	}
	wg.Wait()
}
