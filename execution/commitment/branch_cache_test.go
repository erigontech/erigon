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

func TestBranchCache_AccountTrunkRouting(t *testing.T) {
	c := NewBranchCache(10)

	trunkKey := []byte{0xa0, 0xb0}
	c.Put(trunkKey, []byte("trunk-data"), 0, 100)

	got, _, ok := c.Get(trunkKey)
	require.True(t, ok)
	require.Equal(t, []byte("trunk-data"), got)
	require.Equal(t, uint64(1), c.trunkHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load(), "depth-2 account branch must not land in the tail")

	for i := range 100 {
		c.Put([]byte{0x10, byte(i), byte(i)}, []byte{byte(i)}, 0, 100)
	}
	got, _, ok = c.Get(trunkKey)
	require.True(t, ok, "resident trunk entry must survive tail eviction pressure")
	require.Equal(t, []byte("trunk-data"), got)

	c.Unwind(60)
	_, _, ok = c.Get(trunkKey)
	require.False(t, ok, "trunk entry with txN=100 must drop at unwind floor 60")
}

func TestBranchCache_StorageTrunkPin(t *testing.T) {
	c := NewBranchCache(100)

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

func TestBranchCache_RootPinning(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	deepKey := []byte{0x12, 0x34, 0x56}
	c.Put(rootKey, []byte("root-data"), 0, 0)
	c.Put(deepKey, []byte("deep-data"), 0, 0)

	got, _, ok := c.Get(rootKey)
	require.True(t, ok)
	require.Equal(t, []byte("root-data"), got)
	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())

	got, _, ok = c.Get(deepKey)
	require.True(t, ok)
	require.Equal(t, []byte("deep-data"), got)
	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())
}

func TestBranchCache_RootSurvivesEvictionPressure(t *testing.T) {
	c := NewBranchCache(10)
	rootKey := []byte{0x00}
	c.Put(rootKey, []byte("ROOT-PERSISTS"), 0, 0)

	for i := range 100 {
		c.Put([]byte{byte(i), byte(i)}, []byte{byte(i)}, 0, 0)
	}

	got, _, ok := c.Get(rootKey)
	require.True(t, ok, "root should never be evicted from pinned slot")
	require.Equal(t, []byte("ROOT-PERSISTS"), got)

	require.LessOrEqual(t, c.tailLen(), 10, "tail should respect LRU capacity")
}

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

func TestBranchCache_Clear(t *testing.T) {
	c := NewBranchCache(100)
	deepKey := []byte{0x12, 0x34, 0x56}
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

func TestBranchCache_Stats(t *testing.T) {
	c := NewBranchCache(100)
	tailHit := []byte{0x12, 0x34, 0x56}
	tailMiss := []byte{0x12, 0x34, 0x57}
	c.Put([]byte{0x00}, []byte("rrr"), 0, 0)
	c.Put(tailHit, []byte("ddd"), 0, 0)
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get(tailHit)
	_, _, _ = c.Get(tailMiss)

	s := c.Stats()
	for _, want := range []string{
		"root hit=1 miss=0",
		"tail hit=1 miss=1 (50.0%) entries=1",
	} {
		require.Contains(t, s, want, "Stats output: %s", s)
	}
	require.Contains(t, s, "trunk hit=", "Stats output: %s", s)
	require.Contains(t, s, "pin hit=", "Stats output: %s", s)
	require.True(t, strings.HasPrefix(s, "branch-cache "))
}

func TestBranchCache_Unwind_DropsStaleAboveFloorLazily(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	tailKeyKeep := []byte{0xa0, 0xb0}
	tailKeyDrop := []byte{0xa0, 0xb1}

	c.Put(rootKey, []byte("root-keep"), 0, 50)
	c.Put(tailKeyKeep, []byte("tail-keep"), 0, 50)
	c.Put(tailKeyDrop, []byte("tail-drop"), 0, 100)

	c.Unwind(60)

	_, _, ok := c.Get(rootKey)
	require.True(t, ok, "root entry with txN=50 must survive floor=60")
	_, _, ok = c.Get(tailKeyKeep)
	require.True(t, ok, "tail entry with txN=50 must survive floor=60")
	_, _, ok = c.Get(tailKeyDrop)
	require.False(t, ok, "tail entry with txN=100 must drop at floor=60")
}

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

func TestBranchCache_Unwind_CurrentEpochSurvives(t *testing.T) {
	c := NewBranchCache(100)

	key := []byte{0xa0, 0xb0}
	c.Put(key, []byte("old-fork"), 0, 100)
	c.Unwind(50)
	c.Put(key, []byte("new-fork"), 0, 100)

	v, _, ok := c.Get(key)
	require.True(t, ok, "current-epoch entry must survive even with txN>=floor")
	require.Equal(t, "new-fork", string(v), "must serve the re-executed value, not the dead-fork one")
}

func TestBranchCache_Unwind_FrozenSurvives(t *testing.T) {
	c := NewBranchCache(100)
	key := []byte{0xa0, 0xb0}
	c.Put(key, []byte("frozen"), 0, 0)
	c.Unwind(50)
	_, _, ok := c.Get(key)
	require.True(t, ok, "frozen txN=0 entry must survive any positive-txN unwind")
}

func TestBranchCache_StateKeyNeverCached(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	c.Put(KeyCommitmentState, []byte("checkpoint"), 1, 1)
	_, _, ok := c.Get(KeyCommitmentState)
	require.False(t, ok, "state key must never be served from the cache")
	require.Equal(t, 0, c.tailLen(), "state key must not occupy a tail slot")

	deepKey := []byte{0x12, 0x34}
	c.Put(deepKey, []byte("d"), 0, 0)
	c.Invalidate(KeyCommitmentState)
	got, _, ok := c.Get(deepKey)
	require.True(t, ok, "invalidating the state key must not evict real entries")
	require.Equal(t, []byte("d"), got)
}

func TestBranchCache_ShardedTailUnwindAcrossShards(t *testing.T) {
	c := NewBranchCache(DefaultBranchCacheTailCapacity)
	defer c.Close()

	const n = 64
	const watermark = 32
	for i := range n {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		c.Put(prefix, []byte{byte(i)}, 0, uint64(i))
	}

	c.Unwind(watermark)

	for i := range n {
		prefix := []byte{0x01, byte(i), byte(i >> 8)}
		_, _, ok := c.Get(prefix)
		if uint64(i) >= watermark {
			require.False(t, ok, "entry txN=%d must be dropped by floor=%d", i, watermark)
		} else {
			require.True(t, ok, "entry txN=%d must survive floor=%d", i, watermark)
		}
	}
}

func TestBranchCache_ConcurrentTailGrow(t *testing.T) {
	c := NewBranchCache(4096)
	defer c.Close()

	const (
		workers   = 8
		perWorker = 2000
	)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				key := []byte{0x10, byte(w), byte(i), byte(i >> 8)}
				c.Put(key, []byte{byte(i)}, 0, 100)
				c.Get(key)
			}
		})
	}
	wg.Wait()
}
