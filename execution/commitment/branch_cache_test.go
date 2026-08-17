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
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

func TestBranchCache_PinnedCountSurvivesConcurrentInvalidate(t *testing.T) {
	newPrefix := func(contract byte, storageNibbles int) []byte {
		p := make([]byte, 33+(storageNibbles+1)/2)
		if storageNibbles%2 == 1 {
			p[0] = 0x10
		}
		p[1] = contract
		for i := 2; i < len(p); i++ {
			p[i] = byte(i * 3)
		}
		return p
	}

	for _, storageNibbles := range []int{0, 6} {
		t.Run(fmt.Sprintf("depth%d", storageNibbles), func(t *testing.T) {
			prefix := newPrefix(1, storageNibbles)

			for range 20000 {
				c := NewBranchCache(100)
				c.PinEntry(prefix, []byte("v0"), 0, 100)

				var wg sync.WaitGroup
				wg.Go(func() { c.PinEntry(prefix, []byte("v1"), 0, 100) })
				wg.Go(func() { c.Invalidate(prefix) })
				wg.Wait()

				_, _, resident := c.Get(prefix)
				want := 0
				if resident {
					want = 1
				}
				require.Equalf(t, want, c.PinnedCount(),
					"PinnedCount must match residency (resident=%v)", resident)
				c.Close()
			}
		})
	}
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

func TestBranchCache_ClearRacingPut_EpochAlias(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()
	c.Unwind(300)

	key := []byte{0x00}
	preClearEpoch := c.coh.Epoch()
	c.Clear()
	c.store(key, &branchCacheEntry{data: []byte("dead-fork-branch"), txN: 200, epoch: preClearEpoch})
	c.Unwind(150)

	_, _, ok := c.Get(key)
	require.False(t, ok, "pre-Clear epoch must not alias the live epoch after a later unwind")
}

func fenceDuringBlockedBranchCacheWrite(block *sync.Mutex, write, fence func()) {
	// GOMAXPROCS=1 makes each Gosched yield deterministically to the queued goroutine.
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	block.Lock()
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		write()
		close(writerDone)
	}()
	<-writerStarted
	runtime.Gosched()

	clearDone := make(chan struct{})
	go func() {
		fence()
		close(clearDone)
	}()
	runtime.Gosched()

	block.Unlock()
	<-writerDone
	<-clearDone
}

func TestBranchCache_ClearFencesStartedPut(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()
	c.Unwind(300)

	key := []byte{0x12, 0x34, 0x56}
	fenceDuringBlockedBranchCacheWrite(&c.tailMu, func() {
		c.Put(key, []byte("dead-fork-branch"), 0, 200)
	}, c.Clear)

	_, _, ok := c.Get(key)
	require.False(t, ok, "Clear must remove a Put that started in the retiring generation")
}

func TestBranchCache_ClearFencesStartedPinEntry(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()
	c.Unwind(300)

	key := make([]byte, 33)
	key[32] = 1
	fenceDuringBlockedBranchCacheWrite(&c.pinnedMu, func() {
		c.PinEntry(key, []byte("dead-fork-branch"), 0, 200)
	}, c.Clear)

	_, _, ok := c.Get(key)
	require.False(t, ok, "Clear must remove a PinEntry that started in the retiring generation")
}

func TestBranchCacheReconcileFilesPreservesAppliedState(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.Put(prefix, []byte("branch"), 0, 100)
	c.ReconcileFiles(101)

	_, _, ok := c.Get(prefix)
	require.True(t, ok)
}

func TestBranchCacheReconcileFilesClearsReadFills(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.ReconcileFiles(100)
	c.View().Fill(prefix, []byte("branch"), 0, 90, 100)
	_, _, ok := c.Get(prefix)
	require.True(t, ok)

	c.ReconcileFiles(150)
	_, _, ok = c.Get(prefix)
	require.False(t, ok)
}

func TestBranchCacheRejectsReadFillBehindPublication(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.ReconcileFiles(150)
	c.View().Fill(prefix, []byte("stale"), 0, 90, 100)
	_, _, ok := c.Get(prefix)
	require.False(t, ok)

	c.View().Fill(prefix, []byte("current"), 0, 140, 150)
	value, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte("current"), value)
}

func TestBranchCacheRejectsAuthoritativePutBehindPublication(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.ReconcileFiles(150)
	c.Put(prefix, []byte("stale"), 0, 100)

	_, _, ok := c.Get(prefix)
	require.False(t, ok)
}

func TestBranchCacheRejectsPinBehindPublication(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := make([]byte, 33)
	prefix[32] = 1
	c.ReconcileFiles(150)
	c.PinEntry(prefix, []byte("stale"), 0, 100)
	_, _, ok := c.Get(prefix)
	require.False(t, ok)

	c.PinEntry(prefix, []byte("current"), 0, 150)
	value, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte("current"), value)
}

func TestBranchCacheDeleteAdvancesAppliedFrontier(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	kept := []byte{0x01}
	deleted := []byte{0x02}
	c.Put(kept, []byte("kept"), 0, 50)
	c.Put(deleted, []byte("deleted"), 0, 50)
	c.Delete(deleted, 100)
	c.ReconcileFiles(101)

	_, _, ok := c.Get(kept)
	require.True(t, ok)
	_, _, ok = c.Get(deleted)
	require.False(t, ok)
}

func TestBranchCacheAdvanceCommitPreservesStateAcrossQuietRange(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.Put(prefix, []byte("branch"), 0, 50)
	c.AdvanceCommit(100)
	c.ReconcileFiles(101)

	_, _, ok := c.Get(prefix)
	require.True(t, ok)
}

func TestBranchCacheQuietCommitKeepsUnchangedFillEligible(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.ReconcileFiles(100)
	c.AdvanceCommit(149)
	c.View().Fill(prefix, []byte("branch"), 0, 90, 100)

	_, _, ok := c.Get(prefix)
	require.True(t, ok)
}

func TestBranchCacheReconcileFilesFencesStartedFill(t *testing.T) {
	c := NewBranchCache(100)
	t.Cleanup(c.Close)
	c.ReconcileFiles(100)

	key := []byte{0x12, 0x34, 0x56}
	view := c.View()
	fenceDuringBlockedBranchCacheWrite(&c.tailMu, func() {
		view.Fill(key, []byte("stale"), 0, 90, 100)
	}, func() {
		c.ReconcileFiles(150)
	})

	_, _, ok := c.Get(key)
	require.False(t, ok)
}

func TestBranchCacheRejectsReadFillAcrossVisibilityLowering(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	c.ReconcileFiles(100)
	older := c.View()
	c.ReconcileFiles(50)
	older.Fill([]byte{0x01}, []byte("stale"), 0, 90, 100)

	_, _, ok := c.Get([]byte{0x01})
	require.False(t, ok)
}

func TestBranchCacheRejectsPinAcrossVisibilityLowering(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := make([]byte, 33)
	prefix[32] = 1
	c.ReconcileFiles(100)
	older := c.View()
	c.ReconcileFiles(50)
	older.PinEntry(prefix, []byte("stale"), 0, 100)

	_, _, ok := c.Get(prefix)
	require.False(t, ok)
}

func TestBranchCacheUnwindLowersAppliedFrontier(t *testing.T) {
	t.Parallel()
	c := NewBranchCache(64)
	t.Cleanup(c.Close)

	prefix := []byte{0x01}
	c.Put(prefix, []byte("old fork"), 0, 100)
	c.Unwind(50)
	c.Put(prefix, []byte("new fork"), 0, 50)

	value, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte("new fork"), value)
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

const twoTailKeyCapacity = 2 * branchCacheTailShards

func TestBranchCache_Unwind_DropsStaleAboveFloorLazily(t *testing.T) {
	c := NewBranchCache(twoTailKeyCapacity)

	rootKey := []byte{0x00}
	tailKeyKeep := []byte{0x1a, 0xb0, 0x00}
	tailKeyDrop := []byte{0x1a, 0xb0, 0x01}

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
	require.Equal(t, uint64(2), c.tailHits.Load(), "both keys must exercise the LRU tail")
}

func TestBranchCache_Unwind_AcrossAllTiers(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00}
	trunkKey := []byte{0xa0, 0xb0}
	tailKey := []byte{0x1a, 0xb0, 0x00}

	c.Put(rootKey, []byte("root"), 0, 100)
	c.Put(trunkKey, []byte("trunk"), 0, 100)
	c.Put(tailKey, []byte("tail"), 0, 100)

	c.Unwind(50)

	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root entry at txN>=floor must drop")
	_, _, ok = c.Get(trunkKey)
	require.False(t, ok, "trunk entry at txN>=floor must drop")
	_, _, ok = c.Get(tailKey)
	require.False(t, ok, "tail entry at txN>=floor must drop")
	require.Equal(t, uint64(1), c.trunkHits.Load(), "trunk key must route to the account trunk")
	require.Equal(t, uint64(1), c.tailHits.Load(), "tail key must route to the LRU tail")
}

func TestBranchCache_Unwind_FloorBoundary(t *testing.T) {
	c := NewBranchCache(twoTailKeyCapacity)

	belowKey := []byte{0x1a, 0xb0, 0x00}
	atKey := []byte{0x1a, 0xb0, 0x01}
	c.Put(belowKey, []byte("below"), 0, 99)
	c.Put(atKey, []byte("at"), 0, 100)

	c.Unwind(100)

	_, _, ok := c.Get(belowKey)
	require.True(t, ok, "entry at txN=floor-1 must survive")
	_, _, ok = c.Get(atKey)
	require.False(t, ok, "entry at txN==floor must drop (rolled-back block)")
	require.Equal(t, uint64(2), c.tailHits.Load(), "both keys must exercise the LRU tail")
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

func storageNibblesReference(prefix []byte) (nib [4]byte, n int) {
	full := nibbles.CompactToHex(prefix)
	n = len(full) - 64
	for i := 0; i < n && i < 4; i++ {
		nib[i] = full[64+i]
	}
	return nib, n
}

func TestStorageNibbles_MatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for range 5000 {
		l := 33 + rng.Intn(20)
		prefix := make([]byte, l)
		rng.Read(prefix)
		oddBit := byte(0)
		if rng.Intn(2) == 1 {
			oddBit = 0x10
		}
		prefix[0] = prefix[0]&0x0f | oddBit

		wantNib, wantN := storageNibblesReference(prefix)
		var gotNib [4]byte
		gotN := storageNibbles(prefix, &gotNib)
		require.Equalf(t, wantN, gotN, "n mismatch len=%d prefix0=%#x", l, prefix[0])
		if gotN <= 4 {
			require.Equalf(t, wantNib, gotNib, "nibbles mismatch len=%d prefix0=%#x", l, prefix[0])
		}
	}
}

func TestBranchCache_StorageRouteRejectsTerminator(t *testing.T) {
	prefix := make([]byte, 34)
	for i := 1; i < len(prefix); i++ {
		prefix[i] = byte(i)
	}
	for _, flag := range []byte{0x20, 0x30} {
		prefix[0] = flag
		c := NewBranchCache(100)
		var nibBuf [4]byte
		_, _, routed := c.storageRoute(prefix, true, &nibBuf)
		require.Falsef(t, routed, "terminator-flagged prefix (%#x) must fall through to the tail", flag)

		c.PinEntry(prefix, []byte("v"), 0, 100)
		got, _, ok := c.Get(prefix)
		require.Truef(t, ok, "terminator-flagged prefix (%#x) must still round-trip", flag)
		require.Equal(t, []byte("v"), got)
		c.Close()
	}
}

func TestBranchCache_StorageRoute_ZeroAlloc(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	even := make([]byte, 33)
	for i := 1; i < 33; i++ {
		even[i] = byte(i)
	}
	c.PinEntry(even, []byte("even"), 0, 100)

	odd := make([]byte, 34)
	odd[0] = 0x10
	for i := 1; i < 34; i++ {
		odd[i] = byte(i * 7)
	}
	c.PinEntry(odd, []byte("odd"), 0, 100)

	for _, prefix := range [][]byte{even, odd} {
		allocs := testing.AllocsPerRun(1000, func() {
			var nibBuf [4]byte
			_, _, _ = c.storageRoute(prefix, false, &nibBuf)
		})
		require.Zerof(t, allocs, "storageRoute must not allocate on a storage-tier lookup, prefix0=%#x", prefix[0])

		allocs = testing.AllocsPerRun(1000, func() {
			var nibBuf [4]byte
			_, _, _ = c.storageRoute(prefix, true, &nibBuf)
		})
		require.Zerof(t, allocs, "storageRoute must not allocate routing to a resident contract, prefix0=%#x", prefix[0])
	}
}

func TestBranchCache_StorageTrunkRoundTripAcrossDepths(t *testing.T) {
	for depth := range 9 {
		total := 64 + depth
		oddFlag := total%2 == 1
		prefix := make([]byte, total/2+1)
		if oddFlag {
			prefix[0] = 0x10
		}
		for i := 1; i < len(prefix); i++ {
			prefix[i] = byte(i*11 + depth)
		}

		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			c := NewBranchCache(100)
			defer c.Close()
			want := fmt.Sprintf("d%d", depth)
			c.PinEntry(prefix, []byte(want), 0, 100)

			got, _, ok := c.Get(prefix)
			require.Truef(t, ok, "pinned entry must read back, depth=%d", depth)
			require.Equal(t, want, string(got))
			require.Equalf(t, 1, c.PinnedCount(), "depth=%d", depth)

			c.Invalidate(prefix)
			_, _, ok = c.Get(prefix)
			require.Falsef(t, ok, "invalidated entry must be gone, depth=%d", depth)
			require.Zerof(t, c.PinnedCount(), "depth=%d", depth)
		})
	}
}
