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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func testBranchGeneration(stateVersion uint64) cache.Generation {
	return cache.BranchGeneration(stateVersion, 0)
}

// TestBranchCache_AccountTrunkRouting verifies account-trie branches at nibble
// depths 1-4 land in the resident fixed-array trunk (counted as trunk hits),
// and survive LRU tail-eviction pressure.
func TestBranchCache_AccountTrunkRouting(t *testing.T) {
	c := NewBranchCache(10) // small tail

	trunkKey := []byte{0xa0, 0xb0} // 2 nibbles (even flag) → accountTrunk.d2
	c.Put(trunkKey, []byte("trunk-data"), 0)

	got, _, ok := c.Get(trunkKey)
	require.True(t, ok)
	require.Equal(t, []byte("trunk-data"), got)
	require.Equal(t, uint64(1), c.trunkHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load(), "depth-2 account branch must not land in the tail")

	// Flood the tail well past capacity with deep (5-nibble) keys; the resident
	// trunk entry must not be evicted.
	for i := range 100 {
		c.Put([]byte{0x10, byte(i), byte(i)}, []byte{byte(i)}, 0) // odd flag, 5 nibbles → tail
	}
	got, _, ok = c.Get(trunkKey)
	require.True(t, ok, "resident trunk entry must survive tail eviction pressure")
	require.Equal(t, []byte("trunk-data"), got)
}

// TestBranchCache_StorageTrunkPin verifies PinEntry routes a storage-trunk
// prefix (>= 64 nibbles) into its per-contract storage trunk, is served from
// the pinned tier, and counts toward PinnedCount.
func TestBranchCache_StorageTrunkPin(t *testing.T) {
	c := NewBranchCache(100)

	// 33-byte compact prefix: even flag (0x00) + 32-byte account hash = 64
	// nibbles exactly → the storage trunk's depth-0 slot for that contract.
	prefix := make([]byte, 33)
	for i := 1; i < 33; i++ {
		prefix[i] = byte(i)
	}
	c.PinEntry(prefix, []byte("storage-root"), 0)
	require.Equal(t, 1, c.PinnedCount())

	got, _, ok := c.Get(prefix)
	require.True(t, ok)
	require.Equal(t, []byte("storage-root"), got)
	require.Equal(t, uint64(1), c.pinnedHits.Load())
}

func TestBranchCache_ConcurrentFirstPinsShareStorageTrunk(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	path := make([]byte, 65)
	path[64] = 1
	prefixA := nibbles.HexToCompact(path)
	path[64] = 2
	prefixB := nibbles.HexToCompact(path)

	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	c.pinnedMu.Lock()
	var writers sync.WaitGroup
	pin := func(started chan<- struct{}, prefix, value []byte) {
		defer writers.Done()
		close(started)
		c.PinEntry(prefix, value, 0)
	}

	started := make(chan struct{})
	writers.Add(1)
	go pin(started, prefixA, []byte("a"))
	<-started
	runtime.Gosched()

	started = make(chan struct{})
	writers.Add(1)
	go pin(started, prefixB, []byte("b"))
	<-started
	runtime.Gosched()

	c.pinnedMu.Unlock()
	writers.Wait()

	got, _, ok := c.Get(prefixA)
	require.True(t, ok)
	require.Equal(t, []byte("a"), got)
	got, _, ok = c.Get(prefixB)
	require.True(t, ok)
	require.Equal(t, []byte("b"), got)
}

// TestBranchCache_RootPinning verifies the root branch lands in the pinned
// slot (counted as root-hit) and tail entries land in the LRU tier
// (counted as tail-hit).
func TestBranchCache_RootPinning(t *testing.T) {
	c := NewBranchCache(100)

	rootKey := []byte{0x00} // compact-encoded empty nibble path = root branch
	deepKey := []byte{0x12, 0x34, 0x56}
	c.Put(rootKey, []byte("root-data"), 0)
	c.Put(deepKey, []byte("deep-data"), 0)

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
	c.Put(rootKey, []byte("ROOT-PERSISTS"), 0)

	// Stuff the tail well past capacity
	for i := range 100 {
		c.Put([]byte{byte(i), byte(i)}, []byte{byte(i)}, 0)
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
	c.Put(rootKey, []byte("r"), 0)
	c.Put(deepKey, []byte("d"), 0)

	c.Invalidate(rootKey)
	_, _, ok := c.Get(rootKey)
	require.False(t, ok, "root invalidated")

	c.Invalidate(deepKey)
	_, _, ok = c.Get(deepKey)
	require.False(t, ok, "deep invalidated")
}

// TestBranchCache_Reset empties everything and resets stats.
func TestBranchCache_Reset(t *testing.T) {
	c := NewBranchCache(100)
	deepKey := []byte{0x12, 0x34, 0x56} // 5 nibbles → LRU tail
	c.Put([]byte{0x00}, []byte("r"), 0)
	c.Put(deepKey, []byte("d"), 0)
	_, _, _ = c.Get([]byte{0x00})
	_, _, _ = c.Get(deepKey)

	require.Equal(t, uint64(1), c.rootHits.Load())
	require.Equal(t, uint64(1), c.tailHits.Load())

	c.Reset()
	require.Equal(t, uint64(0), c.rootHits.Load())
	require.Equal(t, uint64(0), c.tailHits.Load())
	_, _, ok := c.Get([]byte{0x00})
	require.False(t, ok)
	_, _, ok = c.Get(deepKey)
	require.False(t, ok)
}

func TestBranchCache_CloseClearsEntries(t *testing.T) {
	c := NewBranchCache(100)
	key := []byte{0x00}
	c.Put(key, []byte("root"), 0)

	c.Close()

	_, _, ok := c.Get(key)
	require.False(t, ok)
}

func resetDuringBlockedBranchCacheWrite(c *BranchCache, block *sync.Mutex, write func()) {
	// Limit Go execution to one logical processor. Each runtime.Gosched call
	// yields to the queued goroutine, which runs until it reaches the blocked lock.
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
		c.Reset()
		close(clearDone)
	}()
	runtime.Gosched()

	block.Unlock()
	<-writerDone
	<-clearDone
}

func TestBranchCache_ResetFencesStartedPut(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	key := []byte{0x12, 0x34, 0x56}
	resetDuringBlockedBranchCacheWrite(c, &c.tailMu, func() {
		c.Put(key, []byte("dead-fork-branch"), 0)
	})

	_, _, ok := c.Get(key)
	require.False(t, ok, "Reset must remove a Put that started before the reset")
}

func TestBranchCache_ResetFencesStartedPinEntry(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	key := make([]byte, 33)
	key[32] = 1
	resetDuringBlockedBranchCacheWrite(c, &c.pinnedMu, func() {
		c.PinEntry(key, []byte("dead-fork-branch"), 0)
	})

	_, _, ok := c.Get(key)
	require.False(t, ok, "Reset must remove a PinEntry that started before the reset")
}

// TestBranchCache_Stats verifies the format of the stats string is
// deterministic and contains the expected per-tier counts.
func TestBranchCache_Stats(t *testing.T) {
	c := NewBranchCache(100)
	// 3-byte odd-flag prefixes are 5 nibbles deep → LRU tail (the account
	// trunk only holds depths 1-4).
	tailHit := []byte{0x12, 0x34, 0x56}
	tailMiss := []byte{0x12, 0x34, 0x57}
	c.Put([]byte{0x00}, []byte("rrr"), 0)
	c.Put(tailHit, []byte("ddd"), 0)
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

// TestBranchCache_StateKeyNeverCached pins that the commitment checkpoint key is
// never served or stored (serving a stale checkpoint corrupts the trie root),
// and that invalidating it doesn't evict real entries.
func TestBranchCache_StateKeyNeverCached(t *testing.T) {
	c := NewBranchCache(100)
	defer c.Close()

	c.Put(KeyCommitmentState, []byte("checkpoint"), 1)
	_, _, ok := c.Get(KeyCommitmentState)
	require.False(t, ok, "state key must never be served from the cache")
	require.Equal(t, 0, c.tailLen(), "state key must not occupy a tail slot")

	deepKey := []byte{0x12, 0x34}
	c.Put(deepKey, []byte("d"), 0)
	c.Invalidate(KeyCommitmentState)
	got, _, ok := c.Get(deepKey)
	require.True(t, ok, "invalidating the state key must not evict real entries")
	require.Equal(t, []byte("d"), got)
}

// TestBranchCache_ConcurrentTailGrow drives concurrent tail Puts well past the
// 512-entry start capacity so maybeGrow runs under contention. It regresses the
// data race where Add read tailLRU.curCap unsynchronized while maybeGrow/reset
// wrote it under resizeMu. Must be run under -race to be meaningful.
func TestBranchCache_ConcurrentTailGrow(t *testing.T) {
	c := NewBranchCache(4096) // max >> 512 start, so the tail actually grows
	defer c.Close()

	const (
		workers   = 8
		perWorker = 2000 // 16k distinct deep keys >> 512 → forces maybeGrow
	)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				// odd flag (0x10) + 3 bytes → 7 nibbles → tail; unique per (w,i).
				key := []byte{0x10, byte(w), byte(i), byte(i >> 8)}
				c.Put(key, []byte{byte(i)}, 0)
				c.Get(key)
			}
		})
	}
	wg.Wait()
}

func TestBranchCache_ViewRequiresExactGeneration(t *testing.T) {
	c := NewBranchCache(100)
	t.Cleanup(c.Close)
	publisher := c.Publisher()
	publisher.Initialize(testBranchGeneration(7))

	key := []byte{0xa0, 0xb0}
	view := c.View(testBranchGeneration(7))
	view.Fill(key, []byte("version-7"), 3)

	value, step, ok := view.Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("version-7"), value)
	require.Equal(t, uint64(3), step)

	_, _, ok = c.View(testBranchGeneration(6)).Get(key)
	require.False(t, ok, "an older database snapshot must not read the current branch generation")
	_, _, ok = c.View(testBranchGeneration(8)).Get(key)
	require.False(t, ok, "a newer database snapshot must wait for its branch generation to be published")
	_, _, ok = c.View(cache.BranchGeneration(7, 1)).Get(key)
	require.False(t, ok, "a different files view must not read the current branch generation")
}

func TestBranchCache_PublicationRejectsLateFill(t *testing.T) {
	c := NewBranchCache(100)
	t.Cleanup(c.Close)
	publisher := c.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	key := []byte{0xa0, 0xb0}
	oldView := c.View(testBranchGeneration(1))
	oldView.Fill(key, []byte("old"), 1)

	publication := publisher.Begin()
	_, _, ok := oldView.Get(key)
	require.False(t, ok, "Begin must revoke existing branch views")
	oldView.Fill(key, []byte("late-old-fill"), 1)

	publication.Publish(testBranchGeneration(2), []BranchUpdate{{
		Key:   key,
		Value: []byte("new"),
		Step:  2,
	}}, false, nil)

	_, _, ok = oldView.Get(key)
	require.False(t, ok, "a published generation must not revalidate an old view")
	value, step, ok := c.View(testBranchGeneration(2)).Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("new"), value)
	require.Equal(t, uint64(2), step)
}

func TestBranchCache_PublicationAbortRestoresPreviousView(t *testing.T) {
	c := NewBranchCache(100)
	t.Cleanup(c.Close)
	publisher := c.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	key := []byte{0xa0, 0xb0}
	view := c.View(testBranchGeneration(1))
	view.Fill(key, []byte("unchanged"), 1)

	publication := publisher.Begin()
	_, _, ok := view.Get(key)
	require.False(t, ok)

	publication.Abort()
	value, _, ok := view.Get(key)
	require.True(t, ok, "rollback must restore the unchanged branch generation")
	require.Equal(t, []byte("unchanged"), value)
}

func TestBranchCache_ResetRevokesViewsUntilNextPublication(t *testing.T) {
	c := NewBranchCache(100)
	t.Cleanup(c.Close)
	publisher := c.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	key := []byte{0xa0, 0xb0}
	oldView := c.View(testBranchGeneration(1))
	oldView.Fill(key, []byte("old-layout"), 1)

	c.Reset()
	_, _, ok := oldView.Get(key)
	require.False(t, ok)
	_, _, ok = c.View(testBranchGeneration(1)).Get(key)
	require.False(t, ok, "Reset must leave the cache unpublished")

	publication := publisher.Begin()
	publication.Publish(testBranchGeneration(2), []BranchUpdate{{
		Key:   key,
		Value: []byte("new-layout"),
		Step:  2,
	}}, false, nil)
	value, _, ok := c.View(testBranchGeneration(2)).Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("new-layout"), value)
}
