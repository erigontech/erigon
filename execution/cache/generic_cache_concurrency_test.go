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

package cache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/maphash"
)

// TestGenericCache_ConcurrentPutAcrossGrow guards the jump-grow data race:
// curCap is written under resizeMu in maybeGrow but read on the put fast-path
// outside it, so concurrent writers crossing the grow threshold raced on it
// (surfaced by the -race eest shard). Many goroutines insert enough distinct
// keys to trigger several grow steps while others put concurrently; run with
// -race, this must stay clean.
func TestGenericCache_ConcurrentPutAcrossGrow(t *testing.T) {
	// Budget well above the start size (1024 slots) so maybeGrow fires repeatedly.
	c := closeOnCleanup(t, NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU))

	const workers = 8
	const perWorker = 20_000
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			key := make([]byte, 8)
			for i := range perWorker {
				binary.BigEndian.PutUint64(key, uint64(base*perWorker+i))
				c.Put(key, []byte{byte(i)}, uint64(i))
				c.Get(key)
			}
		}(w)
	}
	wg.Wait()
}

// A same-key put serialized by its stripe must never be undone by a grow: with
// copy-then-swap migration, a writer that loaded the old generation before the
// swap landed its write in the abandoned generation, and the migrated (older)
// value resurfaced as live — a stale serve, not a benign miss. The writer
// self-verifies each put and a reader checks the hot key's monotonically
// increasing value never goes backward.
func TestGenericCache_PutNotLostAcrossGrow(t *testing.T) {
	value := func(n uint64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, n)
		return b
	}
	for round := range 50 {
		c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
		hot := []byte("hot-key")
		c.Put(hot, value(0), 1)

		stop := make(chan struct{})
		var regressed atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for n := uint64(1); ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				c.Put(hot, value(n), n)
				if v, ok := c.Get(hot); ok {
					if got := binary.BigEndian.Uint64(v); got < n {
						regressed.Store(true)
						return
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			last := uint64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if v, ok := c.Get(hot); ok {
					if n := binary.BigEndian.Uint64(v); n < last {
						regressed.Store(true)
						return
					} else {
						last = n
					}
				}
			}
		}()

		// Cross the grow threshold so maybeGrow swaps the generation while the
		// hot-key writer runs.
		key := make([]byte, 8)
		for i := range 3 * genericCacheStartCapacity {
			binary.BigEndian.PutUint64(key, uint64(1+i))
			c.Put(key, []byte{1}, 1)
		}

		close(stop)
		wg.Wait()
		c.Close()
		require.False(t, regressed.Load(), "round %d: a striped put was lost across a grow (older value resurfaced)", round)
	}
}

// A conditional put must keep deferring to a live entry across a grow. The
// vulnerable writer class: a put of a brand-new key that lands in the
// retiring generation after the copy snapshotted Keys() is lost on the swap,
// and a follow-up PutIfAbsent finds the key absent and installs its stale
// value as live. With the fence the put either lands pre-fence (and is
// migrated — Keys() is taken with every stripe held) or lands in the new
// generation; either way the conditional put defers.
//
// A writer hammers fresh keys while the grow swaps generations; every key
// that straddled the swap is then probed with a stale conditional put. The
// grow is forced by lowering curCap over a lightly-populated cache, so
// capacity eviction cannot explain a missing key.
func TestGenericCache_PutIfAbsentDefersAcrossGrow(t *testing.T) {
	fresh := []byte("fresh-value")
	stale := []byte("stale-value")
	for round := range 100 {
		c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
		key := make([]byte, 8)
		for i := range 256 {
			binary.BigEndian.PutUint64(key, uint64(1+i))
			c.Put(key, []byte{1}, 1)
		}
		before := c.data.Load()
		c.curCap.Store(uint32(c.Len()))

		var candidates [][]byte
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				k := make([]byte, 9)
				k[0] = 0xfe
				binary.BigEndian.PutUint64(k[1:], uint64(j))
				c.Put(k, fresh, 10)
				candidates = append(candidates, k)
				if c.data.Load() != before {
					return
				}
			}
		}()

		binary.BigEndian.PutUint64(key, 0)
		c.Put(key, []byte{1}, 1) // insert at the lowered cap → triggers the grow
		close(stop)
		wg.Wait()

		for _, k := range candidates {
			c.PutIfAbsent(k, stale, 5)
		}
		for i, k := range candidates {
			v, ok := c.Get(k)
			require.True(t, ok, "round %d: candidate %d missing", round, i)
			require.Equal(t, fresh, v,
				"round %d: candidate %d: PutIfAbsent installed a stale value over a put lost in the retiring generation", round, i)
		}
		c.Close()
	}
}

// A ModeNoOp admission must never observe the byte counter mid-update: the
// update path removes the old entry before adding the new one, and a
// concurrent insert on another stripe that reads the transient dip passes the
// budget check and lands over capacity — breaking "drop new keys when full"
// with a key that should never have been admitted. The counter is reserved
// before the removal, so the budget is transiently over-stated (at worst
// dropping a new key) and never under-stated.
func TestGenericCache_ModeNoOpAdmissionAtomicWithUpdate(t *testing.T) {
	a := []byte("key-a-aaaaaaaaaaaaaa")
	var b []byte
	for i := 0; ; i++ {
		cand := []byte("key-b-bbbbbbbbbbbbb" + string(rune('a'+i%26)))
		if maphash.Hash(a)&(putStripeCount-1) != maphash.Hash(cand)&(putStripeCount-1) {
			b = cand
			break
		}
	}
	v := []byte("valuevalu") // entry size 20+9+24 = 53: the budget fits exactly one entry
	c := newGenericCacheEntries(datasize.ByteSize(53), 8, func(v []byte) int { return len(v) }, ModeNoOp)
	c.Put(a, v, 1)
	for round := range 200000 {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.Put(a, v, 2) }()
		go func() { defer wg.Done(); c.Put(b, v, 1) }()
		wg.Wait()
		if _, ok := c.Get(b); ok {
			t.Fatalf("round %d: ModeNoOp admitted a key past a full budget (SizeBytes=%d, capacityB=%d)",
				round, c.SizeBytes(), c.CapacityBytes())
		}
	}
}

// A grow must migrate every entry. Left to pick its own geometry per
// generation, freelru chooses more, smaller shards as capacity rises, and a
// new shard that overfills during the copy silently evicts — keys clustered
// on the shard-selection bits vanish across a "grow", and a follow-up
// conditional put can install a stale value in the hole. Seeding writes the
// clustered keys after the pad so they are the newest in their shard and
// cannot be seeding-eviction victims; only the migration can lose them.
func TestGenericCache_GrowMigrationLossless(t *testing.T) {
	c := NewGenericCacheWithAvg[[]byte](4*datasize.MB, 256, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()

	// Keys sharing hash bits 16-23 land in one shard of any generation with up
	// to 256 shards.
	target := (maphash.Hash([]byte("cluster-seed")) >> 16) & 255
	var clustered [][]byte
	for i := 0; len(clustered) < 24; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if (maphash.Hash(k)>>16)&255 == target {
			clustered = append(clustered, k)
		}
	}
	pad := make([]byte, 9)
	for j := 0; c.Len() < genericCacheStartCapacity-len(clustered); j++ {
		binary.BigEndian.PutUint64(pad[1:], uint64(j))
		c.Put(pad, []byte{1}, 1)
	}
	for _, k := range clustered {
		c.Put(k, []byte("fresh"), 10)
	}
	for j := 1 << 20; c.Len() < genericCacheStartCapacity; j++ {
		binary.BigEndian.PutUint64(pad[1:], uint64(j))
		c.Put(pad, []byte{1}, 1)
		if j > 1<<21 {
			t.Fatal("seeding could not fill the cache to the grow threshold")
		}
	}
	before := c.data.Load()
	c.Put([]byte("grow-trigger"), []byte{1}, 1)
	require.NotEqual(t, before, c.data.Load(), "grow did not happen")

	lost := 0
	for _, k := range clustered {
		if _, ok := c.Get(k); !ok {
			lost++
		}
	}
	require.Zero(t, lost, "grow migration evicted clustered entries: per-shard capacity shrank across the swap")
}

// A capacity eviction is a size-subtracting writer the put stripes cannot
// serialize: freelru picks its victim per shard (hash bits 16+), so an insert
// on one stripe can evict a key whose own update — on another stripe — is
// between its Get and Add; delta accounting against the pre-eviction size then
// double-subtracts. Capacity 1 collapses freelru to a single shard, making any
// two keys same-shard; the keys are chosen to differ in their put stripe. Each
// hit leaks negative size; drift accumulates and shows after the settle
// deletes.
func TestGenericCache_CapacityEvictionAtomicWithPut_NoSizeDrift(t *testing.T) {
	c := newGenericCacheEntries(1*datasize.MB, 1, func(v []byte) int { return len(v) }, ModeEvictLRU)
	a := makeAddr(1)
	var b []byte
	for i := 2; ; i++ {
		b = makeAddr(i)
		if maphash.Hash(a)&(putStripeCount-1) != maphash.Hash(b)&(putStripeCount-1) {
			break
		}
	}
	v := []byte("value-one")
	for range 100000 {
		c.Put(b, v, 10)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.Put(a, v, 10) }() // insert → evicts b (cap 1)
		go func() { defer wg.Done(); c.Put(b, v, 20) }() // same-key update path
		wg.Wait()
	}
	c.Delete(a)
	c.Delete(b)
	require.Zero(t, c.SizeBytes(), "capacity eviction raced the update-path delta")
}

// A put samples the coherence epoch and then contends for its stripe; a Clear
// that wins the stripe first resets the epoch counter, so the put would stamp
// a pre-Clear epoch onto an entry landing in the post-Clear generation. Once
// a later unwind re-reaches that epoch value, the entry aliases the live
// epoch and serves dead-fork state despite its txNum being at or above the
// floor.
//
// The test holds the key's stripe to park Clear on it (before the reset,
// which runs inside the fence) and then the put behind it; waits beyond 1ms
// put the mutex in starvation mode, so unlocking hands the stripe FIFO to
// Clear first.
func TestGenericCache_ClearRacingPut_EpochAlias(t *testing.T) {
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	c.Unwind(300) // epoch 0 -> 1

	key := []byte("epoch-alias-key")
	mu := &c.putStripes[maphash.Hash(key)&(putStripeCount-1)]
	mu.Lock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.Clear() }()
	time.Sleep(5 * time.Millisecond)
	go func() { defer wg.Done(); c.Put(key, []byte("dead-fork-value"), 200) }()
	time.Sleep(5 * time.Millisecond)
	mu.Unlock()
	wg.Wait()

	c.Unwind(150) // epoch 0 -> 1 again, floor 150

	_, ok := c.Get(key)
	require.False(t, ok, "entry at txNum 200 outlived an unwind to 150: its pre-Clear epoch stamp aliases the live epoch")
}

// A reader that captures a dead (unwind-invalidated) entry from the retiring
// generation must not have it revalidated by Clear's coherence re-init:
// judged against the post-Init state (fresh epoch, lifted floor), the entry
// passes IsStale and dead-fork state is served. Coherence is snapshotted
// before the generation load, so an old-generation entry is always judged by
// coherence that still carries the unwind.
//
// The reader gates on the fence reaching the key's stripe — the last one the
// sweep locks — so its Get lands next to the Init that follows.
func TestGenericCache_ClearRacingGet_DeadEntryStaysDead(t *testing.T) {
	var key []byte
	for i := 0; ; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if maphash.Hash(k)&(putStripeCount-1) == putStripeCount-1 {
			key = k
			break
		}
	}
	dead := []byte("dead-fork-value")
	c := NewDomainCacheMode(1*datasize.MB, ModeEvictLRU)
	defer c.Close()
	for round := range 2000 {
		c.Put(key, dead, 200)
		c.Unwind(150) // the entry is dead-fork state; it must never be served again
		var served atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.Clear() }()
		go func() {
			defer wg.Done()
			mu := &c.putStripes[putStripeCount-1]
			for range 1 << 16 {
				if mu.TryLock() {
					mu.Unlock()
					continue
				}
				break
			}
			for range 4 {
				if _, ok := c.Get(key); ok {
					served.Store(true)
					return
				}
			}
		}()
		wg.Wait()
		require.False(t, served.Load(),
			"round %d: Clear revalidated an unwind-invalidated entry for a concurrent reader", round)
	}
}

// The evictions counter must carry capacity evictions only. Routing
// intentional removals through it — decrement-compensated or netted against a
// removal counter at print time — races a concurrent stats reset: the swap
// straddles the paired updates, underflowing the counter or reporting phantom
// evictions that a later interval cannot retract. A Delete hammer with zero
// capacity pressure must therefore never surface a nonzero count, concurrent
// resets included.
func TestGenericCache_StatsResetAtomicWithDelete_NoPhantomEvictions(t *testing.T) {
	c := NewGenericCache[[]byte](1*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	key := []byte("metrics-key")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.Put(key, []byte{1}, 1)
			c.Delete(key)
		}
	}()
	total := uint64(0)
	for range 1_000_000 {
		total += c.evictions.Swap(0)
	}
	close(stop)
	wg.Wait()
	total += c.evictions.Swap(0)
	require.Zero(t, total, "intentional removals surfaced in the evictions metric")
}
