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
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)

	const workers = 8
	const perWorker = 20_000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			key := make([]byte, 8)
			for i := 0; i < perWorker; i++ {
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
	for round := 0; round < 50; round++ {
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
		for i := 0; i < 3*genericCacheStartCapacity; i++ {
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
	for round := 0; round < 100; round++ {
		c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
		key := make([]byte, 8)
		for i := 0; i < 256; i++ {
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
	for round := 0; round < 100000; round++ {
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

// Intentional removals are netted out of the evictions metric; doing that by
// decrementing the shared counter races PrintStatsAndReset's Swap(0) — the
// swap can land between OnEvict's increment and the decrement, underflowing
// the counter to ~1.8e19 for the next interval. The sampler plays the stats
// reset against a Delete hammer and must never observe an absurd value.
func TestGenericCache_StatsResetAtomicWithDelete_NoEvictionsUnderflow(t *testing.T) {
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
	defer wg.Wait()
	defer close(stop)
	for i := 0; i < 1_000_000; i++ {
		if ev := c.evictions.Swap(0); ev > 1<<40 {
			t.Fatalf("stats reset racing an intentional removal underflowed the evictions counter: %d", ev)
		}
	}
}
