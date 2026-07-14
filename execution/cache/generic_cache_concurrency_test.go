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

// A conditional put must keep deferring to a live entry across a grow: if the
// resize ever publishes a generation the entry hasn't reached yet, a
// PutIfAbsent arriving in that gap finds the key absent and inserts its
// (stale) value — the writer class the if-absent semantics exist to close.
// The prober watches for the generation swap and bursts conditional puts the
// moment it lands, mimicking a fill thread that starts a put mid-resize.
//
// The cache is seeded below any capacity pressure with the hot key inserted
// last — the LRU victim is always an older seed key, so the hot key cannot be
// evicted and a stale value at the end can only have come through a resize
// gap. The grow is forced by lowering curCap: reaching Len >= startCap
// organically needs every freelru shard full, which would make the hot key
// evictable and the signal ambiguous.
func TestGenericCache_PutIfAbsentDefersAcrossGrow(t *testing.T) {
	fresh := []byte("fresh-value")
	stale := []byte("stale-value")
	for round := 0; round < 50; round++ {
		c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
		key := make([]byte, 8)
		for i := 0; i < 512; i++ {
			binary.BigEndian.PutUint64(key, uint64(1+i))
			c.Put(key, []byte{1}, 1)
		}
		hot := []byte("hot-key")
		c.Put(hot, fresh, 10)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			before := c.data.Load()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if c.data.Load() != before {
					for j := 0; j < 4096; j++ {
						c.PutIfAbsent(hot, stale, 5)
					}
					return
				}
			}
		}()

		c.curCap.Store(uint32(c.Len()))
		binary.BigEndian.PutUint64(key, 0)
		c.Put(key, []byte{1}, 1) // insert at the lowered cap → triggers the grow

		close(stop)
		wg.Wait()
		v, ok := c.Get(hot)
		require.True(t, ok, "round %d: hot key missing", round)
		require.Equal(t, fresh, v, "round %d: PutIfAbsent bypassed the live entry across a grow", round)
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
