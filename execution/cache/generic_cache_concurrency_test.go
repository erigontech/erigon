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
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/maphash"
)

// largeCodePayloadBytes is a big-content payload size for the growLRU sizing
// sweeps; the code layers themselves are byte-bounded and no longer use it.
const largeCodePayloadBytes = 12 * 1024

// TestGenericCache_ConcurrentPutAndGet is the race-detector smoke test: many
// goroutines put and read distinct keys. Run with -race, this must stay clean.
func TestGenericCache_ConcurrentPutAndGet(t *testing.T) {
	c := closeOnCleanup(t, NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU))

	const workers = 8
	const perWorker = 20_000
	var wg sync.WaitGroup
	for w := range workers {
		base := w
		wg.Go(func() {
			key := make([]byte, 8)
			for i := range perWorker {
				binary.BigEndian.PutUint64(key, uint64(base*perWorker+i))
				c.Put(key, []byte{byte(i)}, uint64(i))
				c.Get(key)
			}
		})
	}
	wg.Wait()
}

// fixedGenericCache is a byte-bounded cache over a budgeted layer, so its
// resident bytes are tracked exactly, sized by the test.
func fixedGenericCache(t *testing.T, capacityBytes datasize.ByteSize, mode Mode) *GenericCache[[]byte] {
	return closeOnCleanup(t, &GenericCache[[]byte]{
		capacityB: capacityBytes,
		mode:      mode,
		sizeFunc:  func(v []byte) int { return len(v) },
		data:      newByteLRU(capacityBytes, weighEntry[[]byte], nil),
	})
}

// oneEntryBytes is what one entry of key and value weighs.
func oneEntryBytes(key, val []byte) datasize.ByteSize {
	return datasize.ByteSize(weighEntry(0, entry[[]byte]{size: uint32(len(key) + len(val))}))
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
	v := []byte("valuevalu")
	c := fixedGenericCache(t, oneEntryBytes(a, v), ModeNoOp) // fits exactly one entry
	c.Put(a, v, 1)
	for round := range 200000 {
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(a, v, 2) })
		wg.Go(func() { c.Put(b, v, 1) })
		wg.Wait()
		if _, ok := c.Get(b); ok {
			t.Fatalf("round %d: ModeNoOp admitted a key past a full budget (SizeBytes=%d, capacityB=%d)",
				round, c.SizeBytes(), c.CapacityBytes())
		}
	}
}

// A capacity eviction is a size-subtracting writer the put stripes cannot
// serialize: an insert on one stripe evicts a key whose own update, on another
// stripe, is mid-flight. With room for one entry every insert evicts the
// other key, so any drift in the byte counter accumulates and shows after the
// settle deletes.
func TestGenericCache_CapacityEvictionAtomicWithPut_NoSizeDrift(t *testing.T) {
	a := makeAddr(1)
	var b []byte
	for i := 2; ; i++ {
		b = makeAddr(i)
		if maphash.Hash(a)&(putStripeCount-1) != maphash.Hash(b)&(putStripeCount-1) {
			break
		}
	}
	v := []byte("value-one")
	c := fixedGenericCache(t, oneEntryBytes(a, v), ModeEvictLRU)
	for range 100000 {
		c.Put(b, v, 10)
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(a, v, 10) }) // insert → evicts b (cap 1)
		wg.Go(func() { c.Put(b, v, 20) }) // same-key update path
		wg.Wait()
	}
	c.Delete(a)
	c.Delete(b)
	c.data.c.CleanUp()
	require.Zero(t, c.SizeBytes(), "capacity eviction raced the update-path delta")
}

// The failure mode is a pre-Clear epoch stamped on post-Clear storage. If Clear
// reused that epoch value, a later unwind could reach the same value and treat
// the entry as current even though its txNum is above the unwind floor.
//
// The test holds the key's stripe, then queues Clear before Put. Waiting beyond
// the mutex starvation threshold makes the unlock hand the stripe to Clear
// first. Clear must keep the stripe through the data and coherence generation
// changes, so Put stamps the post-Clear epoch and a later unwind invalidates it.
func TestGenericCache_ClearRacingPut_EpochAlias(t *testing.T) {
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	c.Unwind(300) // epoch 0 -> 1

	key := []byte("epoch-alias-key")
	mu := &c.putStripes[maphash.Hash(key)&(putStripeCount-1)]
	mu.Lock()

	var wg sync.WaitGroup
	wg.Go(func() { c.Clear() })
	time.Sleep(5 * time.Millisecond)
	wg.Go(func() { c.Put(key, []byte("dead-fork-value"), 200) })
	time.Sleep(5 * time.Millisecond)
	mu.Unlock()
	wg.Wait()

	c.Unwind(150)

	_, ok := c.Get(key)
	require.False(t, ok, "entry at txNum 200 outlived an unwind to 150")
}

// A reader that captures a dead (unwind-invalidated) entry from the retiring
// generation must not have it revalidated by Clear's coherence reset:
// judged against the post-Reset state (new epoch, lifted floor), the entry
// passes IsStale and dead-fork state is served. Coherence is snapshotted
// before the generation load, so an old-generation entry is always judged by
// pre-Reset coherence that still carries the unwind.
//
// The reader gates on the fence reaching the key's stripe — the last one the
// sweep locks — so its Get lands next to the Reset that follows.
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
		wg.Go(func() { c.Clear() })
		wg.Go(func() {
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
		})
		wg.Wait()
		require.False(t, served.Load(),
			"round %d: Clear revalidated an unwind-invalidated entry for a concurrent reader", round)
	}
}

// A byte budget whose slot quotient exceeds a uint32 must clamp to the ceiling,
// not wrap into a tiny cache.
func TestSlotCeilingClampsAboveUint32(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	// Exactly 2^32 slots, so the quotient no longer fits the uint32 the clamp
	// used to be applied after.
	overflow := func(perSlot uint64) datasize.ByteSize { return datasize.ByteSize(perSlot << 32) }

	g := newGrowLRU[codeSizeEntry](overflow(avgBytesPerEntry+uint64(slotChargeBytes(elemBytesFor[codeSizeEntry]()))), 0, nil)
	t.Cleanup(g.Close)
	require.Equal(t, uint32(maxCacheSlots), g.maxCap)
}

func TestCeilingTerminatesAboveTheSlotClamp(t *testing.T) {
	t.Parallel()

	free := func(uint32) int64 { return 0 }
	for _, estimate := range []uint32{0, 1, 3, 1 << 31, math.MaxUint32} {
		require.Equal(t, uint32(math.MaxUint32),
			fitCeiling(estimate, math.MaxUint32, datasize.ByteSize(math.MaxInt64), free),
			"seeded at %d", estimate)
	}
}

// The same for the unsharded ceiling, over the value types that reach the widest
// freelru elements. Its budget funds one table, so the strand is the same band.
func TestGrowLRUCeilingLeavesNoAffordableSlot(t *testing.T) {
	t.Parallel()

	for _, budget := range []datasize.ByteSize{16 * datasize.MB, 64 * datasize.MB, 512 * datasize.MB} {
		for _, payload := range []uint32{1, 64, 12288} {
			g := closeOnCleanup(t, newGrowLRU[codeEntry](budget, payload, nil))
			cost := func(c uint32) int64 { return growLRUBytes(c, g.procs, g.avgBytes, g.elemBytes) }
			require.LessOrEqual(t, cost(g.maxCap), int64(budget),
				"newGrowLRU(%s, %d) ceiling %d is outside its own budget", budget, payload, g.maxCap)
			if g.maxCap < maxCacheSlots {
				require.Greater(t, cost(g.maxCap+1), int64(budget),
					"newGrowLRU(%s, %d) stopped at %d with a slot still affordable", budget, payload, g.maxCap)
			}
		}
	}
}

// growLRU fits its slot ceiling against the shard geometry it is constructed
// with. GOMAXPROCS can change under a running process, so a generation built
// later must not be sized by a different geometry than the ceiling was fitted
// against: the cache would exceed its configured budget and eat into every
// other cache's allowance. Reserve also ignores a non-positive argument and
// Release a negative one, so the walk changes GOMAXPROCS in both directions.
func TestGrowLRU_ByteBudgetHoldsAcrossShardCountChanges(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(0)
	prevBudget := cachebudget.Global
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prevProcs)
		cachebudget.Global = prevBudget
	})

	// freelru selects the shard from bits 16+ of the key, so a counter would
	// fill shard 0 alone and never reach the capacity a grow is triggered on.
	fill := func(g *growLRU[codeEntry], upTo uint32) {
		for i := range upTo {
			g.Add(spreadKey(uint64(i)), codeEntry{})
		}
	}

	const budget = 1 * datasize.GB
	// ceilingCost walks a cache to its ceiling with GOMAXPROCS changed to `to`
	// halfway, and reports what the full generation charged the envelope.
	ceilingCost := func(t *testing.T, at, to int) int64 {
		cachebudget.Global = cachebudget.New(math.MaxInt64)
		runtime.GOMAXPROCS(at)
		g := newGrowLRU[codeEntry](budget, largeCodePayloadBytes, nil)
		defer g.Close()
		settled := func(step string) {
			t.Helper()
			require.Equal(t, g.reserved, cachebudget.Global.Used(), "procs %d->%d after %s", at, to, step)
			require.LessOrEqual(t, g.reserved, int64(budget),
				"procs %d->%d after %s: %d slots cost %d B, over the %s they were fitted to",
				at, to, step, g.curCap.Load(), g.reserved, budget)
		}
		settled("construction")

		fill(g, 4*genericCacheStartCapacity)
		settled("grow")

		runtime.GOMAXPROCS(to)
		fill(g, g.maxCap)
		require.Equal(t, g.maxCap, g.curCap.Load(), "procs %d->%d: never reached the ceiling", at, to)
		settled("grow to the ceiling")
		cost := g.reserved

		g.Purge()
		settled("purge")

		runtime.GOMAXPROCS(at)
		g.Purge()
		settled("purge back at the original shard count")

		g.Close()
		require.Zero(t, cachebudget.Global.Used(),
			"procs %d->%d: Close left %d B charged", at, to, cachebudget.Global.Used())
		return cost
	}

	for _, procs := range [][2]int{{1, 32}, {32, 1}, {8, 64}, {64, 8}} {
		require.Equal(t, ceilingCost(t, procs[0], procs[0]), ceilingCost(t, procs[0], procs[1]),
			"a GOMAXPROCS change from %d to %d resized the ceiling the budget was fitted against",
			procs[0], procs[1])
	}
}

// Close settles the reservation once. A later grow or purge that charged the
// envelope again would hold bytes nothing releases.
func TestGrowLRU_CloseStopsFurtherReservations(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	g := newGrowLRUEntries[codeSizeEntry](100_000, 8, nil)
	g.Close()
	require.Zero(t, cachebudget.Global.Used())

	for i := range 4 * genericCacheStartCapacity {
		g.Add(spreadKey(uint64(i)), codeSizeEntry{})
	}
	require.Zero(t, cachebudget.Global.Used(), "a grow after Close charged the envelope")

	g.Purge()
	require.Zero(t, cachebudget.Global.Used(), "a purge after Close charged the envelope")
	require.Zero(t, g.Len(), "a purge after Close must still empty the cache")
}

// spreadKey mixes a counter into the bits freelru selects a shard with, so a
// test fills a growLRU the way hashed production keys do.
func spreadKey(i uint64) uint64 { return i * 0x9E3779B97F4A7C15 }
