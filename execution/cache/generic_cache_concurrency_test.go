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
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/cachebudget"
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
// concurrent insert of another key that reads the transient dip passes the
// budget check and lands over capacity — breaking "drop new keys when full"
// with a key that should never have been admitted. The counter is reserved
// before the removal, so the budget is transiently over-stated (at worst
// dropping a new key) and never under-stated.
func TestGenericCache_ModeNoOpAdmissionAtomicWithUpdate(t *testing.T) {
	a, b := []byte("key-a-aaaaaaaaaaaaaa"), []byte("key-b-bbbbbbbbbbbbbb")
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

// A capacity eviction is a size-subtracting writer racing another key's
// update. With room for one entry every insert evicts the other key, so any
// drift in the byte counter accumulates and shows after the settle deletes.
func TestGenericCache_CapacityEvictionAtomicWithPut_NoSizeDrift(t *testing.T) {
	a, b := makeAddr(1), makeAddr(2)
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

// A Put racing Clear may stamp a pre-Clear epoch on an entry that lands after
// the wipe. Clear must not reuse that epoch value, or a later unwind could
// treat the entry as current although its txNum is above the unwind floor.
func TestGenericCache_ClearRacingPut_EpochAlias(t *testing.T) {
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	key := []byte("epoch-alias-key")
	for round := range 2000 {
		c.Unwind(300)
		var wg sync.WaitGroup
		wg.Go(func() { c.Clear() })
		wg.Go(func() { c.Put(key, []byte("dead-fork-value"), 200) })
		wg.Wait()
		c.Unwind(150)
		_, ok := c.Get(key)
		require.False(t, ok, "round %d: entry at txNum 200 outlived an unwind to 150", round)
	}
}

// A reader that captures a dead (unwind-invalidated) entry before Clear wipes
// it must not have it revalidated by Clear's coherence reset: judged against
// the post-Reset state (new epoch, lifted floor), the entry passes IsStale and
// dead-fork state is served. Coherence is snapshotted before the entry load,
// so an entry captured before the wipe is judged by pre-Reset coherence that
// still carries the unwind.
func TestGenericCache_ClearRacingGet_DeadEntryStaysDead(t *testing.T) {
	key := makeAddr(7)
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

// A fill that checks for a live entry and then inserts must not overwrite a
// Put that landed between the check and the insert: whichever order the two
// writers take, the live value wins.
func TestGenericCache_PutIfAbsentAtomicWithPut(t *testing.T) {
	key := []byte("live-vs-fill-key")
	live, fill := []byte("live-value"), []byte("fill-value")
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	for round := range 200000 {
		c.Delete(key)
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(key, live, 20) })
		wg.Go(func() { c.PutIfAbsent(key, fill, 10) })
		wg.Wait()
		if v, _ := c.Get(key); !bytes.Equal(v, live) {
			t.Fatalf("round %d: PutIfAbsent overwrote a live Put with %q", round, v)
		}
	}
}
