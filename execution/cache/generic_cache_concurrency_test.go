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
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/maphash"
)

// largeCodePayloadBytes is a big-content payload size for the growLRU sizing
// sweeps; the code layers themselves are byte-bounded and no longer use it.
const largeCodePayloadBytes = 12 * 1024

// The byte bound holds by construction: a cache filled far past its ceiling
// holds no more heap than the envelope reserved for it, entries and payload
// included, for a value behind a pointer and for one held inline.
func TestGenericCache_HeapStaysInsideItsReservation(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	const keyLen = 52
	t.Run("bytes", func(t *testing.T) {
		requireHeapInsideReservation(t, keyLen, avgStoragePayloadBytes, func() []byte { return make([]byte, avgStoragePayloadBytes-keyLen) })
	})
	t.Run("inline", func(t *testing.T) {
		requireHeapInsideReservation(t, keyLen, keyLen, func() [128]byte { return [128]byte{} })
	})
}

func requireHeapInsideReservation[T any](t *testing.T, keyLen int, payload uint32, value func() T) {
	const budget = 16 * datasize.MB
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	c := NewGenericCacheWithAvg[T](budget, payload, func(T) int { return 0 }, ModeEvictLRU)
	defer c.Close()
	key := make([]byte, keyLen)
	for i := range 4 * int(c.maxCap) {
		binary.BigEndian.PutUint64(key, uint64(i))
		c.Put(key, value(), 1)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	require.Equal(t, int(c.maxCap), c.allocatedSlots(), "the fill has to reach the ceiling")
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	require.Positive(t, held)
	require.LessOrEqual(t, held, c.reservedBytes.Load(), "a full cache holds more heap than it reserved")
	require.LessOrEqual(t, c.reservedBytes.Load(), int64(budget))
}

// grownShards counts the shards that have grown past their birth size, so a
// test can assert the grow it exercises actually happened.
func grownShards[T any](c *GenericCache[T]) int {
	n := 0
	for i := range c.shards {
		if uint32(len(*c.shards[i].tbl.Load())) > c.startBuckets {
			n++
		}
	}
	return n
}

// TestGenericCache_ConcurrentPutAcrossGrow is the race-detector smoke test for
// growth: many goroutines insert enough distinct keys to grow shards repeatedly
// while others put and read concurrently. Run with -race, this must stay clean.
func TestGenericCache_ConcurrentPutAcrossGrow(t *testing.T) {
	// Budget well above the start size so shards grow repeatedly.
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

// A same-key put serialized by its stripe must never be undone by a grow: a
// migration running outside the writer's exclusion lands the write in the table
// being retired, and the migrated (older) value resurfaces as live — a stale
// serve, not a benign miss. The writer self-verifies each put and a reader
// checks the hot key's monotonically increasing value never goes backward.
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
		wg.Go(func() {
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
		})
		wg.Go(func() {
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
		})

		// Fill past every shard's birth size while the hot-key writer runs. The
		// start size follows the shard count, which follows GOMAXPROCS, so the
		// fill is derived from it rather than from a fixed constant.
		key := make([]byte, 8)
		for i := range int(2 * c.startCap) {
			binary.BigEndian.PutUint64(key, uint64(1+i))
			c.Put(key, []byte{1}, 1)
		}

		close(stop)
		wg.Wait()
		require.Positive(t, grownShards(c), "round %d: no shard grew, the race under test never happened", round)
		c.Close()
		require.False(t, regressed.Load(), "round %d: a striped put was lost across a grow (older value resurfaced)", round)
	}
}

// A conditional put must keep deferring to a live entry across a grow. The
// vulnerable writer class: a put of a brand-new key that lands in a shard
// being rebuilt one step larger must survive, or a follow-up PutIfAbsent finds
// the key absent and installs its stale value as live. The rebuild runs under
// the shard's own lock, so a concurrent Add for that shard waits and cannot be
// dropped from the copy.
//
// A writer hammers fresh keys while the shards grow under it; each key is then
// probed with a stale conditional put. The cache is far below its ceiling
// throughout, so capacity eviction cannot explain a missing key.
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

		var candidates [][]byte
		stop := make(chan struct{})
		writing := make(chan struct{})
		var once sync.Once
		var wg sync.WaitGroup
		// Bound the writer well below the cache ceiling. It is meant to stop at
		// close(stop), but a scheduling skew must not let it evict its own early
		// candidates for capacity — that would fail the assertions below for a
		// reason this test is not about.
		limit := int(c.maxCap / 4)
		wg.Go(func() {
			for j := range limit {
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
				once.Do(func() { close(writing) })
			}
		})
		<-writing // the race under test needs the writer actually running

		for i := range 4096 {
			binary.BigEndian.PutUint64(key, uint64(1_000_000+i))
			c.Put(key, []byte{1}, 1) // fills shards → each grows a step
		}
		close(stop)
		wg.Wait()
		require.Positive(t, grownShards(c), "round %d: no shard grew, the race under test never happened", round)
		require.NotEmpty(t, candidates, "round %d: writer never ran, the assertions below prove nothing", round)

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

// A ModeNoOp admission must never observe the byte counter mid-update: a
// concurrent insert on another shard that reads a transient dip passes the
// budget check and lands over capacity — breaking "drop new keys when full"
// with a key that should never have been admitted.
func TestGenericCache_ModeNoOpAdmissionAtomicWithUpdate(t *testing.T) {
	v := []byte("valuevalu") // entry size 20+9+24 = 53: the budget fits exactly one entry
	c := newGenericCacheEntries(datasize.ByteSize(53), 128, func(v []byte) int { return len(v) }, ModeNoOp)
	require.Greater(t, len(c.shards), 1)
	a := []byte("key-a-aaaaaaaaaaaaaa")
	var b []byte
	for i := 0; ; i++ {
		cand := []byte("key-b-bbbbbbbbbbbbb" + string(rune('a'+i%26)))
		if c.shard(maphash.Hash(a)) != c.shard(maphash.Hash(cand)) {
			b = cand
			break
		}
	}
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

// A shard grow must migrate every entry of that shard: growing by a whole
// factor sends each old bucket into its own set of new buckets, so nothing can
// be evicted on the way, and the shard count is fixed for the life of the cache.
func TestGenericCache_GrowMigrationLossless(t *testing.T) {
	c := NewGenericCacheWithAvg[[]byte](4*datasize.MB, 256, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	target := &c.shards[3]
	buckets := func() int { return len(*target.tbl.Load()) }

	// One key more than the target shard has slots, so the last insert at the
	// latest is what forces the grow.
	var keys [][]byte
	want := buckets()*bucketWays + 1
	for i := 0; len(keys) < want; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if c.shard(maphash.Hash(k)) == target {
			keys = append(keys, k)
		}
	}

	before := buckets()
	for _, k := range keys {
		c.Put(k, []byte("fresh"), 10)
	}
	require.Greater(t, buckets(), before, "shard did not grow")

	for i, k := range keys {
		v, ok := c.Get(k)
		require.True(t, ok, "key %d lost in the shard migration", i)
		require.Equal(t, []byte("fresh"), v)
	}
}

// A capacity eviction subtracts its victim's size while a same-key update of
// that victim may run next to it; both happen under the shard mutex, so the
// byte counter cannot drift. Capacity 1 is a single bucket, so once it is full
// every insert of a new key evicts.
func TestGenericCache_CapacityEvictionAtomicWithPut_NoSizeDrift(t *testing.T) {
	c := newGenericCacheEntries(1*datasize.MB, 1, func(v []byte) int { return len(v) }, ModeEvictLRU)
	v := []byte("value-one")
	var keys [][]byte
	for i := 1; i <= 16; i++ {
		keys = append(keys, makeAddr(i))
	}
	for i := range 100000 {
		a, b := keys[i%len(keys)], keys[(i+1)%len(keys)]
		c.Put(b, v, 10)
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(a, v, 10) }) // insert → evicts once the bucket is full
		wg.Go(func() { c.Put(b, v, 20) }) // same-key update path
		wg.Wait()
	}
	require.Positive(t, c.evictions.Load(), "nothing was evicted, so the race under test never happened")
	for _, k := range keys {
		c.Delete(k)
	}
	require.Zero(t, c.SizeBytes(), "capacity eviction raced the update-path delta")
}

// The failure mode is a pre-Clear epoch stamped on post-Clear storage. If Clear
// reused that epoch value, a later unwind could reach the same value and treat
// the entry as current even though its txNum is above the unwind floor.
//
// The test holds the key's shard mutex, then queues Clear before Put. Waiting
// beyond the mutex starvation threshold makes the unlock hand the mutex to Clear
// first. Clear must keep it through the table and coherence generation
// changes, so Put stamps the post-Clear epoch and a later unwind invalidates it.
func TestGenericCache_ClearRacingPut_EpochAlias(t *testing.T) {
	c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	c.Unwind(300) // epoch 0 -> 1

	key := []byte("epoch-alias-key")
	mu := &c.shard(maphash.Hash(key)).mu
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
// The reader gates on the fence reaching the key's shard — the last one the
// sweep locks — so its Get lands next to the Reset that follows.
func TestGenericCache_ClearRacingGet_DeadEntryStaysDead(t *testing.T) {
	dead := []byte("dead-fork-value")
	c := NewDomainCacheMode(1*datasize.MB, ModeEvictLRU)
	defer c.Close()
	last := &c.shards[len(c.shards)-1]
	var key []byte
	for i := 0; ; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if c.shard(maphash.Hash(k)) == last {
			key = k
			break
		}
	}
	for round := range 2000 {
		c.Put(key, dead, 200)
		c.Unwind(150) // the entry is dead-fork state; it must never be served again
		var served atomic.Bool
		var wg sync.WaitGroup
		wg.Go(func() { c.Clear() })
		wg.Go(func() {
			mu := &last.mu
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
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.Put(key, []byte{1}, 1)
			c.Delete(key)
		}
	})
	total := uint64(0)
	for range 1_000_000 {
		total += c.evictions.Swap(0)
	}
	close(stop)
	wg.Wait()
	total += c.evictions.Swap(0)
	require.Zero(t, total, "intentional removals surfaced in the evictions metric")
}

// The entry count is kept per shard under that shard's lock, so an insert/evict mix can neither drive it negative nor let it drift
// from the shards it counts. A negative count breaks the ModeNoOp admission
// guard and panics any make() sized from it.
func TestGenericCache_LenTracksShards(t *testing.T) {
	// One shard, one slot: every insert evicts, so the count is churned as hard
	// as it can be while a reader samples it.
	c := newGenericCacheEntries(64*datasize.MB, 1, func(v []byte) int { return len(v) }, ModeEvictLRU)
	var negative atomic.Bool
	stop := make(chan struct{})
	var sampler, writers sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if c.Len() < 0 {
				negative.Store(true)
				return
			}
		}
	})
	for w := range 8 {
		writers.Go(func() {
			key := make([]byte, 8)
			for i := range 200_000 {
				binary.BigEndian.PutUint64(key, uint64(w)<<40|uint64(i))
				c.Put(key, []byte{1}, 1)
				if i%3 == 0 {
					c.Delete(key)
				}
			}
		})
	}
	writers.Wait()
	close(stop)
	sampler.Wait()
	require.False(t, negative.Load(), "entry count went negative")

	sum := 0
	for i := range c.shards {
		t := *c.shards[i].tbl.Load()
		for b := range t {
			for way := range bucketWays {
				if t[b].slots[way].Load() != nil {
					sum++
				}
			}
		}
	}
	require.Equal(t, sum, c.Len(), "entry count drifted from the shards it counts")
}

// Close must settle the envelope reservation behind the same fence a grow runs
// under. Outside it, a grow racing Close either hands its step back after Close
// already released it — leaving the shared budget permanently under-counted —
// or funds a step nothing gives back.
func TestGenericCache_CloseSettlesAgainstConcurrentGrow(t *testing.T) {
	before := cachebudget.Global.Used()
	for range 50 {
		c := NewGenericCache[[]byte](64*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU)
		perWorker := int(c.startCap) / 2
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Go(func() {
				key := make([]byte, 8)
				for i := range perWorker {
					binary.BigEndian.PutUint64(key, uint64(w)<<40|uint64(i))
					c.Put(key, []byte{1}, 1)
				}
			})
		}
		c.Close() // races the writers still growing shards
		wg.Wait()
	}
	require.Equal(t, before, cachebudget.Global.Used(), "envelope not restored after Close raced a grow")
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

	c := closeOnCleanup(t, NewGenericCacheWithAvg[[]byte](overflow(uint64(unsafe.Sizeof(entry[[]byte]{}))), 0,
		func(v []byte) int { return len(v) }, ModeEvictLRU))
	require.LessOrEqual(t, c.maxCap, uint32(maxCacheSlots))
	require.Greater(t, c.maxCap, uint32(maxCacheSlots/2))

	g := newGrowLRU[codeSizeEntry](overflow(avgBytesPerEntry+uint64(slotChargeBytes(elemBytesFor[codeSizeEntry]()))), 0, nil)
	t.Cleanup(g.Close)
	require.Equal(t, uint32(maxCacheSlots), g.maxCap)
}

// The reservation counts bytes outside the entry; the usage report counts
// currentSize, which adds bookkeeping living inside it. Folding that into the
// estimate charges it twice.
func TestGenericCache_PayloadEstimateExcludesEntryBookkeeping(t *testing.T) {
	t.Parallel()

	const budget = 1 * datasize.GB
	sizeFunc := func(v []byte) int { return len(v) }

	external := closeOnCleanup(t, NewGenericCacheWithAvg(budget, avgStoragePayloadBytes, sizeFunc, ModeEvictLRU))
	doubled := closeOnCleanup(t, NewGenericCacheWithAvg(budget, avgStoragePayloadBytes+currentSizeEntryOverhead, sizeFunc, ModeEvictLRU))
	require.Equal(t,
		int64(bucketWays)*currentSizeEntryOverhead,
		doubled.bucketBytes()-external.bucketBytes(),
		"charging the in-entry bookkeeping again takes that much extra per bucket from the shared envelope")

	// currentSize carries the in-element bookkeeping the estimate leaves out.
	key := make([]byte, 52)
	val := make([]byte, avgStoragePayloadBytes-len(key))
	external.Put(key, val, 1)
	require.Equal(t, int64(avgStoragePayloadBytes+currentSizeEntryOverhead), external.currentSize.Load())
}

// The ceiling is the largest bucket count per shard whose full cost fits the
// budget: one bucket more on every shard has to be over it.
func TestCeilingLeavesNoAffordableSlot(t *testing.T) {
	t.Parallel()

	for _, budget := range []datasize.ByteSize{16 * datasize.MB, 150 * datasize.MB, 512 * datasize.MB, 1 * datasize.GB} {
		for _, payload := range []uint32{0, 64, avgStoragePayloadBytes, avgAccountPayloadBytes, 12288} {
			c := closeOnCleanup(t, NewGenericCacheWithAvg[[]byte](budget, payload, func(v []byte) int { return len(v) }, ModeEvictLRU))
			require.LessOrEqual(t, c.generationBytes(c.maxBuckets), int64(budget),
				"ceiling of %d buckets per shard for (%s, %d) is outside its own budget", c.maxBuckets, budget, payload)
			if c.maxCap+uint32(len(c.shards))*bucketWays <= maxCacheSlots {
				require.Greater(t, c.generationBytes(c.maxBuckets+1), int64(budget),
					"(%s, %d) stopped at %d buckets per shard with a step still affordable", budget, payload, c.maxBuckets)
			}
		}
	}
}

// The search doubles its seed when the estimate lands under the answer, so a
// ceiling in the top half of uint32 has to be reached in wider arithmetic: the
// doubling wraps to zero otherwise and the walk never ends.
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

// avgBytes counts only what an entry points at, so it cannot also be the
// denominator for a numerator that counts the entry's logical bytes: for a value
// type held inline the two disagree by the whole value.
func TestGenericCache_UsageReportIsNotDrivenByThePayloadEstimate(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	c := closeOnCleanup(t, NewGenericCacheWithAvg[[128]byte](1*datasize.MB, 8,
		func([128]byte) int { return 128 }, ModeEvictLRU))

	key := make([]byte, 32)
	for i := range int(c.maxCap) {
		binary.BigEndian.PutUint64(key, uint64(i))
		c.Put(key, [128]byte{}, 1)
	}

	// How far the fill lands below the allocated slots is hash skew across the
	// shards, so the bound is what to assert, not a figure. The payload estimate
	// as denominator is unbounded above: it has to overstate here, or this cache
	// does not reproduce the case at all.
	payloadPct := float64(c.currentSize.Load()) / float64(int64(c.maxCap)*c.payloadBytes) * 100
	require.Greater(t, payloadPct, 100.0,
		"the payload estimate has to overstate this cache, or the test proves nothing")
	require.LessOrEqual(t, c.slotsPct(), 100.0,
		"a full cache reports %.2f%% against the slots it allocated", c.slotsPct())
	require.Positive(t, c.slotsPct(), "an empty report would pass the bound without measuring anything")
}

// A shard whose grow step the envelope refuses evicts at its current capacity,
// so occupancy has to count the slots allocated, not the ceiling growth aims at.
func TestGenericCache_UsageReportCountsAllocatedSlots(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(0)

	c := closeOnCleanup(t, NewGenericCacheWithAvg[[]byte](1*datasize.GB, 8,
		func(v []byte) int { return len(v) }, ModeEvictLRU))

	allocated := c.allocatedSlots()
	require.Less(t, uint32(allocated), c.maxCap/16, "the ceiling has to stay out of reach")

	key := make([]byte, 32)
	for i := range allocated * 8 {
		binary.BigEndian.PutUint64(key, uint64(i))
		c.Put(key, []byte{1}, 1)
	}

	require.Equal(t, allocated, c.allocatedSlots(), "every grow step is refused at a zero envelope")
	require.InDelta(t, 100.0, c.slotsPct(), 5.0,
		"a cache evicting at its allocated %d slots reports %.2f%%", allocated, c.slotsPct())
}

// cacheBudgets covers the production ceilings and the small budgets the
// exported constructor allows, where the shard count is large against the
// budget and the per-slot estimate has the least room.
var cacheBudgets = []datasize.ByteSize{
	1 * datasize.MB, 16 * datasize.MB, 64 * datasize.MB,
	150 * datasize.MB, 1 * datasize.GB, 4 * datasize.GB,
}

// The slot ceiling is derived from a per-slot estimate that carries neither the
// per-shard structs nor the gap between a fitted table and the 5/4 ratio it is
// charged at. Both scale with the shard count, so the exact cost of the ceiling
// generation has to be checked against the budget across the shard counts a
// host can produce, not at the one GOMAXPROCS the point tests happen to run on.
func TestGenericCache_CeilingFitsItsBudget(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(0)
	prevBudget := cachebudget.Global
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prevProcs)
		cachebudget.Global = prevBudget
	})
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	// growLRU derives its ceiling from the same estimate against its own
	// generation cost, which counts the shard structs separately.
	checkGrow := func(t *testing.T, procs int, payload uint32, budget datasize.ByteSize) {
		t.Helper()
		g := newGrowLRU[codeSizeEntry](budget, payload, nil)
		t.Cleanup(g.Close)
		require.LessOrEqual(t, g.generationBytes(g.maxCap), int64(budget),
			"procs=%d payload=%d budget=%s: a full %d slots costs more than the budget it came from",
			procs, payload, budget, g.maxCap)
	}

	for _, procs := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		runtime.GOMAXPROCS(procs)
		for _, payload := range []uint32{8, avgStoragePayloadBytes, avgAccountPayloadBytes, avgBytesPerEntry, largeCodePayloadBytes} {
			for _, budget := range cacheBudgets {
				// The value type sets the element size, which every term scales with.
				requireCeilingFitsBudget[[]byte](t, procs, payload, budget)
				requireCeilingFitsBudget[[128]byte](t, procs, payload, budget)
				checkGrow(t, procs, payload, budget)
			}
		}
	}
}

// requireCeilingFitsBudget builds the cache the exported constructor builds and
// weighs its ceiling generation, not the intermediate the ceiling is derived
// from: a floor applied after the fit would not show up in the latter.
func requireCeilingFitsBudget[T any](t *testing.T, procs int, payload uint32, budget datasize.ByteSize) {
	t.Helper()
	c := NewGenericCacheWithAvg[T](budget, payload, func(T) int { return 0 }, ModeEvictLRU)
	defer c.Close()
	require.Positive(t, c.maxCap)
	// A budget below the start capacity buys it anyway, so that is the bound
	// there; above it the fitted ceiling has to fit the budget it came from.
	require.LessOrEqual(t, c.generationBytes(c.maxBuckets),
		max(int64(budget), c.generationBytes(c.startBuckets)),
		"procs=%d payload=%d budget=%s: a full %d slots over %d shards costs more than the budget it came from",
		procs, payload, budget, c.maxCap, len(c.shards))
}

// Every reservation path has to be paired: the birth generation, each funded
// grow step and Clear's rebuild all move Global.Used, and a difference applied
// in one direction only strands or over-releases bytes no later Close settles.
func TestGenericCache_BudgetSettlesAcrossTheLifecycle(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(0)
	prevBudget := cachebudget.Global
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prevProcs)
		cachebudget.Global = prevBudget
	})

	key := make([]byte, 32)
	for _, procs := range []int{1, 8, 64} {
		for _, budget := range cacheBudgets {
			cachebudget.Global = cachebudget.New(math.MaxInt64)
			runtime.GOMAXPROCS(procs)
			c := NewGenericCacheWithAvg(budget, avgStoragePayloadBytes,
				func(v []byte) int { return len(v) }, ModeEvictLRU)
			at := func(step string) {
				require.Equal(t, c.reservedBytes.Load(), cachebudget.Global.Used(),
					"procs=%d budget=%s after %s", procs, budget, step)
			}
			at("construction")

			// Enough puts to fund several grow steps on the same shard.
			born := c.allocatedSlots()
			for i := range 32 * int(c.startCap) {
				binary.BigEndian.PutUint64(key, uint64(i))
				c.Put(key, key[:8], 1)
			}
			require.Greater(t, c.allocatedSlots(), born,
				"procs=%d budget=%s: the fill funded no grow step, so the walk weighs nothing", procs, budget)
			at("grow")

			c.Clear()
			at("clear")

			c.Close()
			require.Zero(t, cachebudget.Global.Used(),
				"procs=%d budget=%s: Close left %d B charged", procs, budget, cachebudget.Global.Used())
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

// At the ceiling, summing every per-shard growBytes delta must equal
// generationBytes for the complete generation.
func TestGenericCache_StepAndGenerationCostsAgreeAtTheCeiling(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(0)
	prevBudget := cachebudget.Global
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prevProcs)
		cachebudget.Global = prevBudget
	})

	key := make([]byte, 32)
	for _, procs := range []int{1, 8, 64} {
		runtime.GOMAXPROCS(procs)
		for _, budget := range []datasize.ByteSize{1 * datasize.MB, 16 * datasize.MB} {
			cachebudget.Global = cachebudget.New(math.MaxInt64)
			c := NewGenericCacheWithAvg[[]byte](budget, avgStoragePayloadBytes,
				func(v []byte) int { return len(v) }, ModeEvictLRU)
			// Shards grow independently, so fill until the last one reaches the
			// ceiling. The bound is only there to fail the assertion below rather
			// than spin if a regression stops the climb.
			for i := 0; c.allocatedSlots() < int(c.maxCap) && i < 40*int(c.maxCap); i++ {
				binary.BigEndian.PutUint64(key, uint64(i))
				c.Put(key, key[:8], 1)
			}
			require.Equal(t, int(c.maxCap), c.allocatedSlots(),
				"procs=%d budget=%s: the fill left a shard below the ceiling, so the sums are not comparable",
				procs, budget)
			require.Equal(t, c.generationBytes(c.maxBuckets), c.reservedBytes.Load(),
				"procs=%d budget=%s: the steps charged %d B for a generation costing %d B",
				procs, budget, c.reservedBytes.Load(), c.generationBytes(c.maxBuckets))
			c.Close()
		}
	}
}

// A budget too small to buy the start capacity gets the floored ceiling; the
// shard count has to follow that ceiling, not the pre-floor estimate, or the
// whole cache runs behind one mutex.
func TestFlooredCeilingKeepsShardGranularity(t *testing.T) {
	c := closeOnCleanup(t, NewGenericCacheWithAvg[[]byte](1*datasize.KB, avgStoragePayloadBytes,
		func(v []byte) int { return len(v) }, ModeEvictLRU))
	require.Equal(t, c.startBuckets, c.maxBuckets, "the ceiling has to be the floor")
	require.Equal(t, int(initialShardCount(genericCacheStartCapacity, shardCeil())), len(c.shards))
}

// Writers and readers on one key: a reader must only ever see a whole entry
// some writer stored, so its value and txNum always belong together.
func TestGenericCache_SameKeyConcurrentGetPut(t *testing.T) {
	c := closeOnCleanup(t, NewGenericCache[[]byte](1*datasize.MB, func(v []byte) int { return len(v) }, ModeEvictLRU))
	key := []byte("same-key")
	stop := make(chan struct{})
	var torn atomic.Bool
	var writers, readers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := uint64(w) << 32; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, i)
				c.Put(key, v, i)
			}
		})
	}
	for range 4 {
		readers.Go(func() {
			for range 200_000 {
				if v, txNum, ok := c.GetWithTxNum(key); ok && (len(v) != 8 || binary.BigEndian.Uint64(v) != txNum) {
					torn.Store(true)
				}
			}
		})
	}
	readers.Wait()
	close(stop)
	writers.Wait()
	require.False(t, torn.Load(), "a reader saw a value and txNum from different writes")
	require.Equal(t, 1, c.Len())
}

// A full bucket evicts its own unreferenced entry and leaves other buckets
// alone.
func TestGenericCache_EvictionStaysInsideTheBucket(t *testing.T) {
	c := newGenericCacheEntries(1*datasize.MB, 2*bucketWays, func(v []byte) int { return len(v) }, ModeEvictLRU)
	require.Len(t, c.shards, 1)
	require.Len(t, *c.shards[0].tbl.Load(), 2)

	var full, other [][]byte
	for i := 0; len(full) < bucketWays+1 || len(other) < 3; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if bucketOf(maphash.Hash(k), 2) == 0 {
			full = append(full, k)
		} else {
			other = append(other, k)
		}
	}
	other = other[:3]
	for _, k := range append(full[:bucketWays], other...) {
		c.Put(k, k, 1)
	}
	const cold = 3
	for i, k := range full[:bucketWays] {
		if i != cold {
			_, ok := c.Get(k)
			require.True(t, ok)
		}
	}

	c.Put(full[bucketWays], full[bucketWays], 1)
	require.Equal(t, uint64(1), c.evictions.Load())
	for i, k := range full {
		_, ok := c.Get(k)
		require.Equal(t, i != cold, ok, "key %d", i)
	}
	for i, k := range other {
		_, ok := c.Get(k)
		require.True(t, ok, "the eviction reached key %d of another bucket", i)
	}
	require.Equal(t, bucketWays+3, c.Len())
}

// Below its ceiling a cache grows instead of evicting, however its keys
// cluster in buckets.
func TestGenericCache_GrowsInsteadOfEvicting(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	c := closeOnCleanup(t, NewGenericCacheWithAvg[[]byte](1*datasize.GB, avgStoragePayloadBytes,
		func(v []byte) int { return len(v) }, ModeEvictLRU))
	const keys = 64 * 1024
	key := make([]byte, 52)
	for i := range keys {
		binary.BigEndian.PutUint64(key, uint64(i))
		c.Put(key, key[:8], 1)
	}
	for i := range c.shards {
		require.Less(t, uint32(len(*c.shards[i].tbl.Load())), c.maxBuckets, "shard %d reached the ceiling", i)
	}
	require.Positive(t, grownShards(c))
	require.Zero(t, c.evictions.Load())
	require.Equal(t, keys, c.Len())
	for i := range keys {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, ok := c.Get(key)
		require.True(t, ok, "key %d", i)
	}
}

// Every key length, empty included, has to read back as the key it was stored
// under.
func TestGenericCache_KeyLengths(t *testing.T) {
	c := closeOnCleanup(t, NewGenericCache[int](1*datasize.MB, func(int) int { return 8 }, ModeEvictLRU))
	key := make([]byte, 80)
	for i := range key {
		key[i] = byte(i + 1)
	}
	for n := range len(key) + 1 {
		c.Put(key[:n], n, 1)
	}
	for n := range len(key) + 1 {
		v, ok := c.Get(key[:n])
		require.True(t, ok, "len %d", n)
		require.Equal(t, n, v)
	}
}
