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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/maphash"
)

// grownShards counts the shards that have grown past their birth size, so a
// test can assert the grow it exercises actually happened.
func grownShards[T any](c *GenericCache[T]) int {
	g := c.data.Load()
	n := 0
	for i := range g.shards {
		g.mus[i].Lock()
		if g.curCap[i] > g.startCapPerShard {
			n++
		}
		g.mus[i].Unlock()
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
		wg.Go(func() { c.Put(a, v, 2) })
		wg.Go(func() { c.Put(b, v, 1) })
		wg.Wait()
		if _, ok := c.Get(b); ok {
			t.Fatalf("round %d: ModeNoOp admitted a key past a full budget (SizeBytes=%d, capacityB=%d)",
				round, c.SizeBytes(), c.CapacityBytes())
		}
	}
}

// A shard grow must migrate every entry of that shard. The rebuild copies
// Keys() oldest-first into a strictly larger table, so nothing can be evicted
// on the way; shard count is fixed for the life of the cache, so a grow can
// never re-shard and drop keys clustered on the selection bits.
func TestGenericCache_GrowMigrationLossless(t *testing.T) {
	c := NewGenericCacheWithAvg[[]byte](4*datasize.MB, 256, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	gen := c.data.Load()

	const target = 3
	shardCap := func() uint32 {
		gen.mus[target].Lock()
		defer gen.mus[target].Unlock()
		return gen.curCap[target]
	}

	// Keys that select the target shard; one more than it can hold, so the last
	// insert is what forces the grow.
	var keys [][]byte
	want := int(shardCap()) + 1
	for i := 0; len(keys) < want; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if gen.idx(maphash.Hash(k)) == target {
			keys = append(keys, k)
		}
	}

	before := shardCap()
	for _, k := range keys {
		c.Put(k, []byte("fresh"), 10)
	}
	require.Greater(t, shardCap(), before, "shard did not grow")

	for i, k := range keys {
		v, ok := c.Get(k)
		require.True(t, ok, "key %d lost in the shard migration", i)
		require.Equal(t, []byte("fresh"), v)
	}
}

// A capacity eviction is a size-subtracting writer the put stripes cannot
// serialize: the victim is picked per shard (hash bits 16+), so an insert
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
		wg.Go(func() { c.Put(a, v, 10) }) // insert → evicts b (cap 1)
		wg.Go(func() { c.Put(b, v, 20) }) // same-key update path
		wg.Wait()
	}
	c.Delete(a)
	c.Delete(b)
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

// Steady-state throughput with many more goroutines than cores. Accesses are a
// hot/cold mix -- a hot set about the size of the ceiling plus a long cold tail
// -- and every miss fills read-through, so eviction runs continuously and the
// measured hit rate lands near what the storage domain shows in production.
// Both the hit and the miss path are therefore exercised in realistic
// proportion; a benchmark that only hits, or only misses, says nothing here.
func BenchmarkGenericCacheParallelMixed(b *testing.B) {
	const (
		hotKeys  = 1 << 20
		coldKeys = 10 << 20
		samples  = 8 << 20
		hotPct   = 79
	)
	rnd := rand.New(rand.NewSource(1))
	keys := make([]uint64, samples)
	for i := range keys {
		if rnd.Intn(100) < hotPct {
			keys[i] = uint64(rnd.Intn(hotKeys)) * 0x9E3779B97F4A7C15
		} else {
			keys[i] = uint64(hotKeys+rnd.Intn(coldKeys)) * 0x9E3779B97F4A7C15
		}
	}

	c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88,
		func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	val := make([]byte, 32)

	k := make([]byte, 8)
	for _, key := range keys {
		binary.BigEndian.PutUint64(k, key)
		if _, ok := c.Get(k); !ok {
			c.Put(k, val, 1)
		}
	}

	hits0, misses0 := c.hits.Load(), c.misses.Load()
	var stream atomic.Uint64
	b.SetParallelism(64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine enters the shared stream at its own offset, so shard
		// contention comes from hashing rather than from a shared cursor.
		i := int(stream.Add(1)) * 7919
		k := make([]byte, 8)
		for pb.Next() {
			binary.BigEndian.PutUint64(k, keys[i&(samples-1)])
			i++
			if _, ok := c.Get(k); !ok {
				c.Put(k, val, 1)
			}
		}
	})
	b.StopTimer()
	hits, misses := c.hits.Load()-hits0, c.misses.Load()-misses0
	b.ReportMetric(100*float64(hits)/float64(hits+misses), "hit%")
}

// Filling a cache from cold to a large working set. On main this pays whole-
// cache migration copies; here each shard migrates on its own. Uses only the
// public put path, so it runs unchanged on both.
func BenchmarkGenericCacheFill(b *testing.B) {
	const keys = 1 << 20
	val := make([]byte, 32)
	for b.Loop() {
		c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88, func(v []byte) int { return len(v) }, ModeEvictLRU)
		k := make([]byte, 8)
		for i := range uint64(keys) {
			binary.BigEndian.PutUint64(k, i*0x9E3779B97F4A7C15)
			c.Put(k, val, 1)
		}
		c.Close()
	}
}

// The longest a single put is held up while filling a cache from cold: on main
// that is a whole-cache migration behind every put stripe, here it is one
// shard's migration behind that shard's mutex.
func BenchmarkGenericCacheWorstPut(b *testing.B) {
	const keys = 1 << 20
	val := make([]byte, 32)
	var worst time.Duration
	for b.Loop() {
		c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88, func(v []byte) int { return len(v) }, ModeEvictLRU)
		k := make([]byte, 8)
		for i := range uint64(keys) {
			binary.BigEndian.PutUint64(k, i*0x9E3779B97F4A7C15)
			start := time.Now()
			c.Put(k, val, 1)
			if d := time.Since(start); d > worst {
				worst = d
			}
		}
		c.Close()
	}
	b.ReportMetric(float64(worst.Microseconds()), "worst-put-us")
}

// The entry count is derived from each shard's own length under that shard's
// lock, so an insert/evict mix can neither drive it negative nor let it drift
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

	g := c.data.Load()
	sum := 0
	for i := range g.shards {
		sum += g.shards[i].Len()
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
