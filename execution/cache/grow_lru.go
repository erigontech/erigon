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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/math"
)

// growLRU is a uint64-keyed sharded LRU that starts small and jump-resizes ×4
// toward a byte-budget ceiling as it fills, funding each step from the shared
// cachebudget envelope. It exists so a cache with a small working set never
// pre-commits its full configured capacity — the same demand-growth the state
// caches use — reused across the CodeCache's content and size layers.
//
// Generation swaps (maybeGrow, Purge) are not fenced against writers — safe
// only for content-addressed layers, where a key's payload never changes: a
// write lost in a retired generation is a benign miss, and an entry whose
// removal a racing copy undid serves correct bytes until its stale stamp
// drops it on the next read. Do not reuse for mutable-per-key values — those
// need GenericCache's fenced swap. The onEvict-maintained counters are
// approximate across grow windows (a lost write is counted but never
// evicted; a raced removal can subtract twice).
type growLRU[V any] struct {
	cur       atomic.Pointer[freelru.ShardedLRU[uint64, V]]
	onEvict   func(uint64, V)
	avgBytes  int64
	elemBytes int64

	startCap uint32
	maxCap   uint32
	// procs is the GOMAXPROCS the ceiling was fitted against, sampled once:
	// GOMAXPROCS can change under a running process, and a generation sized by a
	// different shard count than the fit used would leave the cache outside its
	// byte budget.
	procs uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
	closed   bool
}

// newGrowLRUEntries builds a growLRU from an entry ceiling rather than a byte
// budget, for layers whose contract is the entry count. avgBytes is the payload
// held outside the freelru element; a layer with an inline value passes 0.
func newGrowLRUEntries[V any](maxEntries, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	return newGrowLRUWith(max(min(maxEntries, maxCacheSlots), 1), int64(avgBytes),
		uint32(runtime.GOMAXPROCS(0)), onEvict)
}

func newGrowLRU[V any](maxBytes datasize.ByteSize, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	procs := uint32(runtime.GOMAXPROCS(0))
	elemBytes := elemBytesFor[V]()
	perSlot := int64(avgBytes) + slotChargeBytes(elemBytes)
	approx := max(fitTableSlots(uint32(min(uint64(maxBytes)/uint64(perSlot), maxCacheSlots))), 1)
	maxCap := fitCeiling(approx, maxBytes, func(c uint32) int64 {
		return growLRUBytes(c, procs, int64(avgBytes), elemBytes)
	})
	return newGrowLRUWith(maxCap, int64(avgBytes), procs, onEvict)
}

// growLRUGeneration is the table size and shard count freelru builds a capacity
// with: capacity plus 25% rounded up to a power of two once for the whole cache,
// then the procs-derived shard count dropped until it fits that table.
func growLRUGeneration(capacity, procs uint32) (table uint64, shards uint32) {
	table = math.NextPowerOfTwo(uint64(capacity) * 5 / 4)
	shards = uint32(math.NextPowerOfTwo(uint64(procs) * 16))
	for uint64(shards) > table/16 {
		shards /= 16
	}
	return table, max(shards, 1)
}

// growLRUBytes is what a generation of this capacity costs at this shard
// geometry. The table is 2x at a power-of-two capacity and 5/4 only on the
// fitted boundary. The per-shard structs are charged separately: a value
// filling the freelru element leaves the table charge no slack to absorb them.
func growLRUBytes(capacity, procs uint32, payloadBytes, elemBytes int64) int64 {
	if capacity == 0 {
		return 0
	}
	table, shards := growLRUGeneration(capacity, procs)
	return int64(capacity)*payloadBytes + int64(table)*elemBytes +
		int64(shards)*shardChargeBytes(elemBytes)
}

func (g *growLRU[V]) generationBytes(capacity uint32) int64 {
	return growLRUBytes(capacity, g.procs, g.avgBytes, g.elemBytes)
}

func newGrowLRUWith[V any](maxCap uint32, payloadBytes int64, procs uint32, onEvict func(uint64, V)) *growLRU[V] {
	// Start small (bounded by the ceiling); the floor is on the start size, not
	// the ceiling -- a tiny configured budget yields a tiny, still-evicting cap.
	start := min(uint32(genericCacheStartCapacity), maxCap)
	g := &growLRU[V]{
		onEvict:   onEvict,
		avgBytes:  payloadBytes,
		elemBytes: elemBytesFor[V](),
		startCap:  start,
		maxCap:    maxCap,
		procs:     max(procs, 1),
	}
	g.curCap.Store(start)
	g.reserved = g.generationBytes(start)
	cachebudget.Global.Take(g.reserved)
	g.cur.Store(g.newShards(start))
	return g
}

// newShards builds a generation at the pinned geometry, so the shard count the
// reservation was computed from is the one freelru allocates.
func (g *growLRU[V]) newShards(capacity uint32) *freelru.ShardedLRU[uint64, V] {
	table, shards := growLRUGeneration(capacity, g.procs)
	lru, err := freelru.NewShardedWithSize[uint64, V](shards, capacity, uint32(table), u64identity)
	if err != nil {
		panic(fmt.Sprintf("growLRU: NewShardedWithSize(%d, %d): %s", shards, capacity, err))
	}
	if g.onEvict != nil {
		lru.SetOnEvict(g.onEvict)
	}
	return lru
}

func (g *growLRU[V]) Get(key uint64) (V, bool) { return g.cur.Load().Get(key) }

func (g *growLRU[V]) Add(key uint64, value V) {
	lru := g.cur.Load()
	if curCap := g.curCap.Load(); curCap < g.maxCap && lru.Len() >= int(curCap) {
		g.maybeGrow()
		lru = g.cur.Load()
	}
	lru.Add(key, value)
}

func (g *growLRU[V]) maybeGrow() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	old := g.cur.Load()
	curCap := g.curCap.Load()
	// A step funded after Close would hold envelope bytes nothing releases.
	if g.closed || curCap >= g.maxCap || old.Len() < int(curCap) {
		return
	}
	newCap := min(curCap*genericCacheGrowFactor, g.maxCap)
	delta := g.generationBytes(newCap) - g.generationBytes(curCap)
	if !cachebudget.Global.Reserve(delta) {
		return
	}
	next := g.newShards(newCap)
	for _, k := range old.Keys() {
		if v, ok := old.Get(k); ok {
			next.Add(k, v)
		}
	}
	g.cur.Store(next)
	g.curCap.Store(newCap)
	g.reserved += delta
}

func (g *growLRU[V]) Remove(key uint64) { g.cur.Load().Remove(key) }
func (g *growLRU[V]) Len() int          { return g.cur.Load().Len() }

// Purge empties the LRU and shrinks it back to the start size, returning the
// grown budget to the envelope (it regrows on demand).
func (g *growLRU[V]) Purge() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	g.curCap.Store(g.startCap)
	g.cur.Store(g.newShards(g.startCap))
	// Close settles the reservation once; charging the rebuild after that would
	// hold envelope bytes nothing releases.
	if g.closed {
		return
	}
	start := g.generationBytes(g.startCap)
	cachebudget.Global.Release(g.reserved - start)
	g.reserved = start
}

// Close returns this LRU's envelope reservation. Idempotent.
func (g *growLRU[V]) Close() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	cachebudget.Global.Release(g.reserved)
	g.reserved = 0
}
