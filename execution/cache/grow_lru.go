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
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/cachebudget"
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
// need GenericCache's fenced swap. Each generation's entry count stays exact
// for that generation — every mutation resolves one generation and decrements
// the one it acted on — which is what the grow gates rely on. The caller's
// onEvict byte counters remain approximate across grow windows: a write lost in
// a retired generation is counted but never evicted, and a removal a racing
// copy undid subtracts twice.
// lruGen is one generation of a sharded LRU plus an O(1) live-entry count.
// freelru's own Len locks every shard, which serialises readers against each
// other on a path that only wanted a number.
type lruGen[V any] struct {
	lru *freelru.ShardedLRU[uint64, V]
	n   atomic.Int64
}

func (g *lruGen[V]) len() int { return int(g.n.Load()) }

// add inserts a key the LRU does not already hold -- Put removes first, so it
// always does -- and the count rises by one; a capacity eviction inside freelru
// fires OnEvict, which takes it back down.
func (g *lruGen[V]) add(h uint64, v V) (evicted bool) {
	evicted = g.lru.Add(h, v)
	g.n.Add(1)
	return evicted
}

type growLRU[V any] struct {
	cur      atomic.Pointer[lruGen[V]]
	onEvict  func(uint64, V)
	avgBytes int64

	startCap uint32
	maxCap   uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
	closed   bool
}

func newGrowLRU[V any](maxBytes datasize.ByteSize, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	maxCap := min(max(uint32(uint64(maxBytes)/uint64(avgBytes)), 1), 1<<24)
	// Start small (bounded by the ceiling); the floor is on the start size, not
	// the ceiling — a tiny configured budget yields a tiny, still-evicting cap.
	start := min(uint32(genericCacheStartCapacity), maxCap)
	g := &growLRU[V]{onEvict: onEvict, avgBytes: int64(avgBytes), startCap: start, maxCap: maxCap}
	g.curCap.Store(start)
	g.reserved = int64(start) * g.avgBytes
	cachebudget.Global.Take(g.reserved)
	g.cur.Store(g.newShards(start))
	return g
}

func (g *growLRU[V]) newShards(capacity uint32) *lruGen[V] {
	lru, err := freelru.NewSharded[uint64, V](capacity, u64identity)
	if err != nil {
		panic(fmt.Sprintf("growLRU: NewSharded(%d): %s", capacity, err))
	}
	gen := &lruGen[V]{lru: lru}
	lru.SetOnEvict(func(k uint64, v V) {
		gen.n.Add(-1)
		if g.onEvict != nil {
			g.onEvict(k, v)
		}
	})
	return gen
}

func (g *growLRU[V]) Get(key uint64) (V, bool) { return g.cur.Load().lru.Get(key) }

// Put stores a key, growing first when the generation is full. Both the removal
// and the store resolve one generation, and the removal is not redundant on a
// caller that just missed: an unfenced grow can copy the key into the next
// generation, a read-path Remove can drop it from the still-current old one, and
// the publish can land between that miss and this load. freelru would then
// replace the copy in place without firing OnEvict, leaving the count and the
// caller's byte counter carrying an entry that is no longer there.
func (g *growLRU[V]) Put(key uint64, value V) {
	gen := g.cur.Load()
	if curCap := g.curCap.Load(); curCap < g.maxCap && gen.len() >= int(curCap) {
		g.maybeGrow()
		gen = g.cur.Load()
	}
	gen.lru.Remove(key)
	gen.add(key, value)
}

func (g *growLRU[V]) maybeGrow() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	old := g.cur.Load()
	curCap := g.curCap.Load()
	if curCap >= g.maxCap || old.len() < int(curCap) {
		return
	}
	newCap := min(curCap*genericCacheGrowFactor, g.maxCap)
	delta := int64(newCap-curCap) * g.avgBytes
	if !cachebudget.Global.Reserve(delta) {
		return
	}
	next := g.newShards(newCap)
	for _, k := range old.lru.Keys() {
		if v, ok := old.lru.Peek(k); ok {
			next.add(k, v)
		}
	}
	g.cur.Store(next)
	g.curCap.Store(newCap)
	g.reserved += delta
}

func (g *growLRU[V]) Remove(key uint64) { g.cur.Load().lru.Remove(key) }
func (g *growLRU[V]) Len() int          { return g.cur.Load().len() }

// Purge empties the LRU and shrinks it back to the start size, returning the
// grown budget to the envelope (it regrows on demand).
func (g *growLRU[V]) Purge() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	cachebudget.Global.Release(g.reserved - int64(g.startCap)*g.avgBytes)
	g.reserved = int64(g.startCap) * g.avgBytes
	g.curCap.Store(g.startCap)
	g.cur.Store(g.newShards(g.startCap))
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
