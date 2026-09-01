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
// need GenericCache's fenced swap. The onEvict-maintained counters are
// approximate across grow windows (a lost write is counted but never
// evicted; a raced removal can subtract twice).
type growLRU[V any] struct {
	cur      atomic.Pointer[freelru.ShardedLRU[uint64, V]]
	onEvict  func(uint64, V)
	avgBytes int64

	// avgBytes only estimates the entry ceiling, so a layer whose entries run
	// larger than the estimate holds several x its configured budget. maxBytes
	// bounds measured residency instead; 0 leaves a layer entry-bounded.
	maxBytes int64
	sizeOf   func(V) int64
	curBytes atomic.Int64

	startCap uint32
	maxCap   uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
	closed   bool
}

func newGrowLRU[V any](maxBytes datasize.ByteSize, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	return newGrowLRUSized(maxBytes, avgBytes, nil, onEvict)
}

// newGrowLRUSized bounds residency by what sizeOf reports rather than by the
// avgBytes estimate. A nil sizeOf keeps the entry-only ceiling.
func newGrowLRUSized[V any](maxBytes datasize.ByteSize, avgBytes uint32, sizeOf func(V) int64, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	maxCap := min(max(uint32(uint64(maxBytes)/uint64(avgBytes)), 1), 1<<24)
	// Start small (bounded by the ceiling); the floor is on the start size, not
	// the ceiling — a tiny configured budget yields a tiny, still-evicting cap.
	start := min(uint32(genericCacheStartCapacity), maxCap)
	g := &growLRU[V]{onEvict: onEvict, avgBytes: int64(avgBytes), startCap: start, maxCap: maxCap}
	if sizeOf != nil {
		g.sizeOf, g.maxBytes = sizeOf, int64(maxBytes)
	}
	g.curCap.Store(start)
	g.reserved = int64(start) * g.avgBytes
	cachebudget.Global.Take(g.reserved)
	g.cur.Store(g.newShards(start))
	return g
}

func (g *growLRU[V]) newShards(capacity uint32) *freelru.ShardedLRU[uint64, V] {
	lru, err := freelru.NewSharded[uint64, V](capacity, u64identity)
	if err != nil {
		panic(fmt.Sprintf("growLRU: NewSharded(%d): %s", capacity, err))
	}
	if g.sizeOf != nil {
		lru.SetOnEvict(func(k uint64, v V) {
			g.curBytes.Add(-g.sizeOf(v))
			if g.onEvict != nil {
				g.onEvict(k, v)
			}
		})
	} else if g.onEvict != nil {
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
	if g.sizeOf != nil {
		// Callers Add only an absent key, so this never double-counts a replace.
		// Across an unfenced grow it can, which is the drift the type documents.
		g.curBytes.Add(g.sizeOf(value))
	}
}

func (g *growLRU[V]) maybeGrow() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	old := g.cur.Load()
	curCap := g.curCap.Load()
	if curCap >= g.maxCap || old.Len() < int(curCap) {
		return
	}
	// Refusing a bigger table at the budget is what bounds residency: entries
	// already admitted are never evicted for size, so the bound is soft by up
	// to one generation's worth of oversized entries.
	if g.maxBytes > 0 && g.curBytes.Load() >= g.maxBytes {
		return
	}
	newCap := min(curCap*genericCacheGrowFactor, g.maxCap)
	if g.maxBytes > 0 {
		// A x4 step taken while still under budget is what overshoots it, so the
		// step is clamped to what the budget affords at the size entries are
		// actually running — the estimate only sizes the first table.
		if n := int64(old.Len()); n > 0 {
			if avg := g.curBytes.Load() / n; avg > 0 {
				newCap = min(newCap, uint32(min(g.maxBytes/avg, int64(g.maxCap))))
			}
		}
		if newCap <= curCap {
			return
		}
	}
	delta := int64(newCap-curCap) * g.avgBytes
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
	cachebudget.Global.Release(g.reserved - int64(g.startCap)*g.avgBytes)
	g.reserved = int64(g.startCap) * g.avgBytes
	g.curCap.Store(g.startCap)
	g.curBytes.Store(0)
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
