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
	cur      atomic.Pointer[freelru.ShardedLRU[uint64, V]]
	onEvict  func(uint64, V)
	avgBytes int64

	startCap uint32
	maxCap   uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
	closed   bool
}

// newGrowLRUEntries builds a growLRU from an entry ceiling rather than a byte
// budget, for layers whose contract is the entry count. The envelope charge
// follows freelru.NewSharded's real geometry: it rounds capacity*5/4 up to a
// power of two for the whole cache, so the overhead per slot is that table over
// the capacity, not a constant.
func newGrowLRUEntries[V any](maxEntries, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	return newGrowLRUWith(max(min(maxEntries, maxCacheSlots), 1), int64(avgBytes), onEvict)
}

func newGrowLRU[V any](maxBytes datasize.ByteSize, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	perSlot := int64(avgBytes) + freelruSlotBytes
	maxCap := max(fitTableSlots(min(uint32(uint64(maxBytes)/uint64(perSlot)), maxCacheSlots)), 1)
	return newGrowLRUWith(maxCap, int64(avgBytes), onEvict)
}

// growLRUBytes is what a generation costs. freelru.NewSharded rounds capacity
// plus 25% up to a power of two once for the whole cache, so the table is 2x the
// capacity at a power-of-two generation and 5/4 only on the fitted boundary.
func growLRUBytes(capacity uint32, payloadBytes int64) int64 {
	if capacity == 0 {
		return 0
	}
	table := math.NextPowerOfTwo(uint64(capacity) * 5 / 4)
	return int64(capacity)*payloadBytes + int64(table)*freelruElemBytes
}

func newGrowLRUWith[V any](maxCap uint32, payloadBytes int64, onEvict func(uint64, V)) *growLRU[V] {
	// Start small (bounded by the ceiling); the floor is on the start size, not
	// the ceiling -- a tiny configured budget yields a tiny, still-evicting cap.
	start := min(uint32(genericCacheStartCapacity), maxCap)
	g := &growLRU[V]{onEvict: onEvict, avgBytes: payloadBytes, startCap: start, maxCap: maxCap}
	g.curCap.Store(start)
	g.reserved = growLRUBytes(start, g.avgBytes)
	cachebudget.Global.Take(g.reserved)
	g.cur.Store(g.newShards(start))
	return g
}

func (g *growLRU[V]) newShards(capacity uint32) *freelru.ShardedLRU[uint64, V] {
	lru, err := freelru.NewSharded[uint64, V](capacity, u64identity)
	if err != nil {
		panic(fmt.Sprintf("growLRU: NewSharded(%d): %s", capacity, err))
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
	if curCap >= g.maxCap || old.Len() < int(curCap) {
		return
	}
	newCap := min(curCap*genericCacheGrowFactor, g.maxCap)
	delta := growLRUBytes(newCap, g.avgBytes) - growLRUBytes(curCap, g.avgBytes)
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
	start := growLRUBytes(g.startCap, g.avgBytes)
	cachebudget.Global.Release(g.reserved - start)
	g.reserved = start
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
