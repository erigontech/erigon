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
// A write racing a resize may land in the LRU about to be replaced and be
// dropped; that is a benign cache miss (the value is re-read from the DB).
type growLRU[V any] struct {
	cur      atomic.Pointer[freelru.ShardedLRU[uint64, V]]
	onEvict  func(uint64, V)
	avgBytes int64

	startCap uint32
	maxCap   uint32

	resizeMu sync.Mutex
	curCap   uint32
	reserved int64
	closed   bool
}

func newGrowLRU[V any](maxBytes datasize.ByteSize, avgBytes uint32, onEvict func(uint64, V)) *growLRU[V] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	maxCap := uint32(uint64(maxBytes) / uint64(avgBytes))
	if maxCap < 1 {
		maxCap = 1
	}
	if maxCap > 1<<24 {
		maxCap = 1 << 24
	}
	// Start small (bounded by the ceiling); the floor is on the start size, not
	// the ceiling — a tiny configured budget yields a tiny, still-evicting cap.
	start := uint32(genericCacheStartCapacity)
	if start > maxCap {
		start = maxCap
	}
	g := &growLRU[V]{onEvict: onEvict, avgBytes: int64(avgBytes), startCap: start, maxCap: maxCap, curCap: start}
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
	if g.onEvict != nil {
		lru.SetOnEvict(g.onEvict)
	}
	return lru
}

func (g *growLRU[V]) Get(key uint64) (V, bool) { return g.cur.Load().Get(key) }

func (g *growLRU[V]) Add(key uint64, value V) {
	lru := g.cur.Load()
	if lru.Len() >= int(g.curCap) && g.curCap < g.maxCap {
		g.maybeGrow()
		lru = g.cur.Load()
	}
	lru.Add(key, value)
}

func (g *growLRU[V]) maybeGrow() {
	g.resizeMu.Lock()
	defer g.resizeMu.Unlock()
	old := g.cur.Load()
	if g.curCap >= g.maxCap || old.Len() < int(g.curCap) {
		return
	}
	newCap := g.curCap * genericCacheGrowFactor
	if newCap > g.maxCap {
		newCap = g.maxCap
	}
	delta := int64(newCap-g.curCap) * g.avgBytes
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
	g.curCap = newCap
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
	g.curCap = g.startCap
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
