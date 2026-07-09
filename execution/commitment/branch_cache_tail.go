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

package commitment

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/cachebudget"
)

const (
	// tailStartCapacity is the entry count a tail is first sized to. It jump-grows
	// (×tailGrowFactor) toward its max only while the shared budget allows, so a
	// cache over a shallow trie stays at the start size.
	tailStartCapacity = 512
	tailGrowFactor    = 4
	// tailEntryBytes is the assumed resident cost of one tail slot (freelru
	// element + the branch payload it points at), used only for budget accounting.
	tailEntryBytes = 512
)

// tailLRU is the BranchCache LRU tail. It wraps a sharded freelru that is
// jump-resized (allocate larger, copy the live entries over) as it fills,
// bounded by the shared cachebudget envelope and a per-cache max. Reads and writes take no
// tail-level lock — they load the current freelru atomically and rely on its own
// per-shard locking; the resize mutex is held only during the rare grow. A write
// racing a resize may land in the freelru about to be replaced and be dropped,
// which is a benign cache miss (the branch is re-read from the authoritative DB).
type tailLRU struct {
	cur    atomic.Pointer[freelru.ShardedLRU[uint64, *branchCacheEntry]]
	maxCap uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
}

func newTailLRU(maxCapacity uint32) *tailLRU {
	start := uint32(tailStartCapacity)
	if start > maxCapacity {
		start = maxCapacity
	}
	t := &tailLRU{maxCap: maxCapacity}
	t.reserved = int64(start) * tailEntryBytes
	cachebudget.Global.Take(t.reserved) // initial slice is small; take it unconditionally
	t.curCap.Store(start)
	t.cur.Store(newTailShards(start))
	return t
}

func newTailShards(capacity uint32) *freelru.ShardedLRU[uint64, *branchCacheEntry] {
	lru, err := freelru.NewShardedWithSize[uint64, *branchCacheEntry](
		branchCacheTailShards, capacity, capacity+capacity/4, u64ident)
	if err != nil {
		panic(fmt.Sprintf("BranchCache tail: NewShardedWithSize(%d): %s", capacity, err))
	}
	return lru
}

func (t *tailLRU) Get(key uint64) (*branchCacheEntry, bool) {
	return t.cur.Load().Get(key)
}

func (t *tailLRU) Add(key uint64, entry *branchCacheEntry) {
	lru := t.cur.Load()
	// curCap < maxCap first: a fully-grown tail can never grow, so it must not
	// pay lru.Len()'s per-shard locks on every insert.
	if curCap := t.curCap.Load(); curCap < t.maxCap && lru.Len() >= int(curCap) {
		t.maybeGrow()
		lru = t.cur.Load()
	}
	lru.Add(key, entry)
}

// maybeGrow jump-resizes the tail one step larger when it is full, the per-cache
// max hasn't been reached, and the shared budget has room. Otherwise the tail
// keeps its size and freelru evicts LRU on the next insert.
func (t *tailLRU) maybeGrow() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()

	old := t.cur.Load()
	curCap := t.curCap.Load()
	if curCap >= t.maxCap || old.Len() < int(curCap) {
		return
	}
	newCap := curCap * tailGrowFactor
	if newCap > t.maxCap {
		newCap = t.maxCap
	}
	delta := int64(newCap-curCap) * tailEntryBytes
	if !cachebudget.Global.Reserve(delta) {
		return
	}
	next := newTailShards(newCap)
	for _, k := range old.Keys() {
		if v, ok := old.Get(k); ok {
			next.Add(k, v)
		}
	}
	t.cur.Store(next)
	t.curCap.Store(newCap)
	t.reserved += delta
}

func (t *tailLRU) Remove(key uint64) {
	t.cur.Load().Remove(key)
}

// reset shrinks the tail back to the start size and returns its budget, keeping
// the cache adaptive across unwind/clear (it regrows on demand afterwards).
func (t *tailLRU) reset() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()
	start := uint32(tailStartCapacity)
	if start > t.maxCap {
		start = t.maxCap
	}
	cachebudget.Global.Release(t.reserved - int64(start)*tailEntryBytes)
	t.reserved = int64(start) * tailEntryBytes
	t.curCap.Store(start)
	t.cur.Store(newTailShards(start))
}

func (t *tailLRU) Len() int {
	return t.cur.Load().Len()
}

// Close returns the tail's envelope reservation. Call once (BranchCache.Close
// guards against double-release with its closed CAS).
func (t *tailLRU) Close() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()
	cachebudget.Global.Release(t.reserved)
	t.reserved = 0
}
