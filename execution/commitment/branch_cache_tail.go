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
	tailStartCapacity = 512
	tailGrowFactor    = 4
	tailEntryBytes    = 512
)

// Writes racing a resize may be dropped (benign cache miss; re-read from DB).
type tailLRU struct {
	cur    atomic.Pointer[freelru.ShardedLRU[uint64, *branchCacheEntry]]
	maxCap uint32

	resizeMu sync.Mutex
	curCap   atomic.Uint32
	reserved int64
}

func newTailLRU(maxCapacity uint32) *tailLRU {
	start := min(uint32(tailStartCapacity), maxCapacity)
	t := &tailLRU{maxCap: maxCapacity}
	t.reserved = int64(start) * tailEntryBytes
	cachebudget.Global.Take(t.reserved)
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
	// Avoid lru.Len() locks once fully grown.
	if curCap := t.curCap.Load(); curCap < t.maxCap && lru.Len() >= int(curCap) {
		t.maybeGrow()
		lru = t.cur.Load()
	}
	lru.Add(key, entry)
}

func (t *tailLRU) maybeGrow() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()

	old := t.cur.Load()
	curCap := t.curCap.Load()
	if curCap >= t.maxCap || old.Len() < int(curCap) {
		return
	}
	newCap := min(curCap*tailGrowFactor, t.maxCap)
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

func (t *tailLRU) reset() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()
	start := min(uint32(tailStartCapacity), t.maxCap)
	cachebudget.Global.Release(t.reserved - int64(start)*tailEntryBytes)
	t.reserved = int64(start) * tailEntryBytes
	t.curCap.Store(start)
	t.cur.Store(newTailShards(start))
}

func (t *tailLRU) Len() int {
	return t.cur.Load().Len()
}

// Call once; BranchCache.Close guards against double-release.
func (t *tailLRU) Close() {
	t.resizeMu.Lock()
	defer t.resizeMu.Unlock()
	cachebudget.Global.Release(t.reserved)
	t.reserved = 0
}
