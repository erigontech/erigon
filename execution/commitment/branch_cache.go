// Copyright 2024 The Erigon Authors
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
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
)

// BranchCache is a two-tier persistent cache for branch node data that survives
// across Process() calls. Unlike WarmupCache (ephemeral, populated by warmup workers),
// BranchCache is populated on-demand during trie reads and invalidated when
// branches are written during fold().
//
// Tier 1 (pinned): Fixed-size arrays for root-zone branches, indexed directly
// from compact key bytes. These entries are never evicted — the root zone is
// accessed on every commitment.
//
//	t0          — depth 0 (root node, compact key [0x00])
//	t1[16]      — depth 1 (compact key [0x1N], N = nibble)
//	t2[256]     — depth 2 (compact key [0x00, byte])
//	t3[4096]    — depth 3 (compact key [0x1N, byte])
//	t4[65536]   — depth 4 (compact key [0x00, byte, byte])
//
// Tier 2 (LRU): Bounded cache for deeper branches, evicts least-recently-used.
//
// Thread safety: pinned arrays protected by RWMutex (low contention — mostly reads).
// LRU tier uses hashicorp/golang-lru which is internally thread-safe.
// Note on struct layout: hits/misses atomics share a cache line with the LRU
// pointer and mu. In theory this could cause false sharing under heavy
// contention, but in practice the pinned tier (hot path) is RWMutex-guarded
// and the atomics are simple counters — the overhead is negligible compared
// to the DB reads this cache avoids. Not worth a cache-line pad for now.
type BranchCache struct {
	t0 []byte
	t1 [16][]byte
	t2 [256][]byte
	t3 [4096][]byte
	t4 [65536][]byte
	mu sync.RWMutex //nolint:gocritic

	// dirty tracks keys that the main trie has Invalidated (modified) but not
	// yet written via Put. Background warmup goroutines check this set via
	// PutIfClean to avoid overwriting authoritative trie data with stale reads.
	dirty map[string]struct{}

	branches *maphash.LRU[[]byte] // LRU for deeper branches
	hits     atomic.Uint64
	misses   atomic.Uint64
}

const DefaultBranchCacheSize = 65536 // ~65k entries for LRU tier

// NewBranchCache creates a new two-tier persistent branch cache.
// lruCapacity controls the LRU tier size; the pinned tier is fixed arrays.
func NewBranchCache(lruCapacity int) *BranchCache {
	cache, err := maphash.NewLRU[[]byte](lruCapacity)
	if err != nil {
		panic("BranchCache: " + err.Error())
	}
	return &BranchCache{
		dirty:    make(map[string]struct{}),
		branches: cache,
	}
}

// CompactKeyIsPinned returns true if the compact key addresses a pinned tier slot (depth 0-4).
// 3-byte odd keys (depth 5) are NOT pinned — they go to LRU.
//
// Tier index computation from compact key bytes:
//
//	t0:         len=1, even  → root
//	t1[N]:      len=1, odd   → key[0]&0x0f
//	t2[I]:      len=2, even  → key[1]
//	t3[I]:      len=2, odd   → uint16(key[0]&0x0f)<<8 | uint16(key[1])
//	t4[I]:      len=3, even  → uint16(key[1])<<8 | uint16(key[2])
//
// Kept in sync with db/state branchCache on add_execution_context_with_caches branch.
func CompactKeyIsPinned(key []byte) bool {
	switch len(key) {
	case 1, 2:
		return true
	case 3:
		return key[0]&0x10 == 0 // even = depth 4 (pinned), odd = depth 5 (LRU)
	}
	return false
}

// Get retrieves a copy of branch data from the cache. Returns nil, false on miss.
// A copy is returned so callers may not modify the cached entry.
// Compact key format: byte[0] bit 4 (0x10) = odd nibble count, bits 0-3 = first nibble.
func (c *BranchCache) Get(key []byte) ([]byte, bool) {
	var data []byte
	var found bool

	if CompactKeyIsPinned(key) {
		c.mu.RLock()
		data, found = c.getPinned(key)
		c.mu.RUnlock()
	} else {
		data, found = c.branches.Get(key)
	}

	if found {
		c.hits.Add(1)
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp, true
	}
	c.misses.Add(1)
	return nil, false
}

// getPinned returns data from the pinned tier arrays. Caller must hold mu.RLock.
// Only call for keys where CompactKeyIsPinned() returns true.
func (c *BranchCache) getPinned(key []byte) ([]byte, bool) {
	switch len(key) {
	case 1:
		if key[0]&0x10 == 0 {
			return c.t0, c.t0 != nil
		}
		data := c.t1[key[0]&0x0f]
		return data, data != nil
	case 2:
		if key[0]&0x10 == 0 {
			data := c.t2[key[1]]
			return data, data != nil
		}
		idx := uint16(key[0]&0x0f)<<8 | uint16(key[1])
		data := c.t3[idx]
		return data, data != nil
	case 3:
		idx := uint16(key[1])<<8 | uint16(key[2])
		data := c.t4[idx]
		return data, data != nil
	}
	return nil, false
}

// Put stores branch data in the cache, making a copy to avoid buffer reuse issues.
// Put always writes (authoritative data from main trie or DB reads) and clears
// the dirty flag for the key.
func (c *BranchCache) Put(key []byte, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	if CompactKeyIsPinned(key) {
		c.mu.Lock()
		c.setPinned(key, dataCopy)
		delete(c.dirty, string(key))
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	delete(c.dirty, string(key))
	c.mu.Unlock()
	c.branches.Set(key, dataCopy)
}

// PutIfClean stores branch data only if the key has not been marked dirty by
// Invalidate. This is used by background warmup goroutines and prefetchers
// to avoid overwriting authoritative data that the main trie has written or
// is about to write.
func (c *BranchCache) PutIfClean(key []byte, data []byte) {
	c.mu.RLock()
	_, isDirty := c.dirty[string(key)]
	c.mu.RUnlock()
	if isDirty {
		return
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	if CompactKeyIsPinned(key) {
		c.mu.Lock()
		// Re-check under write lock to avoid TOCTOU race.
		if _, isDirty = c.dirty[string(key)]; !isDirty {
			c.setPinned(key, dataCopy)
		}
		c.mu.Unlock()
		return
	}

	// For LRU tier, the RLock check above is sufficient: if the key became
	// dirty between the check and this Set, the next authoritative Put will
	// overwrite with correct data. The window is tiny and the consequence
	// is a single stale cache entry that gets corrected immediately.
	c.branches.Set(key, dataCopy)
}

// setPinned stores data in the pinned tier. Caller must hold mu.Lock.
// Only call for keys where CompactKeyIsPinned() returns true.
func (c *BranchCache) setPinned(key []byte, data []byte) {
	switch len(key) {
	case 1:
		if key[0]&0x10 == 0 {
			c.t0 = data
		} else {
			c.t1[key[0]&0x0f] = data
		}
	case 2:
		if key[0]&0x10 == 0 {
			c.t2[key[1]] = data
		} else {
			c.t3[uint16(key[0]&0x0f)<<8|uint16(key[1])] = data
		}
	case 3:
		c.t4[uint16(key[1])<<8|uint16(key[2])] = data
	}
}

// Invalidate removes a branch entry from the cache and marks the key as dirty.
// The dirty flag prevents background warmup goroutines from writing stale data
// back via PutIfClean. The flag is cleared when the main trie writes
// authoritative data via Put, or when Clear is called.
func (c *BranchCache) Invalidate(key []byte) {
	if CompactKeyIsPinned(key) {
		c.mu.Lock()
		c.setPinned(key, nil)
		c.dirty[string(key)] = struct{}{}
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	c.dirty[string(key)] = struct{}{}
	c.mu.Unlock()
	c.branches.Delete(key)
}

// Contains checks if a key exists in the cache without updating recency or counters.
func (c *BranchCache) Contains(key []byte) bool {
	if CompactKeyIsPinned(key) {
		c.mu.RLock()
		_, found := c.getPinned(key)
		c.mu.RUnlock()
		return found
	}

	return c.branches.Contains(key)
}

// Len returns the number of entries in the cache (pinned + LRU).
func (c *BranchCache) Len() int {
	return c.PinnedLen() + c.branches.Len()
}

// PinnedLen returns the number of populated entries in the pinned tier.
func (c *BranchCache) PinnedLen() int {
	count := 0
	c.mu.RLock()
	if c.t0 != nil {
		count++
	}
	for i := range c.t1 {
		if c.t1[i] != nil {
			count++
		}
	}
	for i := range c.t2 {
		if c.t2[i] != nil {
			count++
		}
	}
	for i := range c.t3 {
		if c.t3[i] != nil {
			count++
		}
	}
	for i := range c.t4 {
		if c.t4[i] != nil {
			count++
		}
	}
	c.mu.RUnlock()
	return count
}

// Hits returns the number of cache hits since last reset.
func (c *BranchCache) Hits() uint64 {
	return c.hits.Load()
}

// Misses returns the number of cache misses since last reset.
func (c *BranchCache) Misses() uint64 {
	return c.misses.Load()
}

// ResetCounters resets hit/miss counters.
func (c *BranchCache) ResetCounters() {
	c.hits.Store(0)
	c.misses.Store(0)
}

// Clear removes all entries, resets dirty flags, and resets counters.
func (c *BranchCache) Clear() {
	c.mu.Lock()
	c.t0 = nil
	for i := range c.t1 {
		c.t1[i] = nil
	}
	for i := range c.t2 {
		c.t2[i] = nil
	}
	for i := range c.t3 {
		c.t3[i] = nil
	}
	for i := range c.t4 {
		c.t4[i] = nil
	}
	clear(c.dirty)
	c.mu.Unlock()

	c.branches.Purge()
	c.ResetCounters()
}
