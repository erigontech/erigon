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
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
)

// BranchCache is a persistent LRU cache for branch node data that survives across
// Process() calls. Unlike WarmupCache (ephemeral, populated by warmup workers),
// BranchCache is populated on-demand during trie reads and invalidated when
// branches are written during fold().
//
// The cache primarily benefits:
// - Post-flush cold starts (warmup cache is cleared, but persistent cache retains root-zone branches)
// - Initial sync (branch data persists across batches within the same SharedDomains lifetime)
//
// Thread safety: the underlying maphash.LRU uses hashicorp/golang-lru which is thread-safe.
// However, BranchCache is typically accessed single-threaded within Process().
type BranchCache struct {
	branches *maphash.LRU[[]byte]
	hits     atomic.Uint64
	misses   atomic.Uint64
}

const DefaultBranchCacheSize = 65536 // ~65k entries, ~13MB at ~200 bytes/entry

// NewBranchCache creates a new persistent branch cache with the given capacity.
func NewBranchCache(capacity int) *BranchCache {
	cache, err := maphash.NewLRU[[]byte](capacity)
	if err != nil {
		panic("BranchCache: " + err.Error())
	}
	return &BranchCache{
		branches: cache,
	}
}

// Get retrieves branch data from the cache. Returns nil, false on miss.
func (c *BranchCache) Get(key []byte) ([]byte, bool) {
	data, found := c.branches.Get(key)
	if found {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return data, found
}

// Put stores branch data in the cache, making a copy to avoid buffer reuse issues.
func (c *BranchCache) Put(key []byte, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.branches.Set(key, dataCopy)
}

// Invalidate removes a branch entry from the cache.
func (c *BranchCache) Invalidate(key []byte) {
	c.branches.Delete(key)
}

// Contains checks if a key exists in the cache without updating recency or counters.
func (c *BranchCache) Contains(key []byte) bool {
	return c.branches.Contains(key)
}

// Len returns the number of entries in the cache.
func (c *BranchCache) Len() int {
	return c.branches.Len()
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

// Clear removes all entries and resets counters.
func (c *BranchCache) Clear() {
	c.branches.Purge()
	c.ResetCounters()
}
