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

package cache

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

const (
	// DefaultGenericCacheSize is the number of entries
	// 2^20 = 1048576 entries or 110 MB in the worst case (100 bytes per entry is 110 MB)
	DefaultGenericCacheSize = 1 << 20
)

// GenericCache is a bounded concurrent cache for key-value data.
// Stores key → value (serialized []byte).
// Used for accounts and storage. Must be cleared on reorg.
// Thread-safe via maphash.Map (sync.Map internally).
type GenericCache struct {
	data      *maphash.Map[[]byte] // key → value, concurrent
	capacity  int                  // max entries
	blockHash common.Hash          // hash of the last block processed
	mu        sync.RWMutex

	// Stats counters (atomic for concurrent access)
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewGenericCache creates a new GenericCache with the specified size.
func NewGenericCache(size int) *GenericCache {
	return &GenericCache{
		data:     maphash.NewMap[[]byte](),
		capacity: size,
	}
}

// NewDefaultGenericCache creates a new GenericCache with the default size.
func NewDefaultGenericCache() *GenericCache {
	return NewGenericCache(DefaultGenericCacheSize)
}

// Get retrieves data for the given key.
// Returns the data and true if found, nil and false otherwise.
func (c *GenericCache) Get(key []byte) ([]byte, bool) {
	value, ok := c.data.Get(key)
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return value, true
}

// Put stores data for the given key.
// Reuses existing slice if capacity is sufficient.
func (c *GenericCache) Put(key []byte, value []byte) {
	// Check if we need to skip due to capacity
	if c.data.Len() >= c.capacity {
		return
	}

	// Try to reuse existing slice if it has sufficient capacity
	if existing, ok := c.data.Get(key); ok && cap(existing) >= len(value) {
		existing = existing[:len(value)]
		copy(existing, value)
		c.data.Set(key, existing)
		return
	}

	c.data.Set(key, common.Copy(value))
}

// Delete removes the data for the given key.
func (c *GenericCache) Delete(key []byte) {
	c.data.Delete(key)
}

// Clear removes all entries from the cache.
func (c *GenericCache) Clear() {
	c.data.Clear()
}

// GetBlockHash returns the hash of the last block processed by the cache.
func (c *GenericCache) GetBlockHash() common.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blockHash
}

// SetBlockHash sets the hash of the current block being processed.
func (c *GenericCache) SetBlockHash(hash common.Hash) {
	c.mu.Lock()
	c.blockHash = hash
	c.mu.Unlock()
}

// ValidateAndPrepare checks if the given parentHash matches the cache's current blockHash.
// If there's a mismatch (indicating non-sequential block processing), the cache is cleared.
// Returns true if the cache was valid (hashes matched or cache was empty), false if cleared.
func (c *GenericCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Empty blockHash means cache hasn't processed any block yet - always valid
	if c.blockHash == (common.Hash{}) {
		c.data.Clear()
		c.blockHash = incomingBlockHash
		return true
	}

	// Check if we're continuing from the expected block
	if c.blockHash == parentHash {
		c.blockHash = incomingBlockHash
		return true
	}

	// Mismatch - clear all mappings (they may be stale)
	c.data.Clear()
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the cache and sets the block hash.
// Used during unwind to reset the cache to a known state.
func (c *GenericCache) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.Clear()
	c.blockHash = hash
}

// Len returns the number of entries in the cache.
func (c *GenericCache) Len() int {
	return c.data.Len()
}

// PrintStatsAndReset prints cache statistics and resets counters.
// Call this at the end of each block to see per-block performance.
func (c *GenericCache) PrintStatsAndReset(name string) {
	hits := c.hits.Swap(0)
	misses := c.misses.Swap(0)

	total := hits + misses

	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	log.Debug(name+" cache stats",
		"hits", hits,
		"misses", misses,
		"hit_rate", hitRate,
		"cache_size", c.data.Len(),
	)
}
