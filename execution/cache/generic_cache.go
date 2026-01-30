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

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

const (
	// DefaultAccountCacheBytes is the byte limit for account cache (256 MB)
	DefaultAccountCacheBytes = 256 * datasize.MB
	// DefaultStorageCacheBytes is the byte limit for storage cache (512 MB)
	DefaultStorageCacheBytes = 512 * datasize.MB
)

// GenericCache is a bounded concurrent cache for key-value data.
// Stores key → value (serialized []byte).
// Used for accounts and storage. Must be cleared on reorg.
// Thread-safe via maphash.Map (sync.Map internally).
//
// Capacity is byte-based. Once full, new puts are no-ops but
// modifications to existing entries and deletions are still allowed.
type GenericCache struct {
	data        *maphash.Map[[]byte] // key → value, concurrent
	capacityB   datasize.ByteSize    // max bytes
	currentSize atomic.Int64         // current size in bytes
	blockHash   common.Hash          // hash of the last block processed
	mu          sync.RWMutex

	// Stats counters (atomic for concurrent access)
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewGenericCache creates a new GenericCache with the specified byte capacity.
func NewGenericCache(capacityBytes datasize.ByteSize) *GenericCache {
	return &GenericCache{
		data:      maphash.NewMap[[]byte](),
		capacityB: capacityBytes,
	}
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
// If the key exists, updates the value (always allowed).
// If the key is new and cache is at capacity, this is a no-op.
func (c *GenericCache) Put(key []byte, value []byte) {
	entrySize := int64(len(key) + len(value))

	// Check if key already exists - updates are always allowed
	if existing, ok := c.data.Get(key); ok {
		oldSize := int64(len(key) + len(existing))
		sizeDiff := entrySize - oldSize

		// Reuse existing slice if capacity is sufficient
		if cap(existing) >= len(value) {
			existing = existing[:len(value)]
			copy(existing, value)
			c.data.Set(key, existing)
		} else {
			c.data.Set(key, common.Copy(value))
		}
		c.currentSize.Add(sizeDiff)
		return
	}

	// New key - check if we have capacity
	if c.currentSize.Load()+entrySize > int64(c.capacityB) {
		return // no-op when full
	}

	c.data.Set(key, common.Copy(value))
	c.currentSize.Add(entrySize)
}

// Delete removes the data for the given key.
func (c *GenericCache) Delete(key []byte) {
	if existing, ok := c.data.Get(key); ok {
		entrySize := int64(len(key) + len(existing))
		c.data.Delete(key)
		c.currentSize.Add(-entrySize)
	}
}

// Clear removes all entries from the cache.
func (c *GenericCache) Clear() {
	c.data.Clear()
	c.currentSize.Store(0)
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
		c.currentSize.Store(0)
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
	c.currentSize.Store(0)
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the cache and sets the block hash.
// Used during unwind to reset the cache to a known state.
func (c *GenericCache) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.Clear()
	c.currentSize.Store(0)
	c.blockHash = hash
}

// Len returns the number of entries in the cache.
func (c *GenericCache) Len() int {
	return c.data.Len()
}

// SizeBytes returns the current size of the cache in bytes.
func (c *GenericCache) SizeBytes() int64 {
	return c.currentSize.Load()
}

// CapacityBytes returns the capacity of the cache in bytes.
func (c *GenericCache) CapacityBytes() datasize.ByteSize {
	return c.capacityB
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

	sizeBytes := c.currentSize.Load()
	usagePct := float64(sizeBytes) / float64(c.capacityB) * 100

	log.Debug(name+" cache stats",
		"hits", hits,
		"misses", misses,
		"hit_rate", hitRate,
		"entries", c.data.Len(),
		"size_mb", sizeBytes/(1024*1024),
		"capacity_mb", c.capacityB/datasize.MB,
		"usage_pct", usagePct,
	)
}
