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
type GenericCache struct {
	data        *maphash.Map[[]byte]
	capacityB   datasize.ByteSize
	currentSize atomic.Int64
	blockHash   common.Hash
	mu          sync.RWMutex
	hits        atomic.Uint64
	misses      atomic.Uint64
}

// NewGenericCache creates a new GenericCache with the specified byte capacity.
func NewGenericCache(capacityBytes datasize.ByteSize) *GenericCache {
	return &GenericCache{
		data:      maphash.NewMap[[]byte](),
		capacityB: capacityBytes,
	}
}

// Get retrieves data for the given key.
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
func (c *GenericCache) Put(key []byte, value []byte) {
	entrySize := int64(len(key) + len(value))

	// Check if key already exists
	if existing, ok := c.data.Get(key); ok {
		oldSize := int64(len(key) + len(existing))
		sizeDiff := entrySize - oldSize

		// Always allocate a new slice to avoid modifying shared backing arrays.
		// External code (like diffset) may hold references to the old slice.
		c.data.Set(key, common.Copy(value))
		c.currentSize.Add(sizeDiff)
		return
	}

	// New key
	if c.currentSize.Load()+entrySize > int64(c.capacityB) {
		return
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
func (c *GenericCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.blockHash == (common.Hash{}) {
		c.data.Clear()
		c.currentSize.Store(0)
		c.blockHash = incomingBlockHash
		return true
	}

	if c.blockHash == parentHash {
		c.blockHash = incomingBlockHash
		return true
	}

	c.data.Clear()
	c.currentSize.Store(0)
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the cache and sets the block hash.
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
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"entries", c.data.Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", c.capacityB/datasize.MB, "usage_pct", usagePct,
	)
}
