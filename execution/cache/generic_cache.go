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

// GenericCache is a bounded concurrent cache for key-value data.
type GenericCache[T any] struct {
	data        *maphash.Map[T]
	capacityB   datasize.ByteSize
	currentSize atomic.Int64
	blockHash   common.Hash
	mu          sync.RWMutex
	hits        atomic.Uint64
	misses      atomic.Uint64
	sizeFunc    func(T) int // calculates size of value in bytes
}

// NewGenericCache creates a new GenericCache with the specified byte capacity.
func NewGenericCache[T any](capacityBytes datasize.ByteSize, sizeFunc func(T) int) *GenericCache[T] {
	return &GenericCache[T]{
		data:      maphash.NewMap[T](),
		capacityB: capacityBytes,
		sizeFunc:  sizeFunc,
	}
}

// DomainCache wraps GenericCache[[]byte] to implement the Cache interface.
type DomainCache struct {
	*GenericCache[[]byte]
}

// NewDomainCache creates a new cache for domain data.
func NewDomainCache(capacityBytes datasize.ByteSize) *DomainCache {
	return &DomainCache{
		GenericCache: NewGenericCache(capacityBytes, func(v []byte) int { return len(v) }),
	}
}

// Get retrieves data for the given key, implementing the Cache interface.
func (c *DomainCache) Get(key []byte) ([]byte, bool) {
	entry, ok := c.GenericCache.Get(key)
	if !ok {
		return nil, false
	}
	return entry, true
}

// Put stores data for the given key, implementing the Cache interface.
func (c *DomainCache) Put(key []byte, value []byte) {
	c.GenericCache.Put(key, value)
}

// Delete removes the data for the given key, delegating to GenericCache.
func (c *DomainCache) Delete(key []byte) {
	c.GenericCache.Delete(key)
}

// Get retrieves data for the given key.
func (c *GenericCache[T]) Get(key []byte) (T, bool) {
	value, ok := c.data.Get(key)
	if !ok || c.sizeFunc(value) == 0 {
		c.misses.Add(1)
		var zero T
		return zero, false
	}
	c.hits.Add(1)
	return value, true
}

// Put stores data for the given key.
func (c *GenericCache[T]) Put(key []byte, value T) {
	entrySize := int64(8 + c.sizeFunc(value))

	// Check if key already exists
	if existing, ok := c.data.Get(key); ok {
		oldSize := int64(8 + c.sizeFunc(existing))
		sizeDiff := entrySize - oldSize
		c.data.Set(key, value)
		c.currentSize.Add(sizeDiff)
		return
	}

	// New key
	if c.currentSize.Load()+entrySize > int64(c.capacityB) {
		return
	}

	c.data.Set(key, value)
	c.currentSize.Add(entrySize)
}

// Delete removes the data for the given key.
func (c *GenericCache[T]) Delete(key []byte) {
	if existing, ok := c.data.Get(key); ok {
		entrySize := int64(8 + c.sizeFunc(existing))
		c.data.Delete(key)
		c.currentSize.Add(-entrySize)
	}
}

// Clear removes all entries from the cache.
func (c *GenericCache[T]) Clear() {
	c.data.Clear()
	c.currentSize.Store(0)
}

// GetBlockHash returns the hash of the last block processed by the cache.
func (c *GenericCache[T]) GetBlockHash() common.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blockHash
}

// SetBlockHash sets the hash of the current block being processed.
func (c *GenericCache[T]) SetBlockHash(hash common.Hash) {
	c.mu.Lock()
	c.blockHash = hash
	c.mu.Unlock()
}

// ValidateAndPrepare checks if the given parentHash matches the cache's current blockHash.
func (c *GenericCache[T]) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
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
func (c *GenericCache[T]) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.Clear()
	c.currentSize.Store(0)
	c.blockHash = hash
}

// Len returns the number of entries in the cache.
func (c *GenericCache[T]) Len() int {
	return c.data.Len()
}

// SizeBytes returns the current size of the cache in bytes.
func (c *GenericCache[T]) SizeBytes() int64 {
	return c.currentSize.Load()
}

// CapacityBytes returns the capacity of the cache in bytes.
func (c *GenericCache[T]) CapacityBytes() datasize.ByteSize {
	return c.capacityB
}

// PrintStatsAndReset prints cache statistics and resets counters.
func (c *GenericCache[T]) PrintStatsAndReset(name string) {
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
