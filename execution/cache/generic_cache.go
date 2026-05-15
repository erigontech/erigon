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
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

// avgBytesPerEntry is the assumption used to translate a byte budget into
// the entry-count cap that freelru.ShardedLRU is sized against. 256 B
// approximates account-record + key overhead and storage-slot value+key
// overhead in the same order of magnitude. Actual residency tracked in
// currentSize and reported via PrintStatsAndReset.
const avgBytesPerEntry = 256

// entry stores the full key alongside the value so callers can detect
// hash collisions (the freelru shard key is the uint64 maphash of the
// byte-string key — Go's randomized stdlib hasher, so collisions are
// rare but not impossible). size carries the byte cost of the entry so
// the OnEvict callback can update currentSize without re-running
// sizeFunc.
type entry[T any] struct {
	key  []byte
	val  T
	size int
}

// GenericCache is a sharded, LRU-evicting bounded cache for key-value
// data. Eviction mode is fixed at construction (see policy.go).
type GenericCache[T any] struct {
	data        *freelru.ShardedLRU[uint64, entry[T]]
	capacityB   datasize.ByteSize
	currentSize atomic.Int64
	mode        Mode

	blockHash common.Hash
	mu        sync.RWMutex

	hits      atomic.Uint64
	misses    atomic.Uint64
	inserts   atomic.Uint64
	evictions atomic.Uint64
	dropped   atomic.Uint64

	sizeFunc func(T) int
}

func u64identity(k uint64) uint32 { return uint32(k) }

// NewGenericCache creates a new GenericCache with the specified byte
// capacity. mode selects ModeEvictLRU (default in this tree) or ModeNoOp
// (kept for diagnostic baselines).
func NewGenericCache[T any](capacityBytes datasize.ByteSize, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	capacityEntries := uint32(uint64(capacityBytes) / avgBytesPerEntry)
	if capacityEntries < 1024 {
		capacityEntries = 1024
	}
	if capacityEntries > 1<<22 {
		capacityEntries = 1 << 22
	}
	return newGenericCacheEntries(capacityBytes, capacityEntries, sizeFunc, mode)
}

// newGenericCacheEntries builds a cache against an explicit entry-count
// cap. Used by tests that want to exercise eviction with small capacities;
// production constructs via NewGenericCache.
func newGenericCacheEntries[T any](capacityBytes datasize.ByteSize, capacityEntries uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	if capacityEntries == 0 {
		capacityEntries = 1
	}
	lru, err := freelru.NewSharded[uint64, entry[T]](capacityEntries, u64identity)
	if err != nil {
		panic(err)
	}
	c := &GenericCache[T]{
		data:      lru,
		capacityB: capacityBytes,
		mode:      mode,
		sizeFunc:  sizeFunc,
	}
	// OnEvict fires for capacity-driven LRU eviction (ModeEvictLRU) and
	// for explicit Remove(). In both cases we want currentSize to follow.
	lru.SetOnEvict(func(_ uint64, e entry[T]) {
		c.currentSize.Add(-int64(e.size))
		c.evictions.Add(1)
	})
	return c
}

// DomainCache wraps GenericCache[[]byte] to implement the Cache interface.
type DomainCache struct {
	*GenericCache[[]byte]
}

// NewDomainCache creates a new cache for domain data. Defaults to
// ModeEvictLRU; use NewDomainCacheMode to override.
func NewDomainCache(capacityBytes datasize.ByteSize) *DomainCache {
	return NewDomainCacheMode(capacityBytes, ModeEvictLRU)
}

// NewDomainCacheMode creates a new domain cache with the given mode.
func NewDomainCacheMode(capacityBytes datasize.ByteSize, mode Mode) *DomainCache {
	return &DomainCache{
		GenericCache: NewGenericCache(capacityBytes, func(v []byte) int { return len(v) }, mode),
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
	h := maphash.Hash(key)
	e, ok := c.data.Get(h)
	if !ok || !bytes.Equal(e.key, key) {
		c.misses.Add(1)
		var zero T
		return zero, false
	}
	c.hits.Add(1)
	return e.val, true
}

// Put stores data for the given key. In ModeEvictLRU the underlying
// sharded LRU evicts cold entries when its entry-count cap is reached.
// In ModeNoOp inserts that would overflow the byte budget are dropped
// (and counted via the dropped metric).
func (c *GenericCache[T]) Put(key []byte, value T) {
	h := maphash.Hash(key)
	valBytes := c.sizeFunc(value)
	newSize := len(key) + valBytes + 24

	// Existing key — update in place. Reuse the stored key buffer to
	// avoid an extra allocation; the freshly-decoded value replaces the
	// old one.
	if existing, ok := c.data.Get(h); ok && bytes.Equal(existing.key, key) {
		oldSize := existing.size
		c.data.Add(h, entry[T]{key: existing.key, val: value, size: newSize})
		c.currentSize.Add(int64(newSize - oldSize))
		return
	}

	if c.mode == ModeNoOp {
		if c.currentSize.Load()+int64(newSize) > int64(c.capacityB) {
			c.dropped.Add(1)
			return
		}
	}
	// In ModeEvictLRU the per-shard LRU evicts the oldest entry inside
	// freelru.Add when its slot cap is reached; OnEvict drops the size
	// from currentSize. Eviction is per-shard, not globally-LRU — same
	// trade-off code_cache.go / balcache.go / db/state/cache.go accept.

	keyCopy := common.Copy(key)
	c.data.Add(h, entry[T]{key: keyCopy, val: value, size: newSize})
	c.currentSize.Add(int64(newSize))
	c.inserts.Add(1)
}

// Delete removes the data for the given key.
func (c *GenericCache[T]) Delete(key []byte) {
	h := maphash.Hash(key)
	if existing, ok := c.data.Get(h); ok && bytes.Equal(existing.key, key) {
		c.data.Remove(h)
	}
}

// Clear removes all entries from the cache.
func (c *GenericCache[T]) Clear() {
	c.data.Purge()
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
		c.data.Purge()
		c.currentSize.Store(0)
		c.blockHash = incomingBlockHash
		return true
	}

	if c.blockHash == parentHash {
		c.blockHash = incomingBlockHash
		return true
	}

	c.data.Purge()
	c.currentSize.Store(0)
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the cache and sets the block hash.
func (c *GenericCache[T]) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.Purge()
	c.currentSize.Store(0)
	c.blockHash = hash
}

// Len returns the number of entries in the cache.
func (c *GenericCache[T]) Len() int {
	return int(c.data.Len())
}

// SizeBytes returns the current size of the cache in bytes.
func (c *GenericCache[T]) SizeBytes() int64 {
	return c.currentSize.Load()
}

// CapacityBytes returns the capacity of the cache in bytes.
func (c *GenericCache[T]) CapacityBytes() datasize.ByteSize {
	return c.capacityB
}

// Mode returns the eviction mode this cache was constructed with.
func (c *GenericCache[T]) Mode() Mode {
	return c.mode
}

// PrintStatsAndReset prints cache statistics and resets counters.
func (c *GenericCache[T]) PrintStatsAndReset(name string) {
	hits := c.hits.Swap(0)
	misses := c.misses.Swap(0)
	inserts := c.inserts.Swap(0)
	evictions := c.evictions.Swap(0)
	dropped := c.dropped.Swap(0)
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	sizeBytes := c.currentSize.Load()
	usagePct := float64(sizeBytes) / float64(c.capacityB) * 100
	log.Debug(name+" cache stats",
		"mode", c.mode.String(),
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"inserts", inserts, "evictions", evictions, "dropped", dropped,
		"entries", c.data.Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", c.capacityB/datasize.MB, "usage_pct", usagePct,
	)
}
