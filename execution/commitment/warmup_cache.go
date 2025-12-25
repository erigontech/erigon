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

	"github.com/erigontech/erigon/db/kv"
)

const shardAmount = 32

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing. The cache is designed for
// minimal lock contention using sharded maps.
type WarmupCache struct {
	// Sharded maps to reduce lock contention - 16 shards based on first byte of key
	branchShards  [shardAmount]branchShard
	accountShards [shardAmount]updateShard
	storageShards [shardAmount]updateShard

	// Permanently evicted keys - uses sync.Map for lock-free reads
	// Keys stored here will never be served from cache regardless of type
	evictedKeys sync.Map // map[string]struct{}

	enabled atomic.Bool
}

type branchShard struct {
	mu   sync.RWMutex
	data map[string]branchEntry
}

type branchEntry struct {
	data []byte
	step kv.Step
}

type updateShard struct {
	mu   sync.RWMutex
	data map[string]*Update
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	c := &WarmupCache{}
	for i := range c.branchShards {
		c.branchShards[i].data = make(map[string]branchEntry)
	}
	for i := range c.accountShards {
		c.accountShards[i].data = make(map[string]*Update)
	}
	for i := range c.storageShards {
		c.storageShards[i].data = make(map[string]*Update)
	}
	c.enabled.Store(true)
	return c
}

// Enable enables or disables the cache.
func (c *WarmupCache) Enable(enabled bool) {
	c.enabled.Store(enabled)
}

// IsEnabled returns whether the cache is enabled.
func (c *WarmupCache) IsEnabled() bool {
	return c.enabled.Load()
}

// EvictKey permanently evicts a key from the cache. Once evicted, the key
// will never be served from cache regardless of type (branch, account, or storage).
// This is useful when a key is known to be modified and the cached value is stale.
func (c *WarmupCache) EvictKey(key []byte) {
	c.evictedKeys.Store(string(key), struct{}{})
}

// IsEvicted returns true if the key has been permanently evicted.
func (c *WarmupCache) IsEvicted(key []byte) bool {
	_, evicted := c.evictedKeys.Load(string(key))
	return evicted
}

// shardIndex returns the shard index for a given key.
func shardIndex(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	return int(key[len(key)-1] % shardAmount)
}

// PutBranch stores branch data in the cache.
func (c *WarmupCache) PutBranch(prefix []byte, data []byte, step kv.Step) {
	if !c.enabled.Load() {
		return
	}
	idx := shardIndex(prefix)
	shard := &c.branchShards[idx]

	// Make a copy of the data to avoid issues with buffer reuse
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	shard.mu.Lock()
	shard.data[string(prefix)] = branchEntry{data: dataCopy, step: step}
	shard.mu.Unlock()
}

// GetBranch retrieves branch data from the cache.
// Returns (data, step, found).
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, kv.Step, bool) {
	if !c.enabled.Load() {
		return nil, 0, false
	}
	// Check if key is evicted
	if c.IsEvicted(prefix) {
		return nil, 0, false
	}
	idx := shardIndex(prefix)
	shard := &c.branchShards[idx]

	shard.mu.RLock()
	entry, found := shard.data[string(prefix)]
	shard.mu.RUnlock()

	if found {
		return entry.data, entry.step, true
	}
	return nil, 0, false
}

// PutAccount stores account data in the cache.
func (c *WarmupCache) PutAccount(plainKey []byte, update *Update) {
	if !c.enabled.Load() {
		return
	}
	idx := shardIndex(plainKey)
	shard := &c.accountShards[idx]

	// Make a copy of the update
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	shard.mu.Lock()
	shard.data[string(plainKey)] = updateCopy
	shard.mu.Unlock()
}

// GetAccount retrieves account data from the cache.
// Returns (update, found).
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	// Check if key is evicted
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	idx := shardIndex(plainKey)
	shard := &c.accountShards[idx]

	shard.mu.RLock()
	update, found := shard.data[string(plainKey)]
	shard.mu.RUnlock()

	if found {
		return update, true
	}
	return nil, false
}

// PutStorage stores storage data in the cache.
func (c *WarmupCache) PutStorage(plainKey []byte, update *Update) {
	if !c.enabled.Load() {
		return
	}
	idx := shardIndex(plainKey)
	shard := &c.storageShards[idx]

	// Make a copy of the update
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	shard.mu.Lock()
	shard.data[string(plainKey)] = updateCopy
	shard.mu.Unlock()
}

// GetStorage retrieves storage data from the cache.
// Returns (update, found).
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	// Check if key is evicted
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	idx := shardIndex(plainKey)
	shard := &c.storageShards[idx]

	shard.mu.RLock()
	update, found := shard.data[string(plainKey)]
	shard.mu.RUnlock()

	if found {
		return update, true
	}
	return nil, false
}

// Clear clears all cached data.
func (c *WarmupCache) Clear() {
	for i := range c.branchShards {
		c.branchShards[i].mu.Lock()
		c.branchShards[i].data = make(map[string]branchEntry)
		c.branchShards[i].mu.Unlock()
	}
	for i := range c.accountShards {
		c.accountShards[i].mu.Lock()
		c.accountShards[i].data = make(map[string]*Update)
		c.accountShards[i].mu.Unlock()
	}
	for i := range c.storageShards {
		c.storageShards[i].mu.Lock()
		c.storageShards[i].data = make(map[string]*Update)
		c.storageShards[i].mu.Unlock()
	}
}
