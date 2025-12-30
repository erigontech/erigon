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

// Key sizes for different cache types
const (
	// AccountKeySize is 20 bytes (address)
	AccountKeySize = 20
	// StorageKeySize is 52 bytes (20 addr + 32 hash)
	StorageKeySize = 52
	// BranchKeySize is max 64 bytes (nibble path)
	BranchKeySize = 64
)

// Fixed-size array types for map keys (no allocations)
type (
	accountCacheKey [AccountKeySize]byte
	storageCacheKey [StorageKeySize]byte
	branchCacheKey  [BranchKeySize]byte
)

type branchEntry struct {
	data []byte
	step kv.Step
}

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing. Uses fixed-size array keys
// to avoid string allocations on every lookup.
type WarmupCache struct {
	mu sync.RWMutex

	branches    map[branchCacheKey]branchEntry
	accounts    map[accountCacheKey]*Update
	storage     map[storageCacheKey]*Update
	evictedKeys map[storageCacheKey]struct{} // use largest key size for evicted

	enabled atomic.Bool
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	c := &WarmupCache{
		branches:    make(map[branchCacheKey]branchEntry),
		accounts:    make(map[accountCacheKey]*Update),
		storage:     make(map[storageCacheKey]*Update),
		evictedKeys: make(map[storageCacheKey]struct{}),
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

// Helper functions to convert byte slices to fixed-size arrays
func toBranchKey(key []byte) branchCacheKey {
	var k branchCacheKey
	copy(k[:], key)
	return k
}

func toAccountKey(key []byte) accountCacheKey {
	var k accountCacheKey
	copy(k[:], key)
	return k
}

func toStorageKey(key []byte) storageCacheKey {
	var k storageCacheKey
	copy(k[:], key)
	return k
}

// EvictKey permanently evicts a key from the cache.
func (c *WarmupCache) EvictKey(key []byte) {
	k := toStorageKey(key) // use largest key size
	c.mu.Lock()
	c.evictedKeys[k] = struct{}{}
	c.mu.Unlock()
}

// IsEvicted returns true if the key has been permanently evicted.
func (c *WarmupCache) IsEvicted(key []byte) bool {
	k := toStorageKey(key)
	c.mu.RLock()
	_, evicted := c.evictedKeys[k]
	c.mu.RUnlock()
	return evicted
}

// PutBranch stores branch data in the cache.
func (c *WarmupCache) PutBranch(prefix []byte, data []byte, step kv.Step) {
	if !c.enabled.Load() {
		return
	}
	// Make a copy of the data to avoid issues with buffer reuse
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	k := toBranchKey(prefix)
	c.mu.Lock()
	c.branches[k] = branchEntry{data: dataCopy, step: step}
	c.mu.Unlock()
}

// GetBranch retrieves branch data from the cache.
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, kv.Step, bool) {
	if !c.enabled.Load() {
		return nil, 0, false
	}
	if c.IsEvicted(prefix) {
		return nil, 0, false
	}
	k := toBranchKey(prefix)
	c.mu.RLock()
	entry, found := c.branches[k]
	c.mu.RUnlock()
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
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	k := toAccountKey(plainKey)
	c.mu.Lock()
	c.accounts[k] = updateCopy
	c.mu.Unlock()
}

// GetAccount retrieves account data from the cache.
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	k := toAccountKey(plainKey)
	c.mu.RLock()
	update, found := c.accounts[k]
	c.mu.RUnlock()
	return update, found
}

// PutStorage stores storage data in the cache.
func (c *WarmupCache) PutStorage(plainKey []byte, update *Update) {
	if !c.enabled.Load() {
		return
	}
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	k := toStorageKey(plainKey)
	c.mu.Lock()
	c.storage[k] = updateCopy
	c.mu.Unlock()
}

// GetStorage retrieves storage data from the cache.
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	k := toStorageKey(plainKey)
	c.mu.RLock()
	update, found := c.storage[k]
	c.mu.RUnlock()
	return update, found
}

// Clear clears all cached data.
func (c *WarmupCache) Clear() {
	c.mu.Lock()
	c.branches = make(map[branchCacheKey]branchEntry)
	c.accounts = make(map[accountCacheKey]*Update)
	c.storage = make(map[storageCacheKey]*Update)
	c.evictedKeys = make(map[storageCacheKey]struct{})
	c.mu.Unlock()
}
