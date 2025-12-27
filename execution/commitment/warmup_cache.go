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

type branchEntry struct {
	data []byte
	step kv.Step
}

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing.
type WarmupCache struct {
	branches sync.Map // map[string]branchEntry
	accounts sync.Map // map[string]*Update
	storage  sync.Map // map[string]*Update

	// Permanently evicted keys - keys stored here will never be served from cache
	evictedKeys sync.Map // map[string]struct{}

	enabled atomic.Bool
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	c := &WarmupCache{}
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

// PutBranch stores branch data in the cache.
func (c *WarmupCache) PutBranch(prefix []byte, data []byte, step kv.Step) {
	if !c.enabled.Load() {
		return
	}
	// Make a copy of the data to avoid issues with buffer reuse
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	c.branches.Store(string(prefix), branchEntry{data: dataCopy, step: step})
}

// GetBranch retrieves branch data from the cache.
// Returns (data, step, found).
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, kv.Step, bool) {
	if !c.enabled.Load() {
		return nil, 0, false
	}
	if c.IsEvicted(prefix) {
		return nil, 0, false
	}
	if v, found := c.branches.Load(string(prefix)); found {
		entry := v.(branchEntry)
		return entry.data, entry.step, true
	}
	return nil, 0, false
}

// PutAccount stores account data in the cache.
func (c *WarmupCache) PutAccount(plainKey []byte, update *Update) {
	if !c.enabled.Load() {
		return
	}
	// Make a copy of the update
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	c.accounts.Store(string(plainKey), updateCopy)
}

// GetAccount retrieves account data from the cache.
// Returns (update, found).
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	if v, found := c.accounts.Load(string(plainKey)); found {
		return v.(*Update), true
	}
	return nil, false
}

// PutStorage stores storage data in the cache.
func (c *WarmupCache) PutStorage(plainKey []byte, update *Update) {
	if !c.enabled.Load() {
		return
	}
	// Make a copy of the update
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	c.storage.Store(string(plainKey), updateCopy)
}

// GetStorage retrieves storage data from the cache.
// Returns (update, found).
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	if v, found := c.storage.Load(string(plainKey)); found {
		return v.(*Update), true
	}
	return nil, false
}

// Clear clears all cached data.
func (c *WarmupCache) Clear() {
	c.branches = sync.Map{}
	c.accounts = sync.Map{}
	c.storage = sync.Map{}
	c.evictedKeys = sync.Map{}
}
