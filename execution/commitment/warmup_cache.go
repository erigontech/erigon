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
	"github.com/erigontech/erigon/db/kv"
)

type branchEntry struct {
	data []byte
	step kv.Step
}

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing. Uses maphash.Map for efficient
// byte slice key lookups without string allocations.
type WarmupCache struct {
	branches    *maphash.Map[branchEntry]
	accounts    *maphash.Map[*Update]
	storage     *maphash.Map[*Update]
	evictedKeys *maphash.Map[struct{}]

	enabled atomic.Bool
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	c := &WarmupCache{
		branches:    maphash.NewMap[branchEntry](),
		accounts:    maphash.NewMap[*Update](),
		storage:     maphash.NewMap[*Update](),
		evictedKeys: maphash.NewMap[struct{}](),
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

// EvictKey permanently evicts a key from the cache.
func (c *WarmupCache) EvictKey(key []byte) {
	c.evictedKeys.Set(key, struct{}{})
}

// IsEvicted returns true if the key has been permanently evicted.
func (c *WarmupCache) IsEvicted(key []byte) bool {
	_, evicted := c.evictedKeys.Get(key)
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

	c.branches.Set(prefix, branchEntry{data: dataCopy, step: step})
}

// GetBranch retrieves branch data from the cache.
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, kv.Step, bool) {
	if !c.enabled.Load() {
		return nil, 0, false
	}
	if c.IsEvicted(prefix) {
		return nil, 0, false
	}
	entry, found := c.branches.Get(prefix)
	return entry.data, entry.step, found
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

	c.accounts.Set(plainKey, updateCopy)
}

// GetAccount retrieves account data from the cache.
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	return c.accounts.Get(plainKey)
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

	c.storage.Set(plainKey, updateCopy)
}

// GetStorage retrieves storage data from the cache.
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	if c.IsEvicted(plainKey) {
		return nil, false
	}
	return c.storage.Get(plainKey)
}

// Clear clears all cached data.
func (c *WarmupCache) Clear() {
	c.branches = maphash.NewMap[branchEntry]()
	c.accounts = maphash.NewMap[*Update]()
	c.storage = maphash.NewMap[*Update]()
	c.evictedKeys = maphash.NewMap[struct{}]()
}
