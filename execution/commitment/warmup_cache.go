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
	data      []byte
	isEvicted atomic.Bool
}

type accountEntry struct {
	update    *Update
	isEvicted atomic.Bool
}

type storageEntry struct {
	update    *Update
	isEvicted atomic.Bool
}

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing. Uses maphash.Map for efficient
// byte slice key lookups without string allocations.
type WarmupCache struct {
	branches *maphash.Map[*branchEntry]
	accounts *maphash.Map[*accountEntry]
	storage  *maphash.Map[*storageEntry]

	enabled atomic.Bool
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	c := &WarmupCache{
		branches: maphash.NewMap[*branchEntry](),
		accounts: maphash.NewMap[*accountEntry](),
		storage:  maphash.NewMap[*storageEntry](),
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

// PutBranch stores branch data in the cache.
func (c *WarmupCache) PutBranch(prefix []byte, data []byte) {
	if !c.enabled.Load() {
		return
	}
	// Make a copy of the data to avoid issues with buffer reuse
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	c.branches.Set(prefix, &branchEntry{data: dataCopy})
}

// GetBranch retrieves branch data from the cache.
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	entry, found := c.branches.Get(prefix)
	if !found || entry.isEvicted.Load() {
		return nil, false
	}
	return entry.data, true
}

// GetAndEvictBranch retrieves branch data and marks the entry as evicted in one operation.
func (c *WarmupCache) GetAndEvictBranch(prefix []byte) ([]byte, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	entry, found := c.branches.Get(prefix)
	if !found || entry.isEvicted.Load() {
		return nil, false
	}
	entry.isEvicted.Store(true)
	return entry.data, true
}

// EvictBranch marks a branch entry as evicted without retrieving it.
func (c *WarmupCache) EvictBranch(prefix []byte) {
	if !c.enabled.Load() {
		return
	}
	entry, found := c.branches.Get(prefix)
	if found {
		entry.isEvicted.Store(true)
	}
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

	c.accounts.Set(plainKey, &accountEntry{update: updateCopy})
}

// GetAccount retrieves account data from the cache.
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	entry, found := c.accounts.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil, false
	}
	return entry.update, true
}

// GetAndEvictAccount retrieves account data and marks the entry as evicted in one operation.
// Returns the entry pointer allowing the caller to read the data before it's considered evicted.
func (c *WarmupCache) GetAndEvictAccount(plainKey []byte) *accountEntry {
	if !c.enabled.Load() {
		return nil
	}
	entry, found := c.accounts.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil
	}
	entry.isEvicted.Store(true)
	return entry
}

// EvictAccount marks an account entry as evicted without retrieving it.
func (c *WarmupCache) EvictAccount(plainKey []byte) {
	if !c.enabled.Load() {
		return
	}
	entry, found := c.accounts.Get(plainKey)
	if found {
		entry.isEvicted.Store(true)
	}
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

	c.storage.Set(plainKey, &storageEntry{update: updateCopy})
}

// GetStorage retrieves storage data from the cache.
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	if !c.enabled.Load() {
		return nil, false
	}
	entry, found := c.storage.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil, false
	}
	return entry.update, true
}

// GetAndEvictStorage retrieves storage data and marks the entry as evicted in one operation.
// Returns the entry pointer allowing the caller to read the data before it's considered evicted.
func (c *WarmupCache) GetAndEvictStorage(plainKey []byte) *storageEntry {
	if !c.enabled.Load() {
		return nil
	}
	entry, found := c.storage.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil
	}
	entry.isEvicted.Store(true)
	return entry
}

// EvictStorage marks a storage entry as evicted without retrieving it.
func (c *WarmupCache) EvictStorage(plainKey []byte) {
	if !c.enabled.Load() {
		return
	}
	entry, found := c.storage.Get(plainKey)
	if found {
		entry.isEvicted.Store(true)
	}
}

// EvictPlainKey evicts a key from both accounts and storage caches.
// Use this when you don't know if the key is an account or storage key.
func (c *WarmupCache) EvictPlainKey(plainKey []byte) {
	if !c.enabled.Load() {
		return
	}
	if entry, found := c.accounts.Get(plainKey); found {
		entry.isEvicted.Store(true)
	}
	if entry, found := c.storage.Get(plainKey); found {
		entry.isEvicted.Store(true)
	}
}

// Clear clears all cached data.
func (c *WarmupCache) Clear() {
	c.branches = maphash.NewMap[*branchEntry]()
	c.accounts = maphash.NewMap[*accountEntry]()
	c.storage = maphash.NewMap[*storageEntry]()
}
