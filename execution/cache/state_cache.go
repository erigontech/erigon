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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

const (
	// DefaultAccountCacheSize is the number of account entries
	// 2^20 = 1048576 entries, ~110 MB worst case (100 bytes per account)
	DefaultAccountCacheSize = 1 << 20

	// DefaultStorageCacheSize is the number of storage entries
	// 2^20 = 1048576 entries
	DefaultStorageCacheSize = 1 << 20
)

// StateCache is a unified cache for domain data (Account, Storage, Code).
// Uses an array indexed by kv.Domain. Only Account, Storage, and Code domains
// are supported; other indices are nil.
//
// Account and Storage use GenericCache.
// Code uses CodeCache (two-level for deduplication).
type StateCache struct {
	caches [kv.DomainLen]Cache
}

// NewStateCache creates a new StateCache with the specified sizes.
func NewStateCache(accountSize, storageSize, codeSize, codeAddrSize int) *StateCache {
	sc := &StateCache{}
	sc.caches[kv.AccountsDomain] = NewGenericCache(accountSize)
	sc.caches[kv.StorageDomain] = NewGenericCache(storageSize)
	sc.caches[kv.CodeDomain] = NewCodeCache(codeSize, codeAddrSize)
	return sc
}

// NewDefaultStateCache creates a new StateCache with default sizes.
func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		DefaultAccountCacheSize,
		DefaultStorageCacheSize,
		DefaultCodeCacheSize,
		DefaultAddrCacheSize,
	)
}

// Get retrieves data for the given domain and key.
func (c *StateCache) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	cache := c.caches[domain]
	if cache == nil {
		return nil, false
	}
	return cache.Get(key)
}

// Put stores data for the given domain and key.
func (c *StateCache) Put(domain kv.Domain, key []byte, value []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	cache.Put(key, value)
}

// Delete removes the data for the given domain and key.
func (c *StateCache) Delete(domain kv.Domain, key []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	cache.Delete(key)
}

// Clear removes all mutable entries from all caches.
func (c *StateCache) Clear() {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Clear()
		}
	}
}

// ValidateAndPrepare validates and prepares all caches for a new block.
// Returns true if all caches were valid, false if any were cleared.
func (c *StateCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	allValid := true
	for _, cache := range c.caches {
		if cache != nil {
			if !cache.ValidateAndPrepare(parentHash, incomingBlockHash) {
				allValid = false
			}
		}
	}
	return allValid
}

// ClearWithHash clears all caches and sets their block hash.
func (c *StateCache) ClearWithHash(hash common.Hash) {
	for _, cache := range c.caches {
		if cache != nil {
			cache.ClearWithHash(hash)
		}
	}
}

// GetCache returns the cache for the given domain.
// Returns nil if the domain is not supported.
func (c *StateCache) GetCache(domain kv.Domain) Cache {
	if domain >= kv.DomainLen {
		return nil
	}
	return c.caches[domain]
}
