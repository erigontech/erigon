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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/maphash"
)

const DefaultCodeCacheSize = 512

// CodeCache is an LRU cache for contract code, keyed by address.
// It is a read-only cache intended to speed up repeated code lookups.
// It tracks a block hash to ensure cache consistency across block execution.
type CodeCache struct {
	cache     *maphash.LRU[[]byte]
	blockHash common.Hash // hash of the last block processed
	mu        sync.RWMutex
}

// NewCodeCache creates a new CodeCache with the specified size.
func NewCodeCache(size int) *CodeCache {
	c, err := maphash.NewLRU[[]byte](size)
	if err != nil {
		panic(err)
	}
	return &CodeCache{cache: c}
}

// NewDefaultCodeCache creates a new CodeCache with the default size (512).
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheSize)
}

// Get retrieves contract code for the given address.
// Returns the code and true if found, nil and false otherwise.
func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	return c.cache.Get(addr)
}

// Put stores contract code for the given address.
func (c *CodeCache) Put(addr []byte, code []byte) {
	c.cache.Set(addr, code)
}

// Contains checks if the cache contains code for the given address.
func (c *CodeCache) Contains(addr []byte) bool {
	return c.cache.Contains(addr)
}

// Remove removes the code for the given address from the cache.
func (c *CodeCache) Remove(addr []byte) {
	c.cache.Delete(addr)
}

// Len returns the number of items in the cache.
func (c *CodeCache) Len() int {
	return c.cache.Len()
}

// Clear removes all items from the cache.
func (c *CodeCache) Clear() {
	c.cache.Purge()
}

// GetBlockHash returns the hash of the last block processed by the cache.
func (c *CodeCache) GetBlockHash() common.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blockHash
}

// SetBlockHash sets the hash of the current block being processed.
func (c *CodeCache) SetBlockHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blockHash = hash
}

// ValidateAndPrepare checks if the given parentHash matches the cache's current blockHash.
// If there's a mismatch (indicating non-sequential block processing), the cache is cleared.
// Returns true if the cache was valid (hashes matched or cache was empty), false if cleared.
func (c *CodeCache) ValidateAndPrepare(parentHash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Empty hash means cache hasn't processed any block yet - always valid
	if c.blockHash == (common.Hash{}) {
		return true
	}

	// Check if we're continuing from the expected block
	if c.blockHash == parentHash {
		return true
	}

	// Mismatch - clear the cache as we can't trust its contents
	c.cache.Purge()
	c.blockHash = common.Hash{}
	return false
}

// ClearWithHash clears the cache and sets the block hash.
// Used during unwind to reset the cache to a known state.
func (c *CodeCache) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Purge()
	c.blockHash = hash
}
