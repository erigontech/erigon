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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

const (
	// DefaultCodeCacheSize is the number of code entries (codeHash → code)
	DefaultCodeCacheSize = 10_000
	// DefaultAddrCacheSize is the number of address entries (addr → codeHash)
	// 2^18 = 262144 entries, each storing a 32-byte hash
	DefaultAddrCacheSize = 1 << 18
)

// CodeCache is a two-level LRU cache for contract code.
// Level 1: addr → codeHash (mutable, cleared on reorg)
// Level 2: codeHash → code (immutable, never cleared)
//
// This design is efficient because:
// - Multiple addresses can share the same code (common with proxies/clones)
// - Code hash is immutable - same hash always means same code
// - Address mappings are small (32 bytes) so we can cache many more
type CodeCache struct {
	addrToHash *maphash.LRU[common.Hash] // addr → codeHash
	hashToCode *maphash.LRU[[]byte]      // codeHash → code
	blockHash  common.Hash               // hash of the last block processed
	mu         sync.RWMutex

	// Stats counters (atomic for concurrent access)
	addrHits   atomic.Uint64
	addrMisses atomic.Uint64
	codeHits   atomic.Uint64
	codeMisses atomic.Uint64
}

// NewCodeCache creates a new CodeCache with the specified sizes.
func NewCodeCache(codeSize, addrSize int) *CodeCache {
	addrToHash, err := maphash.NewLRU[common.Hash](addrSize)
	if err != nil {
		panic(err)
	}
	hashToCode, err := maphash.NewLRU[[]byte](codeSize)
	if err != nil {
		panic(err)
	}
	return &CodeCache{
		addrToHash: addrToHash,
		hashToCode: hashToCode,
	}
}

// NewDefaultCodeCache creates a new CodeCache with the default sizes.
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheSize, DefaultAddrCacheSize)
}

// Get retrieves contract code for the given address.
// Returns the code and true if found, nil and false otherwise.
func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	// First, look up the code hash for this address
	codeHash, ok := c.addrToHash.Get(addr)
	if !ok {
		c.addrMisses.Add(1)
		return nil, false
	}
	c.addrHits.Add(1)

	// Then, look up the code by hash
	code, ok := c.hashToCode.Get(codeHash[:])
	if !ok {
		c.codeMisses.Add(1)
		return nil, false
	}
	c.codeHits.Add(1)
	return code, true
}

// GetByHash retrieves contract code directly by code hash.
// This is useful when the code hash is already known.
func (c *CodeCache) GetByHash(codeHash common.Hash) ([]byte, bool) {
	return c.hashToCode.Get(codeHash[:])
}

// Put stores contract code for the given address with its code hash.
func (c *CodeCache) Put(addr []byte, codeHash common.Hash, code []byte) {
	// Store addr → codeHash mapping
	c.addrToHash.Set(addr, codeHash)
	// Store codeHash → code mapping (immutable, safe to overwrite)
	if len(code) > 0 {
		c.hashToCode.Set(codeHash[:], code)
	}
}

// RemoveAddress removes the address → codeHash mapping.
// The codeHash → code mapping is kept since it's immutable.
func (c *CodeCache) RemoveAddress(addr []byte) {
	c.addrToHash.Delete(addr)
}

// Clear removes all address mappings from the cache.
// The codeHash → code mappings are preserved since they're immutable.
func (c *CodeCache) Clear() {
	c.addrToHash.Purge()
}

// ClearAll removes all entries from both caches.
func (c *CodeCache) ClearAll() {
	c.addrToHash.Purge()
	c.hashToCode.Purge()
}

// GetBlockHash returns the hash of the last block processed by the cache.
func (c *CodeCache) GetBlockHash() common.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blockHash
}

// SetBlockHash sets the hash of the current block being processed.
// If blockNum > 0, also prints cache statistics for this block.
func (c *CodeCache) SetBlockHash(hash common.Hash) {
	c.mu.Lock()
	c.blockHash = hash
	c.mu.Unlock()
}

// ValidateAndPrepare checks if the given parentHash matches the cache's current blockHash.
// If there's a mismatch (indicating non-sequential block processing), the address cache is cleared.
// The code cache (codeHash → code) is preserved since code hashes are immutable.
// Returns true if the cache was valid (hashes matched or cache was empty), false if cleared.
func (c *CodeCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Empty blockHash means cache hasn't processed any block yet - always valid
	if c.blockHash == (common.Hash{}) {
		c.addrToHash.Purge()
		c.blockHash = incomingBlockHash
		return true
	}

	// Check if we're continuing from the expected block
	if c.blockHash == parentHash {
		c.blockHash = incomingBlockHash
		return true
	}

	// Mismatch - clear address mappings (they may be stale)
	// Keep code mappings since codeHash → code is immutable
	c.addrToHash.Purge()
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the address cache and sets the block hash.
// Used during unwind to reset the cache to a known state.
func (c *CodeCache) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrToHash.Purge()
	c.blockHash = hash
}

// Len returns the number of entries in the address cache.
func (c *CodeCache) Len() int {
	return c.addrToHash.Len()
}

// CodeLen returns the number of entries in the code cache.
func (c *CodeCache) CodeLen() int {
	return c.hashToCode.Len()
}

// PrintStatsAndReset prints cache statistics and resets counters.
// Call this at the end of each block to see per-block performance.
func (c *CodeCache) PrintStatsAndReset(logger log.Logger) {
	addrHits := c.addrHits.Swap(0)
	addrMisses := c.addrMisses.Swap(0)
	codeHits := c.codeHits.Swap(0)
	codeMisses := c.codeMisses.Swap(0)

	addrTotal := addrHits + addrMisses
	codeTotal := codeHits + codeMisses

	var addrHitRate, codeHitRate float64
	if addrTotal > 0 {
		addrHitRate = float64(addrHits) / float64(addrTotal) * 100
	}
	if codeTotal > 0 {
		codeHitRate = float64(codeHits) / float64(codeTotal) * 100
	}

	logger.Debug("CodeCache stats",
		"addr_hits", addrHits,
		"addr_misses", addrMisses,
		"addr_hit_rate", addrHitRate,
		"code_hits", codeHits,
		"code_misses", codeMisses,
		"code_hit_rate", codeHitRate,
		"addr_cache_size", c.addrToHash.Len(),
		"code_cache_size", c.hashToCode.Len(),
	)
}
