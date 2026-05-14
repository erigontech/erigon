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
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

// uint64AsBytes returns a []byte view of a uint64 without allocation.
func uint64AsBytes(v *uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

const (
	// DefaultCodeCacheBytes is the byte limit for code cache (512 MB)
	DefaultCodeCacheBytes = 512 * datasize.MB
	// DefaultAddrCacheBytes is the byte limit for address cache (16 MB)
	DefaultAddrCacheBytes = 16 * datasize.MB
)

// CodeCache is a two-level concurrent cache for contract code.
// Level 1: addr → maphash(code) (mutable, cleared on reorg)
// Level 2: maphash(code) → code (immutable, never cleared)
//
// This design is efficient because:
// - Multiple addresses can share the same code (common with proxies/clones)
// - Uses fast maphash instead of cryptographic Keccak256
// - Address mappings are small (8 bytes) so we can cache many more
// - Thread-safe via sync.Map
//
// Capacity is byte-based. Once full, new puts are no-ops but
// modifications to existing entries and deletions are still allowed.

type versionedAddressID struct {
	addrID uint64
}

type CodeCache struct {
	addrToHash *maphash.Map[versionedAddressID] // addr → maphash(code), concurrent
	addrSize   atomic.Int64                     // current size in bytes
	hashToCode *maphash.Map[[]byte]             // maphash(code) → code, concurrent
	codeSize   atomic.Int64                     // current size in bytes (code only, hash is fixed 8 bytes)
	blockHash  common.Hash                      // hash of the last block processed
	mu         sync.RWMutex

	// Stats counters (atomic for concurrent access)
	addrHits   atomic.Uint64
	addrMisses atomic.Uint64
	codeHits   atomic.Uint64
	codeMisses atomic.Uint64

	addrCapacityB datasize.ByteSize // capacity in bytes
	codeCapacityB datasize.ByteSize // capacity in bytes
}

// NewCodeCache creates a new CodeCache with the specified byte capacities.
func NewCodeCache(codeCapacityBytes, addrCapacityBytes datasize.ByteSize) *CodeCache {
	return &CodeCache{
		addrToHash:    maphash.NewMap[versionedAddressID](),
		hashToCode:    maphash.NewMap[[]byte](),
		addrCapacityB: addrCapacityBytes,
		codeCapacityB: codeCapacityBytes,
	}
}

// NewDefaultCodeCache creates a new CodeCache with the default sizes.
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheBytes, DefaultAddrCacheBytes)
}

// Get retrieves contract code for the given address, implementing the Cache interface.
func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	// First, look up the code hash for this address
	vID, ok := c.addrToHash.Get(addr)
	if !ok || vID.addrID == 0 {
		c.addrMisses.Add(1)
		return nil, false
	}
	c.addrHits.Add(1)

	// Then, look up the code by hash
	code, ok := c.hashToCode.Get(uint64AsBytes(&vID.addrID))
	if !ok || len(code) == 0 {
		c.codeMisses.Add(1)
		return nil, false
	}
	c.codeHits.Add(1)
	return code, true
}

// Put stores contract code for the given address, implementing the Cache interface.
// Uses fast maphash to compute the code identifier.
// If caches are at capacity, new entries are no-ops but updates are allowed.
func (c *CodeCache) Put(addr []byte, code []byte) {
	if len(code) == 0 {
		return
	}

	codeHash := maphash.Hash(code)
	addrEntrySize := int64(len(addr) + 8) // addr + uint64 hash

	// Check if addr already exists - updates are always allowed
	if _, exists := c.addrToHash.Get(addr); exists {
		c.addrToHash.Set(addr, versionedAddressID{addrID: codeHash})
	} else if c.addrSize.Load()+addrEntrySize <= int64(c.addrCapacityB) {
		c.addrToHash.Set(addr, versionedAddressID{addrID: codeHash})
		c.addrSize.Add(addrEntrySize)
	}

	// Check if code already stored
	hashKey := uint64AsBytes(&codeHash)
	if _, exists := c.hashToCode.Get(hashKey); exists {
		return
	}

	// New code entry - check capacity
	codeEntrySize := int64(8 + len(code)) // hash + code
	if c.codeSize.Load()+codeEntrySize > int64(c.codeCapacityB) {
		return // no-op when full
	}
	c.hashToCode.Set(hashKey, code)
	c.codeSize.Add(codeEntrySize)
}

// Delete removes the address → codeHash mapping.
// The codeHash → code mapping is kept since it's immutable.
func (c *CodeCache) Delete(addr []byte) {
	if _, exists := c.addrToHash.Get(addr); exists {
		addrEntrySize := int64(len(addr) + 8)
		c.addrToHash.Delete(addr)
		c.addrSize.Add(-addrEntrySize)
	}
}

// Clear removes all address mappings from the cache.
// The codeHash → code mappings are preserved since they're immutable.
func (c *CodeCache) Clear() {
	c.addrToHash.Clear()
	c.addrSize.Store(0)
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
	c.blockHash = hash
	c.mu.Unlock()
}

// ValidateAndPrepare checks if the given parentHash matches the cache's current blockHash.
// If there's a mismatch (indicating non-sequential block processing), the address cache is cleared.
// The code cache (hash → code) is preserved since code hashes are immutable.
// Returns true if the cache was valid (hashes matched or cache was empty), false if cleared.
func (c *CodeCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Empty blockHash means cache hasn't processed any block yet - always valid
	if c.blockHash == (common.Hash{}) {
		c.addrToHash.Clear()
		c.addrSize.Store(0)
		c.blockHash = incomingBlockHash
		return true
	}

	// Check if we're continuing from the expected block
	if c.blockHash == parentHash {
		c.blockHash = incomingBlockHash
		return true
	}

	// Mismatch - clear address mappings (they may be stale)
	// Keep code mappings since hash → code is immutable
	c.addrToHash.Clear()
	c.addrSize.Store(0)
	c.blockHash = incomingBlockHash
	return false
}

// ClearWithHash clears the address cache and sets the block hash.
// Used during unwind to reset the cache to a known state.
func (c *CodeCache) ClearWithHash(hash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrToHash.Clear()
	c.addrSize.Store(0)
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

// AddrSizeBytes returns the current size of the address cache in bytes.
func (c *CodeCache) AddrSizeBytes() int64 {
	return c.addrSize.Load()
}

// CodeSizeBytes returns the current size of the code cache in bytes.
func (c *CodeCache) CodeSizeBytes() int64 {
	return c.codeSize.Load()
}

// PrintStatsAndReset prints cache statistics and resets counters.
// Call this at the end of each block to see per-block performance.
func (c *CodeCache) PrintStatsAndReset() {
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

	addrSizeB := c.addrSize.Load()
	codeSizeB := c.codeSize.Load()
	addrUsagePct := float64(addrSizeB) / float64(c.addrCapacityB) * 100
	codeUsagePct := float64(codeSizeB) / float64(c.codeCapacityB) * 100

	log.Debug("CodeCache stats",
		"addr_hits", addrHits,
		"addr_misses", addrMisses,
		"addr_hit_rate", addrHitRate,
		"code_hits", codeHits,
		"code_misses", codeMisses,
		"code_hit_rate", codeHitRate,
		"addr_entries", c.addrToHash.Len(),
		"code_entries", c.CodeLen(),
		"addr_size_mb", addrSizeB/(1024*1024),
		"addr_usage_pct", addrUsagePct,
		"code_size_mb", codeSizeB/(1024*1024),
		"code_usage_pct", codeUsagePct,
	)
}
