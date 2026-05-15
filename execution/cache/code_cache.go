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
	lru "github.com/hashicorp/golang-lru/v2"

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
	// DefaultAddrCacheEntries derives from DefaultAddrCacheBytes assuming
	// ~28 bytes per entry (20-byte addr + 8-byte maphash codeID). Used as
	// the LRU entry cap so the cache evicts oldest entries instead of
	// silently dropping new ones when full — fresh-address workloads
	// (e.g. mainnet thousands of new addrs per block) actually warm up
	// over time, matching geth's lru.Cache pattern in
	// core/state/database_code.go.
	DefaultAddrCacheEntries = int(DefaultAddrCacheBytes) / 28
	// DefaultCodeSizeCacheEntries is the max entry count for the size-only
	// cache (geth-style: code size answers without loading bytes for
	// EXTCODESIZE / EXTCODEHASH callers).
	DefaultCodeSizeCacheEntries int64 = 1_000_000
)

// CodeCache is a multi-level concurrent cache for contract code.
// Level 1: addr → maphash(code) (mutable, cleared on reorg)
// Level 2: maphash(code) → code (immutable, never cleared)
// Level 2b: ethHash(code) → code (immutable, never cleared) — enables
//           bypass of the addr level when the caller already knows the
//           Ethereum codeHash from a prior account read (common path
//           for EXTCODESIZE / EXTCODEHASH / CALL where many addresses
//           share the same bytecode — proxies, factory-deployed clones,
//           ERC-20 holders, etc.).
//
// This design is efficient because:
// - Multiple addresses can share the same code (common with proxies/clones)
// - Uses fast maphash instead of cryptographic Keccak256 for L1/L2
// - Address mappings are small (8 bytes) so we can cache many more
// - L2b lets callers with the codeHash in hand skip L1 entirely
// - Thread-safe via sync.Map
//
// Capacity is byte-based. Once full, new puts are no-ops but
// modifications to existing entries and deletions are still allowed.

type versionedAddressID struct {
	addrID uint64
}

type CodeCache struct {
	// addrToHash maps a 20-byte Ethereum address to the maphash-derived
	// codeID for the code at that address. Real LRU (was a no-op-when-full
	// maphash.Map until commit 7d0998d0db) — fresh-address workloads now
	// evict oldest entries and warm up the working set, matching geth's
	// lru.Cache pattern at core/state/database_code.go.
	addrToHash *lru.Cache[[20]byte, versionedAddressID]
	hashToCode *maphash.Map[[]byte] // maphash(code) → code, concurrent
	codeSize   atomic.Int64         // current size in bytes (code only, hash is fixed 8 bytes)

	// addrToEthHash maps a 20-byte address to its 32-byte Ethereum codeHash
	// (keccak), separately from addrToHash (which uses the cheap maphash
	// for bytes-lookup chaining). Used by SharedDomains.codeHashForAddr to
	// skip a cold account-domain read when the EVM-known codeHash is
	// already in cache. Nethermind-style addr → codeHash LRU.
	addrToEthHash *lru.Cache[[20]byte, [32]byte]

	// L2b: 32-byte Ethereum codeHash (keccak256) → code bytes. Populated
	// alongside L2 when the caller provides ethHash on Put. Independent
	// of L1 — Get-by-ethHash bypasses addr lookup entirely. Memory cost:
	// duplicates code bytes vs L2 (worst case 2x byte storage); accepted
	// for the per-key fast-path on many-addrs-one-code workloads.
	ethHashToCode    *maphash.Map[[]byte] // keccak(code) → code, concurrent
	ethHashCodeSize  atomic.Int64         // current size in bytes (ethHash layer)

	// Size-only layer: ethCodeHash → int (length in bytes). Answers
	// EXTCODESIZE / EXTCODEHASH without loading the bytes. Geth has the
	// equivalent at core/state/database_code.go (1 M-entry LRU). Tiny
	// per-entry footprint (32B key + 8B value) so the same memory budget
	// gives ~1000x the hit surface vs the bytes cache.
	codeSizeByEthHash    *maphash.Map[int]
	codeSizeEntries      atomic.Int64
	codeSizeCapEntries   int64

	blockHash common.Hash // hash of the last block processed
	mu        sync.RWMutex

	// Stats counters (atomic for concurrent access)
	addrHits        atomic.Uint64
	addrMisses      atomic.Uint64
	codeHits        atomic.Uint64
	codeMisses      atomic.Uint64
	ethHashHits     atomic.Uint64
	ethHashMisses   atomic.Uint64
	codeSizeHits    atomic.Uint64
	codeSizeMisses  atomic.Uint64

	addrCapacityB datasize.ByteSize // capacity in bytes
	codeCapacityB datasize.ByteSize // capacity in bytes
}

// NewCodeCache creates a new CodeCache with the specified byte capacities.
func NewCodeCache(codeCapacityBytes, addrCapacityBytes datasize.ByteSize) *CodeCache {
	addrEntries := int(addrCapacityBytes) / 28
	if addrEntries < 1024 {
		addrEntries = 1024
	}
	addrLRU, err := lru.New[[20]byte, versionedAddressID](addrEntries)
	if err != nil {
		panic(err)
	}
	addrEthHashLRU, err := lru.New[[20]byte, [32]byte](addrEntries)
	if err != nil {
		panic(err)
	}
	return &CodeCache{
		addrToHash:         addrLRU,
		addrToEthHash:      addrEthHashLRU,
		hashToCode:         maphash.NewMap[[]byte](),
		ethHashToCode:      maphash.NewMap[[]byte](),
		codeSizeByEthHash:  maphash.NewMap[int](),
		codeSizeCapEntries: DefaultCodeSizeCacheEntries,
		addrCapacityB:      addrCapacityBytes,
		codeCapacityB:      codeCapacityBytes,
	}
}

// addrKey casts a 20-byte slice to a fixed-size key without allocation.
// Caller MUST pass a 20-byte slice (all Ethereum addresses are 20 bytes).
// Returns the zero [20]byte if addr is shorter; only longer slices are
// truncated silently — defensive but should not happen on the hot path.
func addrKey(addr []byte) [20]byte {
	var k [20]byte
	if len(addr) >= 20 {
		copy(k[:], addr[:20])
	} else {
		copy(k[:], addr)
	}
	return k
}

// NewDefaultCodeCache creates a new CodeCache with the default sizes.
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheBytes, DefaultAddrCacheBytes)
}

// Get retrieves contract code for the given address, implementing the Cache interface.
func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	k := addrKey(addr)
	vID, ok := c.addrToHash.Get(k)
	if !ok || vID.addrID == 0 {
		c.addrMisses.Add(1)
		return nil, false
	}
	c.addrHits.Add(1)

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
// addrToHash is an LRU (auto-evicts oldest when full); hashToCode is byte-capped
// and no-ops on new writes when full (code is immutable, so existing entries stay).
func (c *CodeCache) Put(addr []byte, code []byte) {
	if len(code) == 0 {
		return
	}

	codeHash := maphash.Hash(code)

	c.addrToHash.Add(addrKey(addr), versionedAddressID{addrID: codeHash})

	hashKey := uint64AsBytes(&codeHash)
	if _, exists := c.hashToCode.Get(hashKey); exists {
		return
	}
	codeEntrySize := int64(8 + len(code))
	if c.codeSize.Load()+codeEntrySize > int64(c.codeCapacityB) {
		return
	}
	c.hashToCode.Set(hashKey, code)
	c.codeSize.Add(codeEntrySize)
}

// GetAddrCodeHash returns the Ethereum codeHash for addr if cached.
// Nethermind-style lookup that lets SharedDomains.codeHashForAddr skip a
// cold AccountsDomain read when the EVM-known codeHash is already known.
// Eviction is LRU; freshly seen addrs replace coldest entries.
func (c *CodeCache) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	h, ok := c.addrToEthHash.Get(addrKey(addr))
	return h, ok
}

// PutAddrCodeHash records the addr → codeHash mapping. Called from the
// account-decode populate path inside SD.codeHashForAddr; also called by
// readAhead's BAL prefetch when it learns the codeHash from the decoded
// account record.
func (c *CodeCache) PutAddrCodeHash(addr []byte, h [32]byte) {
	c.addrToEthHash.Add(addrKey(addr), h)
}

// DeleteAddrCodeHash drops the addr → codeHash mapping. Called on
// SELFDESTRUCT / CREATE2-replace / unwind where the account's codeHash
// has been mutated.
func (c *CodeCache) DeleteAddrCodeHash(addr []byte) {
	c.addrToEthHash.Remove(addrKey(addr))
}

// GetByEthHash retrieves contract code by its Ethereum codeHash (keccak256).
// Bypasses the addr-keyed L1/L2 path. Returns (code, true) on hit, (nil, false) on miss.
//
// Designed for the common path where the caller has already loaded the
// account and knows the codeHash (EXTCODESIZE, EXTCODEHASH, CALL targets
// after account-load). Many addresses sharing one codeHash all hit this
// single L2b entry after the first population.
func (c *CodeCache) GetByEthHash(ethHash []byte) ([]byte, bool) {
	code, ok := c.ethHashToCode.Get(ethHash)
	if !ok || len(code) == 0 {
		c.ethHashMisses.Add(1)
		return nil, false
	}
	c.ethHashHits.Add(1)
	return code, true
}

// PutWithEthHash stores contract code, populating both the addr-keyed
// path (L1+L2) and the ethHash-keyed path (L2b). Use when the caller
// has the codeHash in hand (typically from a just-loaded account record);
// avoids the maphash-vs-keccak collision risk of re-deriving the ethHash
// from the value, and ensures L2b is fillable without an extra keccak.
//
// addr may be empty to populate only L2b (e.g. when populating from a
// codehash-only path that hasn't seen the addr yet).
func (c *CodeCache) PutWithEthHash(addr []byte, code []byte, ethHash []byte) {
	if len(code) == 0 || len(ethHash) == 0 {
		return
	}

	if len(addr) > 0 {
		c.Put(addr, code)
	}

	// Populate the size-only layer alongside the bytes layer — every time
	// we touch the bytes we can answer a future EXTCODESIZE for free.
	c.PutCodeSizeByEthHash(ethHash, len(code))

	if _, exists := c.ethHashToCode.Get(ethHash); exists {
		return
	}
	entrySize := int64(len(ethHash) + len(code))
	if c.ethHashCodeSize.Load()+entrySize > int64(c.codeCapacityB) {
		return
	}
	c.ethHashToCode.Set(ethHash, code)
	c.ethHashCodeSize.Add(entrySize)
}

// GetCodeSizeByEthHash retrieves the size (in bytes) of a contract by its
// Ethereum codeHash, without loading the bytes. Returns (0, false) on miss.
//
// Designed for EXTCODESIZE / EXTCODEHASH which only need the length; on a
// cache hit the caller answers a 4-instruction map probe instead of paying
// the file-accessor + decompression stack for the full bytes. Geth has the
// equivalent at core/state/database_code.go.
func (c *CodeCache) GetCodeSizeByEthHash(ethHash []byte) (int, bool) {
	size, ok := c.codeSizeByEthHash.Get(ethHash)
	if !ok {
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	c.codeSizeHits.Add(1)
	return size, true
}

// PutCodeSizeByEthHash stores the size of code keyed by its Ethereum
// codeHash. No-op when full (limitation; addrToHash-style LRU is queued as
// a separate surgical change).
func (c *CodeCache) PutCodeSizeByEthHash(ethHash []byte, size int) {
	if len(ethHash) == 0 || size < 0 {
		return
	}
	if _, exists := c.codeSizeByEthHash.Get(ethHash); exists {
		return
	}
	if c.codeSizeEntries.Load() >= c.codeSizeCapEntries {
		return
	}
	c.codeSizeByEthHash.Set(ethHash, size)
	c.codeSizeEntries.Add(1)
}

// Delete removes the address → codeHash mapping.
// The codeHash → code mapping is kept since it's immutable.
func (c *CodeCache) Delete(addr []byte) {
	c.addrToHash.Remove(addrKey(addr))
}

// Clear removes all address mappings from the cache.
// The codeHash → code mappings are preserved since they're immutable.
func (c *CodeCache) Clear() {
	c.addrToHash.Purge()
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
	// Keep code mappings since hash → code is immutable
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

// AddrSizeBytes returns the estimated size of the address cache in bytes.
// LRU-based; estimate uses ~28 bytes per entry (20-byte addr + 8-byte codeID).
func (c *CodeCache) AddrSizeBytes() int64 {
	return int64(c.addrToHash.Len() * 28)
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

	addrSizeB := c.AddrSizeBytes()
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
