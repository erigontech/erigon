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
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
)

// uint64AsBytes returns a []byte view of a uint64 without allocation.
func uint64AsBytes(v *uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 8)
}

const (
	// DefaultCodeCacheBytes is the byte limit for code cache (100 MB — investigation knob; permanent default returns to 512 MB)
	DefaultCodeCacheBytes = 100 * datasize.MB
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
// Level 2b: codeHash(code) → code (immutable, never cleared) — enables
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
// - codeHashToCode lets callers with the codeHash in hand skip L1 entirely
// - Thread-safe via sync.Map
//
// Capacity is byte-based. Once full, new puts are no-ops but
// modifications to existing entries and deletions are still allowed.

// Every cached layer carries (txNum, epoch) so an unwind invalidates code the
// same way as the account/storage/branch caches (#21752): a contract's code
// value never changes for a given hash, but its EXISTENCE does — code deployed
// on a fork that is later unwound must no longer be discoverable, even by
// codeHash. So the content-addressed layers are NOT treated as immutable; they
// honor the same (txNum, epoch) lazy-drop as the addr layers. This can re-fetch
// code shared across deployments when one is unwound (a multiplicity cost), but
// keeps stale code out of the cache.
type versionedAddressID struct {
	addrID uint64
	txNum  uint64
	epoch  uint32
}

type addrCodeHashEntry struct {
	hash  [32]byte
	txNum uint64
	epoch uint32
}

type codeEntry struct {
	code  []byte
	txNum uint64
	epoch uint32
}

type codeSizeEntry struct {
	size  int
	txNum uint64
	epoch uint32
}

type CodeCache struct {
	// addrToHash maps a 20-byte Ethereum address to the maphash-derived
	// codeID for the code at that address. Real LRU (was a no-op-when-full
	// maphash.Map until commit 7d0998d0db) — fresh-address workloads now
	// evict oldest entries and warm up the working set, matching geth's
	// lru.Cache pattern at core/state/database_code.go.
	addrToHash *lru.Cache[[20]byte, versionedAddressID]
	hashToCode *maphash.Map[codeEntry] // maphash(code) → code, concurrent
	codeSize   atomic.Int64            // current size in bytes (code only, hash is fixed 8 bytes)

	// addrToCodeHash maps a 20-byte address to its 32-byte Ethereum codeHash
	// (keccak), separately from addrToHash (which uses the cheap maphash
	// for bytes-lookup chaining). Used by SharedDomains.codeHashForAddr to
	// skip a cold account-domain read when the EVM-known codeHash is
	// already in cache. Nethermind-style addr → codeHash LRU.
	addrToCodeHash *lru.Cache[[20]byte, addrCodeHashEntry]

	// codeHashToCode: 32-byte Ethereum codeHash (keccak256) → code bytes. Populated
	// alongside L2 when the caller provides codeHash on Put. Independent
	// of L1 — Get-by-codeHash bypasses addr lookup entirely. Memory cost:
	// duplicates code bytes vs L2 (worst case 2x byte storage); accepted
	// for the per-key fast-path on many-addrs-one-code workloads.
	codeHashToCode   *maphash.Map[codeEntry] // keccak(code) → code, concurrent
	codeHashCodeSize atomic.Int64            // current size in bytes (codeHash layer)

	// Size-only layer: ethCodeHash → int (length in bytes). Answers
	// EXTCODESIZE / EXTCODEHASH without loading the bytes. Geth has the
	// equivalent at core/state/database_code.go (1 M-entry LRU). Tiny
	// per-entry footprint (32B key + 8B value) so the same memory budget
	// gives ~1000x the hit surface vs the bytes cache.
	codeSizeByCodeHash *maphash.Map[codeSizeEntry]
	codeSizeEntries    atomic.Int64
	codeSizeCapEntries int64

	// Unwind coherence — see GenericCache. An entry is valid iff it was written
	// in the current epoch OR its txNum is below unwindFloor. Unwind bumps the
	// epoch and lowers the floor (O(1), no scan); stale entries drop lazily on
	// their next Get. Applies to every layer, content-addressed ones included.
	epoch       atomic.Uint32
	unwindFloor atomic.Uint64

	// Stats counters (atomic for concurrent access)
	addrHits       atomic.Uint64
	addrMisses     atomic.Uint64
	codeHits       atomic.Uint64
	codeMisses     atomic.Uint64
	codeHashHits   atomic.Uint64
	codeHashMisses atomic.Uint64
	codeSizeHits   atomic.Uint64
	codeSizeMisses atomic.Uint64

	addrCapacityB datasize.ByteSize // capacity in bytes
	codeCapacityB datasize.ByteSize // capacity in bytes
}

// isStale reports whether an entry stamped (txNum, epoch) reflects dead-fork
// state after an unwind: it was written in a superseded epoch and its txNum is
// at or above the unwind floor (the first unwound txNum). Mirrors GenericCache.
func (c *CodeCache) isStale(txNum uint64, epoch uint32) bool {
	return epoch != c.epoch.Load() && txNum >= c.unwindFloor.Load()
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
	addrCodeHashLRU, err := lru.New[[20]byte, addrCodeHashEntry](addrEntries)
	if err != nil {
		panic(err)
	}
	cc := &CodeCache{
		addrToHash:         addrLRU,
		addrToCodeHash:     addrCodeHashLRU,
		hashToCode:         maphash.NewMap[codeEntry](),
		codeHashToCode:     maphash.NewMap[codeEntry](),
		codeSizeByCodeHash: maphash.NewMap[codeSizeEntry](),
		codeSizeCapEntries: DefaultCodeSizeCacheEntries,
		addrCapacityB:      addrCapacityBytes,
		codeCapacityB:      codeCapacityBytes,
	}
	// Before any unwind every entry's txNum is below the floor, so the epoch
	// check never strands a valid entry.
	cc.unwindFloor.Store(math.MaxUint64)
	return cc
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
	if c.isStale(vID.txNum, vID.epoch) {
		c.addrToHash.Remove(k)
		c.addrMisses.Add(1)
		return nil, false
	}
	c.addrHits.Add(1)

	hashKey := uint64AsBytes(&vID.addrID)
	ce, ok := c.hashToCode.Get(hashKey)
	if !ok || len(ce.code) == 0 {
		c.codeMisses.Add(1)
		return nil, false
	}
	if c.isStale(ce.txNum, ce.epoch) {
		c.hashToCode.Delete(hashKey)
		c.codeSize.Add(-int64(8 + len(ce.code)))
		c.codeMisses.Add(1)
		return nil, false
	}
	c.codeHits.Add(1)
	return ce.code, true
}

// Put stores contract code for the given address, implementing the Cache interface.
// Uses fast maphash to compute the code identifier. addrToHash is an LRU; the
// hashToCode bytes are content-addressed (immutable for a hash) but carry a
// (txNum, epoch) stamp so an unwound deployment's code stops being discoverable.
func (c *CodeCache) Put(addr []byte, code []byte, txNum uint64) {
	if len(code) == 0 {
		return
	}
	ep := c.epoch.Load()
	codeHash := maphash.Hash(code)

	c.addrToHash.Add(addrKey(addr), versionedAddressID{addrID: codeHash, txNum: txNum, epoch: ep})

	hashKey := uint64AsBytes(&codeHash)
	if existing, exists := c.hashToCode.Get(hashKey); exists {
		// Bytes are immutable, but revive an entry stranded stale by a prior
		// unwind when the same code is re-deployed on the live fork.
		if c.isStale(existing.txNum, existing.epoch) {
			c.hashToCode.Set(hashKey, codeEntry{code: existing.code, txNum: txNum, epoch: ep})
		}
		return
	}
	codeEntrySize := int64(8 + len(code))
	if c.codeSize.Load()+codeEntrySize > int64(c.codeCapacityB) {
		return
	}
	c.hashToCode.Set(hashKey, codeEntry{code: code, txNum: txNum, epoch: ep})
	c.codeSize.Add(codeEntrySize)
}

// GetAddrCodeHash returns the Ethereum codeHash for addr if cached.
// Nethermind-style lookup that lets SharedDomains.codeHashForAddr skip a
// cold AccountsDomain read when the EVM-known codeHash is already known.
// Eviction is LRU; freshly seen addrs replace coldest entries.
func (c *CodeCache) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	k := addrKey(addr)
	e, ok := c.addrToCodeHash.Get(k)
	if !ok {
		return [32]byte{}, false
	}
	if c.isStale(e.txNum, e.epoch) {
		c.addrToCodeHash.Remove(k)
		return [32]byte{}, false
	}
	return e.hash, true
}

// PutAddrCodeHash records the addr → codeHash mapping. Called from the
// account-decode populate path inside SD.codeHashForAddr; also called by
// readAhead's BAL prefetch when it learns the codeHash from the decoded
// account record. txNum stamps the mapping for unwind invalidation.
func (c *CodeCache) PutAddrCodeHash(addr []byte, h [32]byte, txNum uint64) {
	c.addrToCodeHash.Add(addrKey(addr), addrCodeHashEntry{hash: h, txNum: txNum, epoch: c.epoch.Load()})
}

// DeleteAddrCodeHash drops the addr → codeHash mapping. Called on
// SELFDESTRUCT / CREATE2-replace / unwind where the account's codeHash
// has been mutated.
func (c *CodeCache) DeleteAddrCodeHash(addr []byte) {
	c.addrToCodeHash.Remove(addrKey(addr))
}

// GetByCodeHash retrieves contract code by its Ethereum codeHash (keccak256).
// Bypasses the addr-keyed L1/L2 path. Returns (code, true) on hit, (nil, false) on miss.
//
// Designed for the common path where the caller has already loaded the
// account and knows the codeHash (EXTCODESIZE, EXTCODEHASH, CALL targets
// after account-load). Many addresses sharing one codeHash all hit this
// single codeHashToCode entry after the first population.
func (c *CodeCache) GetByCodeHash(codeHash []byte) ([]byte, bool) {
	ce, ok := c.codeHashToCode.Get(codeHash)
	if !ok || len(ce.code) == 0 {
		c.codeHashMisses.Add(1)
		return nil, false
	}
	if c.isStale(ce.txNum, ce.epoch) {
		c.codeHashToCode.Delete(codeHash)
		c.codeHashCodeSize.Add(-int64(len(codeHash) + len(ce.code)))
		c.codeHashMisses.Add(1)
		return nil, false
	}
	c.codeHashHits.Add(1)
	return ce.code, true
}

// PutWithCodeHash stores contract code, populating both the addr-keyed
// path (L1+L2) and the codeHash-keyed path (codeHashToCode). Use when the caller
// has the codeHash in hand (typically from a just-loaded account record);
// avoids the maphash-vs-keccak collision risk of re-deriving the codeHash
// from the value, and ensures codeHashToCode is fillable without an extra keccak.
//
// addr may be empty to populate only codeHashToCode (e.g. when populating from a
// codehash-only path that hasn't seen the addr yet).
func (c *CodeCache) PutWithCodeHash(addr []byte, code []byte, codeHash []byte, txNum uint64) {
	if len(code) == 0 || len(codeHash) == 0 {
		return
	}
	ep := c.epoch.Load()

	if len(addr) > 0 {
		c.Put(addr, code, txNum)
	}

	// Populate the size-only layer alongside the bytes layer — every time
	// we touch the bytes we can answer a future EXTCODESIZE for free.
	c.PutCodeSizeByCodeHash(codeHash, len(code), txNum)

	if existing, exists := c.codeHashToCode.Get(codeHash); exists {
		// Immutable bytes; revive an entry stranded stale by a prior unwind when
		// the same code is re-deployed on the live fork.
		if c.isStale(existing.txNum, existing.epoch) {
			c.codeHashToCode.Set(codeHash, codeEntry{code: existing.code, txNum: txNum, epoch: ep})
		}
		return
	}
	entrySize := int64(len(codeHash) + len(code))
	if c.codeHashCodeSize.Load()+entrySize > int64(c.codeCapacityB) {
		return
	}
	c.codeHashToCode.Set(codeHash, codeEntry{code: code, txNum: txNum, epoch: ep})
	c.codeHashCodeSize.Add(entrySize)
}

// GetCodeSizeByCodeHash retrieves the size (in bytes) of a contract by its
// Ethereum codeHash, without loading the bytes. Returns (0, false) on miss.
//
// Designed for EXTCODESIZE / EXTCODEHASH which only need the length; on a
// cache hit the caller answers a 4-instruction map probe instead of paying
// the file-accessor + decompression stack for the full bytes. Geth has the
// equivalent at core/state/database_code.go.
func (c *CodeCache) GetCodeSizeByCodeHash(codeHash []byte) (int, bool) {
	e, ok := c.codeSizeByCodeHash.Get(codeHash)
	if !ok {
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	if c.isStale(e.txNum, e.epoch) {
		c.codeSizeByCodeHash.Delete(codeHash)
		c.codeSizeEntries.Add(-1)
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	c.codeSizeHits.Add(1)
	return e.size, true
}

// PutCodeSizeByCodeHash stores the size of code keyed by its Ethereum
// codeHash. No-op when full (limitation; addrToHash-style LRU is queued as
// a separate surgical change). txNum stamps the entry for unwind invalidation.
func (c *CodeCache) PutCodeSizeByCodeHash(codeHash []byte, size int, txNum uint64) {
	if len(codeHash) == 0 || size < 0 {
		return
	}
	ep := c.epoch.Load()
	if existing, exists := c.codeSizeByCodeHash.Get(codeHash); exists {
		if c.isStale(existing.txNum, existing.epoch) {
			c.codeSizeByCodeHash.Set(codeHash, codeSizeEntry{size: existing.size, txNum: txNum, epoch: ep})
		}
		return
	}
	if c.codeSizeEntries.Load() >= c.codeSizeCapEntries {
		return
	}
	c.codeSizeByCodeHash.Set(codeHash, codeSizeEntry{size: size, txNum: txNum, epoch: ep})
	c.codeSizeEntries.Add(1)
}

// Delete removes the address → code mapping for addr.
func (c *CodeCache) Delete(addr []byte) {
	c.addrToHash.Remove(addrKey(addr))
}

// Clear hard-resets every layer and the epoch/floor. Use on Reset /
// fork-validation paths where no entry may carry over.
func (c *CodeCache) Clear() {
	c.addrToHash.Purge()
	c.addrToCodeHash.Purge()
	c.hashToCode.Clear()
	c.codeHashToCode.Clear()
	c.codeSizeByCodeHash.Clear()
	c.codeSize.Store(0)
	c.codeHashCodeSize.Store(0)
	c.codeSizeEntries.Store(0)
	c.epoch.Store(0)
	c.unwindFloor.Store(math.MaxUint64)
}

// Unwind invalidates entries reflecting dead-fork state. Code deployed on the
// rolled-back fork must stop being discoverable — even by codeHash — because
// although a hash → bytes value is invariant, the code's EXISTENCE is not
// (#21752). O(1) and scan-free: bump the epoch and lower the floor to
// unwindToTxNum; every layer's entries at/above the floor from a superseded
// epoch drop lazily on their next Get (re-fetching code shared with a
// still-live deployment, an accepted multiplicity cost).
func (c *CodeCache) Unwind(unwindToTxNum uint64) {
	c.epoch.Add(1)
	for {
		cur := c.unwindFloor.Load()
		if unwindToTxNum >= cur {
			break
		}
		if c.unwindFloor.CompareAndSwap(cur, unwindToTxNum) {
			break
		}
	}
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
