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
	"github.com/erigontech/erigon/db/kv"
)

// hash32 copies a codeHash slice into a fixed [32]byte for storage/compare.
func hash32(b []byte) [32]byte {
	var h [32]byte
	copy(h[:], b)
	return h
}

const (
	// DefaultCodeCacheBytes is the byte limit for the code cache.
	DefaultCodeCacheBytes = 512 * datasize.MB
	// DefaultAddrCacheBytes is the byte limit for address cache (16 MB)
	DefaultAddrCacheBytes = 16 * datasize.MB
	// DefaultCodeSizeCacheEntries is the max entry count for the size-only
	// cache (code size answers without loading bytes for
	// EXTCODESIZE / EXTCODEHASH callers).
	DefaultCodeSizeCacheEntries int64 = 1_000_000
	// avgCodeEntryBytes translates the code byte budget into the freelru
	// entry-count cap (the only bound — the byte counters don't evict). Sized to
	// the resident-code skew (hot contracts run 10-24 KB) rather than the raw
	// average so the cap keeps RAM near the budget instead of several × over; the
	// persistent (MDBX-backed) cold tier backstops entries the tighter cap evicts.
	avgCodeEntryBytes = 12 * 1024
	// codeSizeEntryBytes is the resident cost of one size-layer slot (freelru
	// element holding size/keyHash), used to map the size-layer entry
	// ceiling to an envelope byte budget.
	codeSizeEntryBytes = 48
)

type addressCodeID struct {
	addrID uint64
	// codeHash is the addr's keccak codeHash, used to reject a hashToCode
	// maphash collision (a different contract whose code collides on the
	// 64-bit maphash key). Zero when populated without a known codeHash.
	codeHash [32]byte
	step     kv.Step
}

type addrCodeHashEntry struct {
	hash [32]byte
}

// Per-entry residency of the two addr-keyed LRUs: a 20-byte key plus the
// value struct (which carries codeHash, not just an 8-byte ID).
// Used both to size the LRUs against the byte budget and to report residency.
const (
	addrToHashEntryBytes     = 20 + int(unsafe.Sizeof(addressCodeID{}))
	addrToCodeHashEntryBytes = 20 + int(unsafe.Sizeof(addrCodeHashEntry{}))
	addrEntryBytes           = addrToHashEntryBytes + addrToCodeHashEntryBytes
)

type codeEntry struct {
	code []byte
	// keyHash is the keccak codeHash this entry is keyed under. maphash.Map
	// collapses the key to a 64-bit hash and discards the bytes, so Get must
	// compare keyHash against the requested key to reject a collision serving
	// a different contract's code.
	keyHash [32]byte
}

type codeSizeEntry struct {
	size int
	// keyHash — see codeEntry.keyHash.
	keyHash [32]byte
}

// CodeCache is a multi-level concurrent cache for contract code, keyed by the
// cheap maphash rather than Keccak256 so many shared-code addresses cost little:
//   - L1  addr → maphash(code)
//   - L2  maphash(code) → code
//   - L2b codeHash(code) → code — lets a caller that already knows the Ethereum
//     codeHash (EXTCODESIZE/EXTCODEHASH/CALL after an account read) skip L1.
//
// Configured byte budgets are translated into LRU entry caps; full layers
// evict their coldest entries.
//
// StateCache publishes these layers as one generation. A canonical unwind
// clears the complete CodeCache before the new generation becomes visible.
type CodeCache struct {
	// addrToHash maps a 20-byte Ethereum address to the maphash-derived
	// codeID for the code at that address. An LRU so fresh-address workloads
	// evict oldest entries and warm up the working set.
	addrToHash *lru.Cache[common.Address, addressCodeID]
	hashToCode *growLRU[codeEntry] // codeID(maphash(code)) → code, jump-grow + LRU-evicting
	codeSize   atomic.Int64        // resident bytes (stat; hard bound is the entry cap)

	// addrToCodeHash maps a 20-byte address to its 32-byte Ethereum codeHash
	// (keccak), separately from addrToHash (which uses the cheap maphash
	// for bytes-lookup chaining). Used by SharedDomains.codeHashForAddr to
	// skip a cold account-domain read when the EVM-known codeHash is
	// already in cache. An addr → codeHash LRU.
	addrToCodeHash *lru.Cache[common.Address, addrCodeHashEntry]

	// codeHashToCode: 32-byte Ethereum codeHash (keccak256) → code bytes. Populated
	// alongside L2 when the caller provides codeHash on Put. Independent
	// of L1 — Get-by-codeHash bypasses addr lookup entirely. Memory cost:
	// duplicates code bytes vs L2 (worst case 2x byte storage); accepted
	// for the per-key fast-path on many-addrs-one-code workloads.
	codeHashToCode   *growLRU[codeEntry] // keccak(code) → code, jump-grow + LRU-evicting
	codeHashCodeSize atomic.Int64        // resident bytes (stat; hard bound is the entry cap)

	// Size-only layer: ethCodeHash → int (length in bytes). Answers
	// EXTCODESIZE / EXTCODEHASH without loading the bytes. Tiny per-entry
	// footprint (32B key + 8B value) so the same memory budget gives ~1000x
	// the hit surface vs the bytes cache.
	codeSizeByCodeHash *growLRU[codeSizeEntry]
	codeSizeEntries    atomic.Int64
	codeSizeCapEntries int64

	// addrBindMu serializes addr→code binding writers so PutIfAbsent's
	// check+bind is atomic w.r.t. a concurrent authoritative rebind.
	addrBindMu sync.Mutex

	// putStripes serializes putContentLocked's membership check, accounting, and
	// insertion per key hash: freelru has no LoadOrStore, so without this two
	// concurrent Puts of the same cold code both miss the check and both add to
	// the byte counter while only one entry survives, drifting the stat upward.
	// A full clear takes every stripe so no put crosses it; puts to distinct keys
	// remain parallel otherwise.
	putStripes [256]sync.Mutex

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

	// closed guards the single paired Close of the content layers so a double
	// Close can't over-return their envelope reservations.
	closed atomic.Bool
}

// putContentLocked is the shared insert path for the content-addressed code layers
// (hashToCode, codeHashToCode, codeSizeByCodeHash). Each is a freelru.ShardedLRU
// of per-key-immutable entries. Existing content is retained; once the cap is
// reached freelru.Add evicts the coldest entry. counter tracks resident bytes
// as a stat; the hard bound is the LRU entry cap. The caller holds the key
// stripe.
func putContentLocked[T any](
	lru *growLRU[T],
	h uint64,
	newEntry T,
	valCost func(T) int64,
	counter *atomic.Int64,
	keyCost int64,
) {
	if _, ok := lru.Get(h); ok {
		return
	}
	counter.Add(keyCost + valCost(newEntry))
	lru.Add(h, newEntry) // evicts the coldest entry when full; its OnEvict decrements counter
}

func codeEntryCodeLen(e codeEntry) int64 { return int64(len(e.code)) }
func zeroCost[T any](T) int64            { return 0 }

// NewCodeCache creates a new CodeCache with the specified byte capacities.
func NewCodeCache(codeCapacityBytes, addrCapacityBytes datasize.ByteSize) *CodeCache {
	// The addr budget is shared by both addr-keyed LRUs, so each "slot" costs
	// addrEntryBytes (both entries combined). Divide in ByteSize space so the
	// budget isn't truncated to int before the division.
	addrEntries := max(int(addrCapacityBytes/datasize.ByteSize(addrEntryBytes)), 1024)
	addrLRU, err := lru.New[common.Address, addressCodeID](addrEntries)
	if err != nil {
		panic(err)
	}
	addrCodeHashLRU, err := lru.New[common.Address, addrCodeHashEntry](addrEntries)
	if err != nil {
		panic(err)
	}
	cc := &CodeCache{
		addrToHash:         addrLRU,
		addrToCodeHash:     addrCodeHashLRU,
		codeSizeCapEntries: DefaultCodeSizeCacheEntries,
		addrCapacityB:      addrCapacityBytes,
		codeCapacityB:      codeCapacityBytes,
	}
	// The content-addressed layers jump-grow from a small start into the shared
	// envelope, so a cache over few contracts (a test fixture) never pre-commits
	// the full budget. OnEvict keeps the byte/entry counters following residency.
	cc.hashToCode = newGrowLRU[codeEntry](codeCapacityBytes, avgCodeEntryBytes,
		func(_ uint64, e codeEntry) { cc.codeSize.Add(-(8 + int64(len(e.code)))) })
	cc.codeHashToCode = newGrowLRU[codeEntry](codeCapacityBytes, avgCodeEntryBytes,
		func(_ uint64, e codeEntry) { cc.codeHashCodeSize.Add(-(32 + int64(len(e.code)))) })
	cc.codeSizeByCodeHash = newGrowLRU[codeSizeEntry](
		datasize.ByteSize(DefaultCodeSizeCacheEntries*codeSizeEntryBytes), codeSizeEntryBytes,
		func(_ uint64, _ codeSizeEntry) { cc.codeSizeEntries.Add(-1) })
	return cc
}

// NewDefaultCodeCache creates a new CodeCache with the default sizes.
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheBytes, DefaultAddrCacheBytes)
}

func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	value, _, ok := c.GetWithStep(addr)
	return value, ok
}

func (c *CodeCache) GetWithStep(addr []byte) ([]byte, kv.Step, bool) {
	k := common.BytesToAddress(addr)
	vID, ok := c.addrToHash.Get(k)
	if !ok {
		c.addrMisses.Add(1)
		return nil, 0, false
	}
	c.addrHits.Add(1)

	ce, ok := c.hashToCode.Get(vID.addrID)
	if !ok || len(ce.code) == 0 {
		c.codeMisses.Add(1)
		return nil, 0, false
	}
	// Reject a 64-bit maphash collision: the stored code belongs to a different
	// contract than addr's. Verifiable only when the addr entry carries a
	// codeHash (always for PutWithCodeHash-populated code; the EVM read path).
	if vID.codeHash != ([32]byte{}) && ce.keyHash != vID.codeHash {
		c.codeMisses.Add(1)
		return nil, 0, false
	}
	c.codeHits.Add(1)
	return ce.code, vID.step, true
}

// Put stores contract code for the given address.
func (c *CodeCache) Put(addr []byte, code []byte, step kv.Step) {
	// No codeHash in hand here, so the entry is left unverified against maphash
	// collisions. The EVM read path uses PutWithCodeHash, which records it.
	c.putCode(addr, code, [32]byte{}, step, true)
}

// PutIfAbsent implements Cache.PutIfAbsent for the addr→code binding; the
// content-addressed layers skip existing entries regardless.
func (c *CodeCache) PutIfAbsent(addr []byte, code []byte, step kv.Step) {
	c.putCode(addr, code, [32]byte{}, step, false)
}

// putCode populates the addr→codeID and codeID→code layers. keyHash is the
// code's keccak codeHash when known (zero otherwise), stored so Get can reject
// a 64-bit maphash collision.
func (c *CodeCache) putCode(addr []byte, code []byte, keyHash [32]byte, step kv.Step, overwriteAddr bool) {
	if len(code) == 0 {
		return
	}
	codeID := maphash.Hash(code)
	stripe := &c.putStripes[uint8(codeID)]
	stripe.Lock()
	defer stripe.Unlock()

	c.putCodeLocked(addr, code, keyHash, codeID, step, overwriteAddr)
}

func (c *CodeCache) putCodeLocked(addr []byte, code []byte, keyHash [32]byte, codeID uint64, step kv.Step, overwriteAddr bool) {
	a := common.BytesToAddress(addr)
	c.addrBindMu.Lock()
	bindAddr := overwriteAddr
	if !bindAddr {
		_, ok := c.addrToHash.Get(a)
		bindAddr = !ok
	}
	if bindAddr {
		c.addrToHash.Add(a, addressCodeID{addrID: codeID, codeHash: keyHash, step: step})
	}
	c.addrBindMu.Unlock()

	entry := codeEntry{code: code, keyHash: keyHash}
	// freelru keyed by the codeID (maphash of code) directly; 8-byte key cost.
	putContentLocked(c.hashToCode, codeID, entry, codeEntryCodeLen, &c.codeSize, 8)
}

// GetAddrCodeHash returns the Ethereum codeHash for addr if cached. Lets
// SharedDomains.codeHashForAddr skip a cold AccountsDomain read when the
// EVM-known codeHash is already known. Eviction is LRU; freshly seen addrs
// replace coldest entries.
func (c *CodeCache) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	k := common.BytesToAddress(addr)
	e, ok := c.addrToCodeHash.Get(k)
	if !ok {
		return [32]byte{}, false
	}
	return e.hash, true
}

// PutAddrCodeHash records an addr → codeHash mapping if none is cached.
func (c *CodeCache) PutAddrCodeHash(addr []byte, h [32]byte) {
	a := common.BytesToAddress(addr)
	stripe := &c.putStripes[a[len(a)-1]]
	stripe.Lock()
	defer stripe.Unlock()

	if _, ok := c.addrToCodeHash.Get(a); ok {
		return
	}
	c.addrToCodeHash.Add(a, addrCodeHashEntry{hash: h})
}

// DeleteAddrCodeHash removes the mapping when its account record is invalidated.
func (c *CodeCache) DeleteAddrCodeHash(addr []byte) {
	c.addrToCodeHash.Remove(common.BytesToAddress(addr))
}

// GetByCodeHash retrieves contract code by its Ethereum codeHash (keccak256).
// Bypasses the addr-keyed L1/L2 path. Returns (code, true) on hit, (nil, false) on miss.
//
// Designed for the common path where the caller has already loaded the
// account and knows the codeHash (EXTCODESIZE, EXTCODEHASH, CALL targets
// after account-load). Many addresses sharing one codeHash all hit this
// single codeHashToCode entry after the first population.
func (c *CodeCache) GetByCodeHash(codeHash []byte) ([]byte, bool) {
	h := maphash.Hash(codeHash)
	ce, ok := c.codeHashToCode.Get(h)
	if !ok || len(ce.code) == 0 {
		c.codeHashMisses.Add(1)
		return nil, false
	}
	// Reject a 64-bit maphash collision: a different codeHash collapsed to the
	// same bucket would otherwise serve the wrong contract's code.
	if ce.keyHash != hash32(codeHash) {
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
func (c *CodeCache) PutWithCodeHash(addr []byte, code []byte, codeHash []byte, step kv.Step) {
	c.putWithCodeHash(addr, code, codeHash, step, true)
}

// PutWithCodeHashIfAbsent is PutWithCodeHash with if-absent binding semantics
// (see Cache.PutIfAbsent).
func (c *CodeCache) PutWithCodeHashIfAbsent(addr []byte, code []byte, codeHash []byte, step kv.Step) {
	c.putWithCodeHash(addr, code, codeHash, step, false)
}

func (c *CodeCache) lockPutStripes(a, b uint8) {
	if a > b {
		a, b = b, a
	}
	c.putStripes[a].Lock()
	if a != b {
		c.putStripes[b].Lock()
	}
}

func (c *CodeCache) unlockPutStripes(a, b uint8) {
	if a > b {
		a, b = b, a
	}
	if a != b {
		c.putStripes[b].Unlock()
	}
	c.putStripes[a].Unlock()
}

func (c *CodeCache) lockAllPutStripes() {
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
}

func (c *CodeCache) unlockAllPutStripes() {
	for i := len(c.putStripes) - 1; i >= 0; i-- {
		c.putStripes[i].Unlock()
	}
}

func (c *CodeCache) putWithCodeHash(addr []byte, code []byte, codeHash []byte, step kv.Step, overwriteAddr bool) {
	if len(code) == 0 || len(codeHash) == 0 {
		return
	}
	hcc := maphash.Hash(codeHash)
	codeID := hcc
	if len(addr) > 0 {
		codeID = maphash.Hash(code)
	}
	c.lockPutStripes(uint8(codeID), uint8(hcc))
	defer c.unlockPutStripes(uint8(codeID), uint8(hcc))

	kh := hash32(codeHash)

	if len(addr) > 0 {
		c.putCodeLocked(addr, code, kh, codeID, step, overwriteAddr)
	}

	// Populate the size-only layer alongside the bytes layer — every time
	// we touch the bytes we can answer a future EXTCODESIZE for free.
	c.putCodeSizeByCodeHashLocked(codeHash, len(code), hcc)

	entry := codeEntry{code: code, keyHash: kh}
	// freelru keyed by maphash(codeHash); 32-byte key cost.
	putContentLocked(c.codeHashToCode, hcc, entry, codeEntryCodeLen,
		&c.codeHashCodeSize, int64(len(codeHash)))
}

// GetCodeSizeByCodeHash retrieves the size (in bytes) of a contract by its
// Ethereum codeHash, without loading the bytes. Returns (0, false) on miss.
//
// Designed for EXTCODESIZE / EXTCODEHASH which only need the length; on a
// cache hit the caller answers a 4-instruction map probe instead of paying
// the file-accessor + decompression stack for the full bytes.
func (c *CodeCache) GetCodeSizeByCodeHash(codeHash []byte) (int, bool) {
	h := maphash.Hash(codeHash)
	e, ok := c.codeSizeByCodeHash.Get(h)
	if !ok {
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	// Reject a 64-bit maphash collision (see GetByCodeHash).
	if e.keyHash != hash32(codeHash) {
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	c.codeSizeHits.Add(1)
	return e.size, true
}

// PutCodeSizeByCodeHash stores the size of code keyed by its Ethereum codeHash.
func (c *CodeCache) PutCodeSizeByCodeHash(codeHash []byte, size int) {
	if len(codeHash) == 0 || size < 0 {
		return
	}
	hcs := maphash.Hash(codeHash)
	stripe := &c.putStripes[uint8(hcs)]
	stripe.Lock()
	defer stripe.Unlock()

	c.putCodeSizeByCodeHashLocked(codeHash, size, hcs)
}

func (c *CodeCache) putCodeSizeByCodeHashLocked(codeHash []byte, size int, hcs uint64) {
	kh := hash32(codeHash)
	entry := codeSizeEntry{size: size, keyHash: kh}
	// Entry-counted layer: each entry costs 1 against the entry cap.
	putContentLocked(c.codeSizeByCodeHash, hcs, entry, zeroCost, &c.codeSizeEntries, 1)
}

// Delete removes the address → code mapping for addr.
func (c *CodeCache) Delete(addr []byte) {
	c.addrBindMu.Lock()
	c.addrToHash.Remove(common.BytesToAddress(addr))
	c.addrBindMu.Unlock()
}

// Clear removes every layer and resets accounting. It holds every put stripe
// so no put can cross the multi-layer clear.
func (c *CodeCache) Clear() {
	c.lockAllPutStripes()
	defer c.unlockAllPutStripes()

	c.addrToHash.Purge()
	c.addrToCodeHash.Purge()
	c.hashToCode.Purge()
	c.codeHashToCode.Purge()
	c.codeSizeByCodeHash.Purge()
	c.codeSize.Store(0)
	c.codeHashCodeSize.Store(0)
	c.codeSizeEntries.Store(0)
}

// Close returns the content layers' envelope reservations. Idempotent.
func (c *CodeCache) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.hashToCode.Close()
		c.codeHashToCode.Close()
		c.codeSizeByCodeHash.Close()
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

// AddrSizeBytes returns the estimated size of the address cache in bytes,
// counting both addr-keyed LRUs (addr→codeID and addr→codeHash) at their real
// per-entry residency, not just the addr+codeID pair.
func (c *CodeCache) AddrSizeBytes() int64 {
	return int64(c.addrToHash.Len())*int64(addrToHashEntryBytes) +
		int64(c.addrToCodeHash.Len())*int64(addrToCodeHashEntryBytes)
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
