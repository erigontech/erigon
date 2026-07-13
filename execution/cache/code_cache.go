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
	"github.com/erigontech/erigon/execution/cache/coherence"
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
	// element holding size/keyHash/txNum/epoch), used to map the size-layer entry
	// ceiling to an envelope byte budget.
	codeSizeEntryBytes = 64
)

// CodeCache is a multi-level concurrent cache for contract code, keyed by the
// cheap maphash rather than Keccak256 so many shared-code addresses cost little:
//   - L1  addr → maphash(code)
//   - L2  maphash(code) → code
//   - L2b codeHash(code) → code — lets a caller that already knows the Ethereum
//         codeHash (EXTCODESIZE/EXTCODEHASH/CALL after an account read) skip L1.
//
// Capacity is byte-based; once full new puts are dropped, but updates to
// existing entries and deletions still apply.

// Every cached layer carries (txNum, epoch) so an unwind invalidates code the
// same way as the account/storage/branch caches: a contract's code
// value never changes for a given hash, but its EXISTENCE does — code deployed
// on a fork that is later unwound must no longer be discoverable, even by
// codeHash. So the content-addressed layers are NOT treated as immutable; they
// honor the same (txNum, epoch) lazy-drop as the addr layers. This can re-fetch
// code shared across deployments when one is unwound (a multiplicity cost), but
// keeps stale code out of the cache.
type versionedAddressID struct {
	addrID uint64
	// codeHash is the addr's keccak codeHash, used to reject a hashToCode
	// maphash collision (a different contract whose code collides on the
	// 64-bit maphash key). Zero when populated without a known codeHash.
	codeHash [32]byte
	txNum    uint64
	epoch    uint32
}

type addrCodeHashEntry struct {
	hash  [32]byte
	txNum uint64
	epoch uint32
}

// Per-entry residency of the two addr-keyed LRUs: a 20-byte key plus the
// value struct (which carries codeHash/txNum/epoch, not just an 8-byte ID).
// Used both to size the LRUs against the byte budget and to report residency.
const (
	addrToHashEntryBytes     = 20 + int(unsafe.Sizeof(versionedAddressID{}))
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
	txNum   uint64
	epoch   uint32
}

type codeSizeEntry struct {
	size int
	// keyHash — see codeEntry.keyHash.
	keyHash [32]byte
	txNum   uint64
	epoch   uint32
}

type CodeCache struct {
	// addrToHash maps a 20-byte Ethereum address to the maphash-derived
	// codeID for the code at that address. An LRU so fresh-address workloads
	// evict oldest entries and warm up the working set.
	addrToHash *lru.Cache[common.Address, versionedAddressID]
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

	// Unwind coherence shared by every layer (content-addressed ones included):
	// an entry is valid iff written in the current epoch OR its txNum is below
	// the unwind floor. See execution/cache/coherence.
	coh coherence.Gen

	// addrBindMu serializes addr→code binding writers so PutIfAbsent's
	// check+bind is atomic w.r.t. a concurrent authoritative rebind.
	addrBindMu sync.Mutex

	// putStripes serializes putContent's membership-check + size-account +
	// insert per key hash: freelru has no LoadOrStore, so without this two
	// concurrent Puts of the same cold code both miss the check and both add to
	// the byte counter while only one entry survives, drifting the stat upward.
	// Striped by key so distinct keys still put in parallel.
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

// isStale reports whether an entry stamped (txNum, epoch) reflects dead-fork
// state after an unwind: it was written in a superseded epoch and its txNum is
// at or above the unwind floor (the first unwound txNum). Mirrors GenericCache.
func (c *CodeCache) isStale(txNum uint64, epoch uint32) bool {
	return c.coh.IsStale(txNum, epoch)
}

// putContent is the shared insert path for the content-addressed code layers
// (hashToCode, codeHashToCode, codeSizeByCodeHash). Each is a freelru.ShardedLRU
// of per-key-immutable entries carrying a (txNum, epoch) stamp: a live entry is
// kept (its bytes/size are invariant for a given key), a stale one is removed
// (its OnEvict decrements counter) so the fresh entry can replace it, and once
// the entry-count cap is reached freelru.Add evicts the coldest entry (whose
// OnEvict decrements counter) rather than freezing. counter tracks resident
// bytes as a stat; the hard bound is the LRU's entry cap. stamp/valCost are
// non-capturing so passing them allocates nothing on the put path.
func putContent[T any](
	lru *growLRU[T],
	h uint64,
	newEntry T,
	stamp func(T) (uint64, uint32),
	valCost func(T) int64,
	coh *coherence.Gen,
	counter *atomic.Int64,
	keyCost int64,
	stripe *sync.Mutex,
) {
	stripe.Lock()
	defer stripe.Unlock()
	if existing, ok := lru.Get(h); ok {
		if txNum, epoch := stamp(existing); !coh.IsStale(txNum, epoch) {
			return
		}
		lru.Remove(h) // stale — OnEvict decrements counter for the removed entry
	}
	counter.Add(keyCost + valCost(newEntry))
	lru.Add(h, newEntry) // evicts the coldest entry when full; its OnEvict decrements counter
}

func codeEntryStamp(e codeEntry) (uint64, uint32)         { return e.txNum, e.epoch }
func codeEntryCodeLen(e codeEntry) int64                  { return int64(len(e.code)) }
func codeSizeEntryStamp(e codeSizeEntry) (uint64, uint32) { return e.txNum, e.epoch }
func zeroCost[T any](T) int64                             { return 0 }

// NewCodeCache creates a new CodeCache with the specified byte capacities.
func NewCodeCache(codeCapacityBytes, addrCapacityBytes datasize.ByteSize) *CodeCache {
	// The addr budget is shared by both addr-keyed LRUs, so each "slot" costs
	// addrEntryBytes (both entries combined). Divide in ByteSize space so the
	// budget isn't truncated to int before the division.
	addrEntries := max(int(addrCapacityBytes/datasize.ByteSize(addrEntryBytes)), 1024)
	addrLRU, err := lru.New[common.Address, versionedAddressID](addrEntries)
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
	// Before any unwind every entry's txNum is below the floor, so the epoch
	// check never strands a valid entry.
	cc.coh.Init()
	return cc
}

// NewDefaultCodeCache creates a new CodeCache with the default sizes.
func NewDefaultCodeCache() *CodeCache {
	return NewCodeCache(DefaultCodeCacheBytes, DefaultAddrCacheBytes)
}

// Get retrieves contract code for the given address, implementing the Cache interface.
func (c *CodeCache) Get(addr []byte) ([]byte, bool) {
	v, _, ok := c.GetWithTxNum(addr)
	return v, ok
}

// GetWithTxNum is Get plus the txNum of the addr→code binding, so the read
// path can apply the same step bound the DomainCache/BranchCache reads do.
func (c *CodeCache) GetWithTxNum(addr []byte) ([]byte, uint64, bool) {
	k := common.BytesToAddress(addr)
	vID, ok := c.addrToHash.Get(k)
	if !ok {
		c.addrMisses.Add(1)
		return nil, 0, false
	}
	if c.isStale(vID.txNum, vID.epoch) {
		c.addrToHash.Remove(k)
		c.addrMisses.Add(1)
		return nil, 0, false
	}
	c.addrHits.Add(1)

	ce, ok := c.hashToCode.Get(vID.addrID)
	if !ok || len(ce.code) == 0 {
		c.codeMisses.Add(1)
		return nil, 0, false
	}
	if c.isStale(ce.txNum, ce.epoch) {
		c.hashToCode.Remove(vID.addrID) // OnEvict decrements codeSize
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
	// The addr→code binding is what an unwind re-binds; vID.txNum bounds it.
	return ce.code, vID.txNum, true
}

// Put stores contract code for the given address, implementing the Cache interface.
// Uses fast maphash to compute the code identifier. addrToHash is an LRU; the
// hashToCode bytes are content-addressed (immutable for a hash) but carry a
// (txNum, epoch) stamp so an unwound deployment's code stops being discoverable.
func (c *CodeCache) Put(addr []byte, code []byte, txNum uint64) {
	// No codeHash in hand here, so the entry is left unverified against maphash
	// collisions. The EVM read path uses PutWithCodeHash, which records it.
	c.putCode(addr, code, [32]byte{}, txNum, true)
}

// PutIfAbsent implements Cache.PutIfAbsent for the addr→code binding; the
// content-addressed layers skip live entries regardless.
func (c *CodeCache) PutIfAbsent(addr []byte, code []byte, txNum uint64) {
	c.putCode(addr, code, [32]byte{}, txNum, false)
}

// putCode populates the addr→codeID and codeID→code layers. keyHash is the
// code's keccak codeHash when known (zero otherwise), stored so Get can reject
// a 64-bit maphash collision. Size is accounted only on the goroutine that
// actually inserts (LoadOrStore is atomic), so concurrent Puts of the same
// cold code can't both Add and permanently inflate codeSize.
func (c *CodeCache) putCode(addr []byte, code []byte, keyHash [32]byte, txNum uint64, overwriteAddr bool) {
	if len(code) == 0 {
		return
	}
	ep := c.coh.Epoch()
	codeID := maphash.Hash(code)

	a := common.BytesToAddress(addr)
	c.addrBindMu.Lock()
	bindAddr := overwriteAddr
	if !bindAddr {
		e, ok := c.addrToHash.Get(a)
		bindAddr = !ok || c.isStale(e.txNum, e.epoch)
	}
	if bindAddr {
		c.addrToHash.Add(a, versionedAddressID{addrID: codeID, codeHash: keyHash, txNum: txNum, epoch: ep})
	}
	c.addrBindMu.Unlock()

	entry := codeEntry{code: code, keyHash: keyHash, txNum: txNum, epoch: ep}
	// freelru keyed by the codeID (maphash of code) directly; 8-byte key cost.
	putContent(c.hashToCode, codeID, entry, codeEntryStamp, codeEntryCodeLen,
		&c.coh, &c.codeSize, 8, &c.putStripes[uint8(codeID)])
}

// ContainsLive reports whether addr resolves to live code bytes through the
// addr→code binding, without touching hit/miss counters or LRU recency.
// Prefetchers probe it to skip the keccak+copy work of preparing a conditional
// put that a live binding would no-op; advisory only.
func (c *CodeCache) ContainsLive(addr []byte) bool {
	vID, ok := c.addrToHash.Peek(common.BytesToAddress(addr))
	if !ok || c.isStale(vID.txNum, vID.epoch) {
		return false
	}
	ce, ok := c.hashToCode.Peek(vID.addrID)
	if !ok || len(ce.code) == 0 || c.isStale(ce.txNum, ce.epoch) {
		return false
	}
	return vID.codeHash == ([32]byte{}) || ce.keyHash == vID.codeHash
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
	c.addrToCodeHash.Add(common.BytesToAddress(addr), addrCodeHashEntry{hash: h, txNum: txNum, epoch: c.coh.Epoch()})
}

// DeleteAddrCodeHash drops the addr → codeHash mapping. Called on
// SELFDESTRUCT / CREATE2-replace / unwind where the account's codeHash
// has been mutated.
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
	if c.isStale(ce.txNum, ce.epoch) {
		c.codeHashToCode.Remove(h) // OnEvict decrements codeHashCodeSize
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
	c.putWithCodeHash(addr, code, codeHash, txNum, true)
}

// PutWithCodeHashIfAbsent is PutWithCodeHash with if-absent binding semantics
// (see Cache.PutIfAbsent).
func (c *CodeCache) PutWithCodeHashIfAbsent(addr []byte, code []byte, codeHash []byte, txNum uint64) {
	c.putWithCodeHash(addr, code, codeHash, txNum, false)
}

func (c *CodeCache) putWithCodeHash(addr []byte, code []byte, codeHash []byte, txNum uint64, overwriteAddr bool) {
	if len(code) == 0 || len(codeHash) == 0 {
		return
	}
	ep := c.coh.Epoch()
	kh := hash32(codeHash)

	if len(addr) > 0 {
		c.putCode(addr, code, kh, txNum, overwriteAddr)
	}

	// Populate the size-only layer alongside the bytes layer — every time
	// we touch the bytes we can answer a future EXTCODESIZE for free.
	c.PutCodeSizeByCodeHash(codeHash, len(code), txNum)

	entry := codeEntry{code: code, keyHash: kh, txNum: txNum, epoch: ep}
	// freelru keyed by maphash(codeHash); 32-byte key cost.
	hcc := maphash.Hash(codeHash)
	putContent(c.codeHashToCode, hcc, entry, codeEntryStamp, codeEntryCodeLen,
		&c.coh, &c.codeHashCodeSize, int64(len(codeHash)), &c.putStripes[uint8(hcc)])
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
	if c.isStale(e.txNum, e.epoch) {
		c.codeSizeByCodeHash.Remove(h) // OnEvict decrements codeSizeEntries
		c.codeSizeMisses.Add(1)
		return 0, false
	}
	c.codeSizeHits.Add(1)
	return e.size, true
}

// PutCodeSizeByCodeHash stores the size of code keyed by its Ethereum
// codeHash. No-op when the entry cap is reached. txNum stamps the entry for
// unwind invalidation.
func (c *CodeCache) PutCodeSizeByCodeHash(codeHash []byte, size int, txNum uint64) {
	if len(codeHash) == 0 || size < 0 {
		return
	}
	ep := c.coh.Epoch()
	kh := hash32(codeHash)
	entry := codeSizeEntry{size: size, keyHash: kh, txNum: txNum, epoch: ep}
	// Entry-counted layer: each entry costs 1 against the entry cap.
	hcs := maphash.Hash(codeHash)
	putContent(c.codeSizeByCodeHash, hcs, entry, codeSizeEntryStamp, zeroCost,
		&c.coh, &c.codeSizeEntries, 1, &c.putStripes[uint8(hcs)])
}

// Delete removes the address → code mapping for addr.
func (c *CodeCache) Delete(addr []byte) {
	c.addrToHash.Remove(common.BytesToAddress(addr))
}

// Clear hard-resets every layer and the epoch/floor. Use on Reset /
// fork-validation paths where no entry may carry over.
func (c *CodeCache) Clear() {
	c.addrToHash.Purge()
	c.addrToCodeHash.Purge()
	c.hashToCode.Purge()
	c.codeHashToCode.Purge()
	c.codeSizeByCodeHash.Purge()
	c.codeSize.Store(0)
	c.codeHashCodeSize.Store(0)
	c.codeSizeEntries.Store(0)
	c.coh.Init()
}

// Close returns the content layers' envelope reservations. Idempotent.
func (c *CodeCache) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.hashToCode.Close()
		c.codeHashToCode.Close()
		c.codeSizeByCodeHash.Close()
	}
}

// Unwind invalidates entries reflecting dead-fork state. Code deployed on the
// rolled-back fork must stop being discoverable — even by codeHash — because
// although a hash → bytes value is invariant, the code's EXISTENCE is not.
// O(1) and scan-free; every layer's entries at/above the floor from a superseded
// epoch drop lazily on their next Get (re-fetching code shared with a still-live
// deployment, an accepted multiplicity cost). See coherence.Gen.Unwind.
func (c *CodeCache) Unwind(unwindToTxNum uint64) {
	c.coh.Unwind(unwindToTxNum)
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
