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
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

const (
	// DefaultAccountCacheBytes is the byte limit for the account cache.
	DefaultAccountCacheBytes = 1 * datasize.GB
	// DefaultStorageCacheBytes is the byte limit for storage cache. 150 MB
	// holds the hot storage working set with headroom so eviction pressure
	// doesn't push the hot set out.
	DefaultStorageCacheBytes = 150 * datasize.MB

	// Per-domain avg entry size used to translate the byte budget into the
	// entry-count cap the underlying sharded LRU is sized against. Account
	// and storage are near-fixed: addr + record or addr+slot + value plus
	// entry overhead.
	avgAccountEntryBytes = 96 // 20 addr + ~50 account record + 24 overhead
	avgStorageEntryBytes = 88 // 52 addr+slot + ~12 value + 24 overhead
)

// StateCache is a unified cache for domain data (Account, Storage, Code).
// Uses an array indexed by kv.Domain. Only Account, Storage, and Code domains
// are supported; other indices are nil.
//
// Account and Storage use GenericCache.
// Code uses CodeCache (two-level for deduplication).
type StateCache struct {
	caches [kv.DomainLen]Cache
	// admissionMu makes Apply's frontier advance + cache mutation atomic
	// against concurrent read-fills, which recheck freshness under RLock.
	admissionMu sync.RWMutex
	appliedEnd  [kv.DomainLen]uint64
}

// NewStateCache creates a new StateCache with the specified byte capacities.
// Mode for the byte-budget DomainCaches (Account/Storage) is read once from
// STATE_CACHE_MODE (evict|noop, default evict). CodeCache has its own LRU and
// is not gated by this knob.
func NewStateCache(accountBytes, storageBytes, codeBytes, addrBytes datasize.ByteSize) *StateCache {
	mode := stateCacheModeFromEnv()
	sc := &StateCache{}
	sc.caches[kv.AccountsDomain] = newDomainCacheBytes(accountBytes, avgAccountEntryBytes, mode)
	sc.caches[kv.StorageDomain] = newDomainCacheBytes(storageBytes, avgStorageEntryBytes, mode)
	sc.caches[kv.CodeDomain] = NewCodeCache(codeBytes, addrBytes)
	return sc
}

// stateCacheModeFromEnv reads STATE_CACHE_MODE (once per NewStateCache). Unset
// or unrecognised returns ModeEvictLRU. Recognised values: "evict", "noop". The
// noop and unrecognised cases log; the default evict path is silent.
func stateCacheModeFromEnv() Mode {
	v := strings.ToLower(strings.TrimSpace(dbg.EnvString("STATE_CACHE_MODE", "")))
	switch v {
	case "", "evict":
		return ModeEvictLRU
	case "noop":
		log.Info("[cache] STATE_CACHE_MODE=noop — Account/Storage caches will drop new keys when full (diagnostic baseline; not for production)")
		return ModeNoOp
	default:
		log.Warn("[cache] unrecognised STATE_CACHE_MODE; defaulting to evict", "value", v)
		return ModeEvictLRU
	}
}

// newDomainCacheBytes constructs a DomainCache whose growth ceiling is derived
// from the byte budget using the supplied per-domain avg. It jump-grows from a
// small start into the shared envelope on demand, so a domain with a small
// working set (a test fixture) never pre-commits the full budget.
func newDomainCacheBytes(capacityBytes datasize.ByteSize, avgBytes uint32, mode Mode) *DomainCache {
	return &DomainCache{
		GenericCache: NewGenericCacheWithAvg(capacityBytes, avgBytes, func(v []byte) int { return len(v) }, mode),
	}
}

// NewDefaultStateCache creates a new StateCache with the production byte budgets
// (Account 1GB, Storage 150MB, Code 512MB, Addr 16MB). Test/CLI harnesses that
// build many short-lived ExecModules pass an explicit small cache instead — via
// ExecModuleTester, or via ethconfig.Config.StateCacheBudget for the eth.New path.
func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		DefaultAccountCacheBytes,
		DefaultStorageCacheBytes,
		DefaultCodeCacheBytes,
		DefaultAddrCacheBytes,
	)
}

// Get retrieves data for the given domain and key.
// Returns (value, true) on cache hit — including (nil, true) for cached negatives —
// and (nil, false) on cache miss.
func (c *StateCache) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	cache := c.caches[domain]
	if cache == nil {
		return nil, false
	}
	return cache.Get(key)
}

// GetWithTxNum is Get plus the txNum the cached value reflects, so the read
// path can bound a hit by step against an in-flight unwind's maxStep.
func (c *StateCache) GetWithTxNum(domain kv.Domain, key []byte) ([]byte, uint64, bool) {
	cache := c.caches[domain]
	if cache == nil {
		return nil, 0, false
	}
	return cache.GetWithTxNum(key)
}

// GetCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss or
// when the code domain cache is not a CodeCache (defensive fallback).
//
// Use when the caller has the codeHash in hand (post-account-load) — typical
// for EXTCODESIZE / EXTCODEHASH / CALL targets. Lets many-addrs-one-code
// patterns (proxies, factory clones, ERC-20 holders) share a single codeHashToCode
// entry.
func (c *StateCache) GetCodeByHash(codeHash []byte) ([]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return nil, false
	}
	return cc.GetByCodeHash(codeHash)
}

// PutCodeWithHash stores code populating both the addr-keyed path and the
// codeHash-keyed codeHashToCode layer. Callers should prefer this over Put when they
// have the codeHash from the account record — avoids a redundant keccak.
func (c *StateCache) PutCodeWithHash(addr, code, codeHash []byte, txNum uint64) {
	c.putCodeWithHash(addr, code, codeHash, txNum, true)
}

func (c *StateCache) putCodeWithHash(addr, code, codeHash []byte, txNum uint64, overwrite bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	if overwrite {
		cc.PutWithCodeHash(addr, common.Copy(code), codeHash, txNum)
	} else {
		cc.PutWithCodeHashIfAbsent(addr, common.Copy(code), codeHash, txNum)
	}
}

// GetCodeSizeByHash returns the size of code by its Ethereum codeHash
// without loading the bytes. Returns (0, false) when the size-only layer
// is not populated for this hash.
func (c *StateCache) GetCodeSizeByHash(codeHash []byte) (int, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return 0, false
	}
	return cc.GetCodeSizeByCodeHash(codeHash)
}

// PutCodeSizeByHash records the code size for a given codeHash. Useful when
// the caller has the size in hand (e.g. from an account-domain probe that
// resolved a sibling addr to the same code) but doesn't have the bytes.
func (c *StateCache) PutCodeSizeByHash(codeHash []byte, size int, txNum uint64) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutCodeSizeByCodeHash(codeHash, size, txNum)
}

// GetAddrCodeHash returns the Ethereum codeHash for addr without an
// account-domain round-trip. (0xff..ff/false, no entry on miss; (h, true)
// on hit.)
func (c *StateCache) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return [32]byte{}, false
	}
	return cc.GetAddrCodeHash(addr)
}

// PutAddrCodeHashIfFresh conditionally fills a mapping from a current account snapshot.
func (c *StateCache) PutAddrCodeHashIfFresh(addr []byte, h [32]byte, txNum, snapshotEnd uint64) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if snapshotEnd < c.appliedEnd[kv.AccountsDomain] {
		return
	}
	cc.PutAddrCodeHash(addr, h, txNum)
}

func (c *StateCache) deleteAddrCodeHash(addr []byte) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.DeleteAddrCodeHash(addr)
}

// Put stores data for the given domain and key, stamped with the txNum the
// value reflects (for txNum/epoch unwind invalidation).
func (c *StateCache) Put(domain kv.Domain, key []byte, value []byte, txNum uint64) {
	c.put(domain, key, value, txNum, true)
}

// FillIfFresh conditionally inserts a snapshot read without replacing an
// authoritative entry. Negative values use the snapshot's last visible txNum.
func (c *StateCache) FillIfFresh(domain kv.Domain, key []byte, value []byte, readTxNum, snapshotEnd uint64) {
	cache := c.caches[domain]
	if cache == nil || (domain == kv.CodeDomain && len(value) == 0) {
		return
	}

	var codeHash []byte
	if domain == kv.CodeDomain {
		codeHash = crypto.Keccak256(value)
	}

	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if snapshotEnd < c.appliedEnd[domain] {
		return
	}

	if domain == kv.CodeDomain {
		c.putCodeWithHash(key, value, codeHash, readTxNum, false)
		return
	}
	if len(value) == 0 {
		readTxNum = 0
		if snapshotEnd > 0 {
			readTxNum = snapshotEnd - 1
		}
	}
	c.put(domain, key, value, readTxNum, false)
}

func (c *StateCache) put(domain kv.Domain, key []byte, value []byte, txNum uint64, overwrite bool) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	if overwrite {
		cache.Put(key, common.Copy(value), txNum)
	} else {
		cache.PutIfAbsent(key, common.Copy(value), txNum)
	}
}

// Delete removes the data for the given domain and key.
func (c *StateCache) Delete(domain kv.Domain, key []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	cache.Delete(key)
}

// Apply makes a committed domain update authoritative for subsequent fills.
func (c *StateCache) Apply(domain kv.Domain, key, value []byte, txNum uint64) {
	var codeHash []byte
	if domain == kv.CodeDomain && len(value) > 0 {
		// Copy before hashing so the stored bytes and codeHash come from the
		// same snapshot of the caller-owned buffer.
		value = common.Copy(value)
		codeHash = crypto.Keccak256(value)
	}

	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	c.noteApplied(domain, txNum)

	switch domain {
	case kv.AccountsDomain:
		putOrDelete(cache, key, value, txNum)
		c.deleteAddrCodeHash(key)
		if len(value) == 0 {
			if codeCache := c.caches[kv.CodeDomain]; codeCache != nil {
				codeCache.Delete(key)
			}
		}
	case kv.CodeDomain:
		if len(value) == 0 {
			cache.Delete(key)
			c.deleteAddrCodeHash(key)
		} else if codeCache, ok := cache.(*CodeCache); ok {
			codeCache.PutWithCodeHash(key, value, codeHash, txNum)
		}
	default:
		putOrDelete(cache, key, value, txNum)
	}
}

func putOrDelete(cache Cache, key, value []byte, txNum uint64) {
	if len(value) == 0 {
		cache.Delete(key)
		return
	}
	cache.Put(key, common.Copy(value), txNum)
}

func (c *StateCache) noteApplied(domain kv.Domain, txNum uint64) {
	end := txNum
	if end < math.MaxUint64 {
		end++
	}
	if end > c.appliedEnd[domain] {
		c.appliedEnd[domain] = end
	}
}

// Clear removes all mutable entries from all caches.
func (c *StateCache) Clear() {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	for _, cache := range c.caches {
		if cache != nil {
			cache.Clear()
		}
	}
	for i := range c.appliedEnd {
		c.appliedEnd[i] = 0
	}
}

// Close releases every sub-cache's slot in the shared memory envelope so later
// caches size against real concurrency. Idempotent.
func (c *StateCache) Close() {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Close()
		}
	}
}

// Unwind invalidates, across all caches, entries reflecting state above
// unwindToTxNum on a now-dead fork. Diffset-free and O(1): every cache (the
// GenericCaches and the CodeCache, all layers) bumps an epoch + lowers a floor
// and drops stale entries lazily on read. This is the sole cache-invalidation
// path on unwind — the executor never touches the cache during forward execution.
func (c *StateCache) Unwind(unwindToTxNum uint64) {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	for _, cache := range c.caches {
		if cache != nil {
			cache.Unwind(unwindToTxNum)
		}
	}
	for i := range c.appliedEnd {
		if c.appliedEnd[i] > unwindToTxNum {
			c.appliedEnd[i] = unwindToTxNum
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

// PrintStatsAndReset prints cache statistics for all domains and resets counters.
func (c *StateCache) PrintStatsAndReset() {
	if c == nil {
		return
	}
	if acc, ok := c.caches[kv.AccountsDomain].(*DomainCache); ok {
		acc.PrintStatsAndReset("Account")
	}
	if stor, ok := c.caches[kv.StorageDomain].(*DomainCache); ok {
		stor.PrintStatsAndReset("Storage")
	}
	if code, ok := c.caches[kv.CodeDomain].(*CodeCache); ok {
		code.PrintStatsAndReset()
	}
}
