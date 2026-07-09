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
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
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

	// warmupsInFlight counts fire-and-forget cache-populating prefetches
	// (WarmupStarted/WarmupDone). Unwind asserts it is zero: a prefetch put
	// racing the epoch bump could stamp a dead-fork value with the post-unwind
	// epoch and have it served as canonical.
	warmupsInFlight atomic.Int64

	// appliedProgress is the per-domain txNum watermark of flush-applied
	// commits. A fill from a snapshot behind it must be skipped: the reader's
	// view may predate a deletion, and the deletion's tombstone defends its
	// key only while resident in the LRU — the watermark cannot be evicted.
	appliedProgress [kv.DomainLen]atomic.Uint64
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
// Returns (value, true) on cache hit — including (nil, true) for deleted keys —
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

// PutCodeWithHashIfAbsent is PutCodeWithHash with if-absent binding semantics
// (see Cache.PutIfAbsent).
func (c *StateCache) PutCodeWithHashIfAbsent(addr, code, codeHash []byte, txNum uint64) {
	c.putCodeWithHash(addr, code, codeHash, txNum, false)
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

// HasLiveCode reports whether addr has a live code-cache binding a
// conditional put would defer to; see CodeCache.ContainsLive.
func (c *StateCache) HasLiveCode(addr []byte) bool {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	return ok && cc.ContainsLive(addr)
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

// PutAddrCodeHash records the addr → codeHash mapping in the addr-keyed
// LRU above SD. Callers that have just decoded an account record should
// call this so subsequent lookups skip the account-domain read.
func (c *StateCache) PutAddrCodeHash(addr []byte, h [32]byte, txNum uint64) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutAddrCodeHash(addr, h, txNum)
}

// DeleteAddrCodeHash drops the addr → codeHash mapping. Used by
// invalidation paths (SELFDESTRUCT / CREATE2-replace / unwind diffsets)
// where the account's codeHash has been mutated.
func (c *StateCache) DeleteAddrCodeHash(addr []byte) {
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

// PutIfAbsent is Put with if-absent semantics (see Cache.PutIfAbsent).
func (c *StateCache) PutIfAbsent(domain kv.Domain, key []byte, value []byte, txNum uint64) {
	c.put(domain, key, value, txNum, false)
}

func (c *StateCache) put(domain kv.Domain, key []byte, value []byte, txNum uint64, overwrite bool) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	if domain == kv.CommitmentDomain && bytes.Equal(key, commitmentdb.KeyCommitmentState) {
		return
	}
	if overwrite {
		cache.Put(key, common.Copy(value), txNum)
	} else {
		cache.PutIfAbsent(key, common.Copy(value), txNum)
	}
}

// Clear removes all mutable entries from all caches.
func (c *StateCache) Clear() {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Clear()
		}
	}
	for i := range c.appliedProgress {
		c.appliedProgress[i].Store(0)
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
//
// Callers must drain any in-flight cache-populating warmup first (see
// WarmupStarted); the assert converts that convention into a loud failure.
func (c *StateCache) Unwind(unwindToTxNum uint64) {
	if dbg.AssertStateCache {
		if n := c.warmupsInFlight.Load(); n != 0 {
			panic(fmt.Sprintf("StateCache.Unwind with %d cache-populating warmup(s) in flight — missing drain before the epoch bump", n))
		}
	}
	for _, cache := range c.caches {
		if cache != nil {
			cache.Unwind(unwindToTxNum)
		}
	}
	for i := range c.appliedProgress {
		if c.appliedProgress[i].Load() > unwindToTxNum {
			c.appliedProgress[i].Store(unwindToTxNum)
		}
	}
}

// NoteApplied raises domain's applied-progress watermark to txNum; the flush
// cache-apply calls it for every update it lands.
func (c *StateCache) NoteApplied(domain kv.Domain, txNum uint64) {
	for {
		cur := c.appliedProgress[domain].Load()
		if txNum <= cur || c.appliedProgress[domain].CompareAndSwap(cur, txNum) {
			return
		}
	}
}

// AppliedProgress returns domain's applied-progress watermark; see the field.
func (c *StateCache) AppliedProgress(domain kv.Domain) uint64 {
	return c.appliedProgress[domain].Load()
}

// WarmupStarted and WarmupDone bracket a fire-and-forget cache-populating
// prefetch; see warmupsInFlight.
func (c *StateCache) WarmupStarted() { c.warmupsInFlight.Add(1) }

// WarmupDone is the counterpart of WarmupStarted.
func (c *StateCache) WarmupDone() { c.warmupsInFlight.Add(-1) }

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
