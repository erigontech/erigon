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
	"strings"

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
	// DefaultStorageCacheBytes is the byte limit for storage cache. 150 MB: the
	// measured mainnet-tip storage working set is ~106 MB for 95% of reads
	// (top 1% of keys = 48%, ~10 MB for 80%); 150 MB leaves headroom over the
	// 95% set so eviction pressure doesn't push the hot set out.
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

// stateCacheModeFromEnv reads STATE_CACHE_MODE once. Unset or unrecognised
// returns ModeEvictLRU. Recognised values: "evict", "noop". Logged at
// startup so operators can see the mode pick.
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

// newDomainCacheBytes constructs a DomainCache where the entry-count cap
// is derived from the byte budget using the supplied per-domain avg.
func newDomainCacheBytes(capacityBytes datasize.ByteSize, avgBytes uint32, mode Mode) *DomainCache {
	capacityEntries := uint32(uint64(capacityBytes) / uint64(avgBytes))
	if capacityEntries == 0 {
		capacityEntries = 1
	}
	return &DomainCache{
		GenericCache: newGenericCacheEntries(capacityBytes, capacityEntries, func(v []byte) int { return len(v) }, mode),
	}
}

// Byte budgets used by NewDefaultStateCache, defaulting to the production
// constants. Mutated only by SetDefaultStateCacheSizesForTesting.
var (
	defaultAccountCacheBytes = DefaultAccountCacheBytes
	defaultStorageCacheBytes = DefaultStorageCacheBytes
	defaultCodeCacheBytes    = DefaultCodeCacheBytes
	defaultAddrCacheBytes    = DefaultAddrCacheBytes
)

// NewDefaultStateCache creates a new StateCache with the default byte budgets
// (Account 1GB, Storage 150MB, Code 512MB, Addr 16MB in production).
func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		defaultAccountCacheBytes,
		defaultStorageCacheBytes,
		defaultCodeCacheBytes,
		defaultAddrCacheBytes,
	)
}

// SetDefaultStateCacheSizesForTesting shrinks the budgets NewDefaultStateCache
// allocates. The eest CLI runners (cmd/evm) call this at startup: they build
// one ExecModule per fixture (the enginextest path goes through eth.New, not
// the per-instance cache), and at production sizes each one allocates ~1.6 GB
// of LRU tables, so the full corpus exhausts the runner's RAM. Production
// (erigon) never calls this; the call lives in the CLI entrypoint, not an
// import-time init, so merely importing a test helper can't change sizing.
func SetDefaultStateCacheSizesForTesting(account, storage, code, addr datasize.ByteSize) {
	defaultAccountCacheBytes = account
	defaultStorageCacheBytes = storage
	defaultCodeCacheBytes = code
	defaultAddrCacheBytes = addr
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
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutWithCodeHash(addr, common.Copy(code), codeHash, txNum)
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
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	if domain == kv.CommitmentDomain && bytes.Equal(key, commitmentdb.KeyCommitmentState) {
		return
	}
	cache.Put(key, common.Copy(value), txNum)
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

// Unwind invalidates, across all caches, entries reflecting state above
// unwindToTxNum on a now-dead fork. Diffset-free and O(1): every cache (the
// GenericCaches and the CodeCache, all layers) bumps an epoch + lowers a floor
// and drops stale entries lazily on read. This is the sole cache-invalidation
// path on unwind — the executor never touches the cache during forward execution.
func (c *StateCache) Unwind(unwindToTxNum uint64) {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Unwind(unwindToTxNum)
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
