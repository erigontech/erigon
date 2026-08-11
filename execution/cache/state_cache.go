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

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

const (
	DefaultAccountCacheBytes = 150 * datasize.MB
	DefaultStorageCacheBytes = 1 * datasize.GB

	// These estimates translate byte budgets into entry-count ceilings for the
	// shared envelope; actual residency is tracked from key and value sizes.
	avgAccountEntryBytes = 88
	avgStorageEntryBytes = 80
)

// StateCache is a unified cache for domain data (Account, Storage, Code).
// Uses an array indexed by kv.Domain. Only Account, Storage, and Code domains
// are supported; other indices are nil.
//
// It holds values for one durable database state over one compatible files
// view. Publication revokes a reader's generation before changing entries, so
// the reader cannot observe mixed state.
type StateCache struct {
	generation GenerationGate

	// coveredTxNumEnd is only a file-provenance watermark. Cache validity is
	// decided by Generation; canonical commits advance every cached domain,
	// including domains with no writes, while incompatible file changes reset
	// each end to the newly visible view.
	coveredTxNumEnd [kv.DomainLen]uint64
	caches          [kv.DomainLen]Cache
	// disableFills (STATE_CACHE_FILLS=false) turns off every reader fill
	// (including the content-addressed ones), leaving committed publications as
	// the only population path ("apply-only" mode) — an A/B lever and an
	// operational kill switch.
	disableFills bool
}

// NewStateCache creates a new StateCache with the specified byte capacities.
// Mode for the byte-budget DomainCaches (Account/Storage) is read once from
// STATE_CACHE_MODE (evict|noop, default evict). CodeCache has its own LRU and
// is not gated by this knob.
func NewStateCache(accountBytes, storageBytes, codeBytes, addrBytes datasize.ByteSize) *StateCache {
	mode := stateCacheModeFromEnv()
	sc := &StateCache{}
	if !dbg.EnvBool("STATE_CACHE_FILLS", true) {
		sc.disableFills = true
		log.Info("[cache] STATE_CACHE_FILLS=false — read fills disabled, only committed publications populate the cache")
	}
	sc.caches[kv.AccountsDomain] = newDomainCacheBytes(accountBytes, avgAccountEntryBytes, mode)
	sc.caches[kv.StorageDomain] = newDomainCacheBytes(storageBytes, avgStorageEntryBytes, mode)
	sc.caches[kv.CodeDomain] = NewCodeCache(codeBytes, addrBytes)
	// CommitmentDomain deliberately gets no cache: commitment data lives in the
	// BranchCache, and the nil slot short-circuits every StateCache path for it
	// (including writes of commitmentdb.KeyCommitmentState).
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
		GenericCache: NewGenericCacheWithAvg(capacityBytes, avgBytes, func(v domainEntry) int { return len(v.value) }, mode),
	}
}

// NewDefaultStateCache creates a new StateCache with the production byte
// budgets, each overridable by env (values parse as "150MB", "1GB") so a
// sizing A/B needs no rebuild. Harnesses that build many short-lived
// ExecModules set a small ethconfig.Config.StateCacheBudget instead.
//
// The budgets draw from one shared envelope (cachebudget.Global), so raising
// them all at once buys nothing — a step that would overflow it is refused and
// that cache stops growing.
func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		dbg.EnvDataSize("STATE_CACHE_ACCOUNTS", DefaultAccountCacheBytes),
		dbg.EnvDataSize("STATE_CACHE_STORAGE", DefaultStorageCacheBytes),
		dbg.EnvDataSize("STATE_CACHE_CODE", DefaultCodeCacheBytes),
		dbg.EnvDataSize("STATE_CACHE_CODE_INDEX", DefaultAddrCacheBytes),
	)
}

// BeginFilesPublication prepares StateCache for new accounts, storage, and code
// files. It retains values covered by this process's committed updates and
// clears the cache when compatibility cannot be proven. A non-nil result keeps
// cache publication blocked until Finish is called after the files are visible.
func (c *StateCache) BeginFilesPublication(filesEnd [kv.DomainLen]uint64) *BackingChange {
	if c == nil {
		return nil
	}
	files := stateFilesView(
		filesEnd[kv.AccountsDomain],
		filesEnd[kv.StorageDomain],
		filesEnd[kv.CodeDomain],
	)
	return c.generation.Publisher().BeginBackingChange(files, func(lowered bool) bool {
		if lowered {
			c.coveredTxNumEnd = filesEnd
			return true
		}
		extended := false
		for domain, cache := range c.caches {
			if cache == nil || filesEnd[domain] <= c.coveredTxNumEnd[domain] {
				continue
			}
			c.coveredTxNumEnd[domain] = filesEnd[domain]
			extended = true
		}
		return extended
	}, c.clearLocked)
}

func (c *StateCache) getWithStep(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	cache := c.getCache(domain)
	if cache == nil {
		return nil, 0, false
	}
	return cache.GetWithStep(key)
}

// getCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss or
// when the code domain cache is not a CodeCache (defensive fallback).
func (c *StateCache) getCodeByHash(codeHash []byte) ([]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return nil, false
	}
	return cc.GetByCodeHash(codeHash)
}

// getCodeSizeByHash returns the size of code by its Ethereum codeHash
// without loading the bytes. Returns (0, false) when the size-only layer
// is not populated for this hash.
func (c *StateCache) getCodeSizeByHash(codeHash []byte) (int, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return 0, false
	}
	return cc.GetCodeSizeByCodeHash(codeHash)
}

// getAddrCodeHash returns the Ethereum codeHash for addr without an
// account-domain round-trip. The hash is zero when ok is false.
func (c *StateCache) getAddrCodeHash(addr []byte) ([32]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return [32]byte{}, false
	}
	return cc.GetAddrCodeHash(addr)
}

func (c *StateCache) fill(
	generation GenerationView,
	domain kv.Domain,
	key, value []byte,
	step kv.Step,
) {
	cache := c.getCache(domain)
	if cache == nil {
		return
	}
	value = bytes.Clone(value)

	generation.Admit(func() {
		cache.PutIfAbsent(key, value, step)
	})
}

func (c *StateCache) fillCode(
	generation GenerationView,
	key, value []byte,
	step kv.Step,
) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok || len(value) == 0 {
		return
	}
	value = bytes.Clone(value)
	codeHash := crypto.Keccak256(value)

	generation.Admit(func() {
		codeCache.PutWithCodeHashIfAbsent(key, value, codeHash, step)
	})
}

func (c *StateCache) seedAddrCodeHash(generation GenerationView, addr []byte, hash [32]byte) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	generation.Admit(func() {
		codeCache.PutAddrCodeHash(addr, hash)
	})
}

func (c *StateCache) fillCodeSize(generation GenerationView, codeHash []byte, size int) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	generation.Admit(func() {
		codeCache.PutCodeSizeByCodeHash(codeHash, size)
	})
}

func (c *StateCache) deleteAddrCodeHash(addr []byte) {
	if codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache); ok {
		codeCache.DeleteAddrCodeHash(addr)
	}
}

func (c *StateCache) applyLocked(update Update) {
	cache := c.getCache(update.Domain)
	if cache == nil {
		return
	}

	switch update.Domain {
	case kv.AccountsDomain:
		putOrDelete(cache, update.Key, update.Value, update.Step)
		c.deleteAddrCodeHash(update.Key)
		if len(update.Value) == 0 {
			if code := c.getCache(kv.CodeDomain); code != nil {
				code.Delete(update.Key)
			}
		}
	case kv.CodeDomain:
		if len(update.Value) == 0 {
			cache.Delete(update.Key)
			c.deleteAddrCodeHash(update.Key)
			return
		}
		value := bytes.Clone(update.Value)
		if codeCache, ok := cache.(*CodeCache); ok {
			codeCache.PutWithCodeHash(update.Key, value, crypto.Keccak256(value), update.Step)
		}
	default:
		putOrDelete(cache, update.Key, update.Value, update.Step)
	}
}

func (c *StateCache) coverCanonicalStateLocked(txNumEnd uint64) {
	for domain, stateCache := range c.caches {
		if stateCache != nil && txNumEnd > c.coveredTxNumEnd[domain] {
			c.coveredTxNumEnd[domain] = txNumEnd
		}
	}
}

func putOrDelete(cache Cache, key, value []byte, step kv.Step) {
	if len(value) == 0 {
		cache.Delete(key)
		return
	}
	cache.Put(key, bytes.Clone(value), step)
}

func (c *StateCache) clearLocked() {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Clear()
		}
	}
}

func (c *StateCache) resetProvenanceAndClearLocked() {
	c.coveredTxNumEnd = [kv.DomainLen]uint64{}
	c.clearLocked()
}

// Reset revokes all views, clears entries and file provenance, and leaves the
// cache unpublished until its canonical owner initializes or publishes it.
func (c *StateCache) Reset() {
	if c == nil {
		return
	}
	c.generation.Reset(c.resetProvenanceAndClearLocked)
}

// Close permanently revokes publication, clears entries, and releases the
// sub-caches' shared-envelope reservations. It is idempotent.
func (c *StateCache) Close() {
	if !c.generation.Close(c.resetProvenanceAndClearLocked) {
		return
	}
	for _, cache := range c.caches {
		if cache != nil {
			cache.Close()
		}
	}
}

func (c *StateCache) Caches(domain kv.Domain) bool {
	return c.getCache(domain) != nil
}

func (c *StateCache) getCache(domain kv.Domain) Cache {
	if domain >= kv.DomainLen {
		return nil
	}
	return c.caches[domain]
}

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

// Update is one value written by the database transaction being published.
// Step is the source step returned on cache hits, preserving bounded-read
// semantics.
type Update struct {
	Domain kv.Domain
	Key    []byte
	Value  []byte
	Step   kv.Step
}

type canonicalPublisher = CanonicalPublisher

// Publisher is the mutation capability for canonical state. Normal readers
// receive only ReadView, while code that makes database state durable uses a
// Publisher to move every cache layer to the same Generation.
type Publisher struct {
	canonicalPublisher
	c *StateCache
}

// Publisher returns a handle that can change the cache's canonical generation.
// It must not be given to speculative execution whose writes may be discarded.
func (c *StateCache) Publisher() Publisher {
	if c == nil {
		return Publisher{}
	}
	return Publisher{
		canonicalPublisher: NewCanonicalPublisher(&c.generation, c.resetProvenanceAndClearLocked),
		c:                  c,
	}
}

// Publication represents one pending cache transition after durable state has
// committed. Begin makes the cache unavailable without changing its entries;
// Abort restores the previous generation if publication is abandoned before
// applying changes.
type Publication struct {
	lifecycle *CanonicalPublication
	c         *StateCache
}

func (p Publisher) Begin() *Publication {
	lifecycle := p.canonicalPublisher.Begin()
	if lifecycle == nil {
		return nil
	}
	return &Publication{lifecycle: lifecycle, c: p.c}
}

func (p *Publication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.lifecycle.Abort()
	p.c = nil
}

// Publish applies updates after a successful database commit. txNumEnd is the
// exclusive canonical boundary covered across every state domain. Forward
// commits retain unchanged entries. A lineage replacement sets clear because
// its updates do not enumerate every entry from the discarded state.
func (p *Publication) Publish(generation Generation, txNumEnd uint64, updates []Update, clear bool) {
	if p == nil || p.c == nil {
		return
	}
	p.lifecycle.Publish(generation, clear, func(_ *GenerationPublication) {
		p.c.coverCanonicalStateLocked(txNumEnd)
		for i := range updates {
			p.c.applyLocked(updates[i])
		}
	})
	p.c = nil
}
