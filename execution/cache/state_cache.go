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
	DefaultAccountCacheBytes = 1 * datasize.GB
	DefaultStorageCacheBytes = 150 * datasize.MB

	avgAccountEntryBytes = 88
	avgStorageEntryBytes = 80
)

// StateCache holds account, storage, and code values for one durable database
// state over one compatible files view. Publication revokes a reader's
// generation before changing entries, so it cannot observe mixed state.
type StateCache struct {
	generation GenerationGate

	// committedTxNumEnd is only a file-provenance watermark. Cache validity is
	// decided by Generation; these ends distinguish files covered by published
	// updates in the current canonical lineage from files downloaded outside it.
	committedTxNumEnd [kv.DomainLen]uint64
	caches            [kv.DomainLen]Cache
	disableFills      bool
}

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
	return sc
}

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

func newDomainCacheBytes(capacityBytes datasize.ByteSize, avgBytes uint32, mode Mode) *DomainCache {
	return &DomainCache{
		GenericCache: NewGenericCacheWithAvg(capacityBytes, avgBytes, func(v domainEntry) int { return len(v.value) }, mode),
	}
}

func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		DefaultAccountCacheBytes,
		DefaultStorageCacheBytes,
		DefaultCodeCacheBytes,
		DefaultAddrCacheBytes,
	)
}

// CurrentStateVersion reports the durable PlainStateVersion represented by
// all cache layers. It returns false while publication is in progress because
// the old version has been revoked and the new version is not visible yet.
func (c *StateCache) CurrentStateVersion() (uint64, bool) {
	return c.generation.CurrentStateVersion()
}

// BeginFilesPublication revokes the old files generation. It retains entries
// backed by this process's committed updates and clears them when the new files
// contain foreign state. Finish publishes the new identity after the files
// become visible.
func (c *StateCache) BeginFilesPublication(filesEnd [kv.DomainLen]uint64) *BackingChange {
	if c == nil {
		return nil
	}
	files := stateFilesView(
		filesEnd[kv.AccountsDomain],
		filesEnd[kv.StorageDomain],
		filesEnd[kv.CodeDomain],
	)
	return c.generation.Publisher().BeginBackingChange(files, func() bool {
		extended := false
		for domain, cache := range c.caches {
			if cache == nil || filesEnd[domain] <= c.committedTxNumEnd[domain] {
				continue
			}
			c.committedTxNumEnd[domain] = filesEnd[domain]
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

func (c *StateCache) getCodeByHash(codeHash []byte) ([]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return nil, false
	}
	return cc.GetByCodeHash(codeHash)
}

func (c *StateCache) getCodeSizeByHash(codeHash []byte) (int, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return 0, false
	}
	return cc.GetCodeSizeByCodeHash(codeHash)
}

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
	if committedEnd := update.TxNum + 1; committedEnd > c.committedTxNumEnd[update.Domain] {
		c.committedTxNumEnd[update.Domain] = committedEnd
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
	c.committedTxNumEnd = [kv.DomainLen]uint64{}
	c.clearLocked()
}

func (c *StateCache) Close() {
	c.generation.Close()
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
// Step is returned by GetLatest. TxNum records how far this process's committed
// writes cover the domain, allowing file publication to detect downloaded
// state that never passed through this publisher.
type Update struct {
	Domain kv.Domain
	Key    []byte
	Value  []byte
	Step   kv.Step
	TxNum  uint64
}

// Publisher is the mutation capability for canonical state. Normal readers
// receive only ReadView, while code that makes database state durable uses a
// Publisher to move every cache layer to the same Generation.
type Publisher struct {
	c          *StateCache
	generation GenerationPublisher
}

// Publisher returns a handle that can change the cache's canonical generation.
// It must not be given to speculative execution whose writes may be discarded.
func (c *StateCache) Publisher() Publisher {
	if c == nil {
		return Publisher{}
	}
	return Publisher{c: c, generation: c.generation.Publisher()}
}

func (p Publisher) Enabled() bool { return p.c != nil && p.generation.Enabled() }

// Initialize binds the cache to the database and files generation seen by its
// canonical owner. A mismatch clears entries and their file provenance because
// neither can be proven compatible with that snapshot.
func (p Publisher) Initialize(generation Generation) {
	if p.c == nil {
		return
	}
	p.generation.Initialize(generation, p.c.resetProvenanceAndClearLocked)
}

// Publication represents one pending transition of the durable database
// state. Begin makes the cache unavailable without changing its entries, so
// Abort can restore the previous generation if the transaction rolls back.
// Publish consumes the transition after the database commit succeeds.
type Publication struct {
	c          *StateCache
	generation *GenerationPublication
}

// Begin revokes every existing ReadView and prevents creation of a new live
// view. It does not alter cache entries; they remain available for Abort until
// the canonical database transaction either commits or rolls back.
func (p Publisher) Begin() *Publication {
	if p.c == nil {
		return nil
	}
	return &Publication{c: p.c, generation: p.generation.Begin()}
}

// Abort restores the previous generation after a failed or abandoned database
// transaction. The entries were not changed during the transition, so the old
// ReadViews become valid again together with their database version.
func (p *Publication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.generation.Abort()
	p.c = nil
}

// Publish applies updates from a successful database transaction and exposes
// generation as one complete cache snapshot. The caller must invoke it
// only after the database commit, so a visible cache generation is never ahead
// of durable state.
//
// A forward commit can retain entries that were not updated because they still
// have the same value in the new state. Canonical unwind sets clear because its
// callbacks do not enumerate every value or file-coverage claim that may belong
// to the discarded fork.
func (p *Publication) Publish(generation Generation, updates []Update, clear bool) {
	if p == nil || p.c == nil {
		return
	}
	p.generation.Publish(generation, func() {
		if clear {
			p.c.resetProvenanceAndClearLocked()
		}
		for i := range updates {
			p.c.applyLocked(updates[i])
		}
	})
	p.c = nil
}

// Clear revokes current views, removes every cached value, and publishes an
// empty generation.
func (p Publisher) Clear(generation Generation) {
	publication := p.Begin()
	publication.Publish(generation, nil, true)
}
