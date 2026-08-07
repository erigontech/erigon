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
	"sync"
	"sync/atomic"

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

type cacheGeneration struct {
	stateVersion uint64
	active       bool
}

// StateCache holds account, storage, and code data for one durable state
// version. A generation is made inactive before any publication changes the
// underlying caches, so readers never observe a partially published version.
type StateCache struct {
	generation   atomic.Pointer[cacheGeneration]
	admissionMu  sync.RWMutex
	caches       [kv.DomainLen]Cache
	disableFills bool
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

func (c *StateCache) generationFor(stateVersion uint64) *cacheGeneration {
	generation := c.generation.Load()
	if generation == nil || !generation.active || generation.stateVersion != stateVersion {
		return nil
	}
	return generation
}

// CurrentStateVersion reports the durable version represented by the cache.
// It is unavailable while a publication is in progress.
func (c *StateCache) CurrentStateVersion() (uint64, bool) {
	generation := c.generation.Load()
	if generation == nil || !generation.active {
		return 0, false
	}
	return generation.stateVersion, true
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
	generation *cacheGeneration,
	domain kv.Domain,
	key, value []byte,
	step kv.Step,
) {
	cache := c.getCache(domain)
	if cache == nil {
		return
	}
	value = bytes.Clone(value)

	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.generation.Load() != generation {
		return
	}
	cache.PutIfAbsent(key, value, step)
}

func (c *StateCache) fillCode(
	generation *cacheGeneration,
	key, value []byte,
	step kv.Step,
) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok || len(value) == 0 {
		return
	}
	value = bytes.Clone(value)
	codeHash := crypto.Keccak256(value)

	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.generation.Load() != generation {
		return
	}
	codeCache.PutWithCodeHashIfAbsent(key, value, codeHash, step)
}

func (c *StateCache) seedAddrCodeHash(generation *cacheGeneration, addr []byte, hash [32]byte) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.generation.Load() != generation {
		return
	}
	codeCache.PutAddrCodeHash(addr, hash)
}

func (c *StateCache) fillCodeSize(generation *cacheGeneration, codeHash []byte, size int) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.generation.Load() != generation {
		return
	}
	codeCache.PutCodeSizeByCodeHash(codeHash, size)
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

func (c *StateCache) Close() {
	c.admissionMu.Lock()
	c.generation.Store(&cacheGeneration{})
	c.admissionMu.Unlock()
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

// Update is one committed cache value. Step is returned on a later GetLatest
// hit; it is not used for cache coherence.
type Update struct {
	Domain kv.Domain
	Key    []byte
	Value  []byte
	Step   kv.Step
}

// Publisher is the canonical mutation handle for StateCache.
type Publisher struct {
	c *StateCache
}

func (c *StateCache) Publisher() Publisher { return Publisher{c: c} }

func (p Publisher) Enabled() bool { return p.c != nil }

// Initialize makes the cache represent stateVersion. A version mismatch drops
// all entries because their source version is unknown.
func (p Publisher) Initialize(stateVersion uint64) {
	if p.c == nil {
		return
	}
	c := p.c
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()

	current := c.generation.Load()
	if current != nil && current.active {
		if current.stateVersion == stateVersion {
			return
		}
	} else if current != nil {
		panic("state cache publication already in progress")
	}

	c.generation.Store(&cacheGeneration{})
	c.clearLocked()
	c.generation.Store(&cacheGeneration{stateVersion: stateVersion, active: true})
}

// Publication keeps the previous generation available for rollback until the
// database commit succeeds.
type Publication struct {
	c          *StateCache
	previous   *cacheGeneration
	transition *cacheGeneration
}

// Begin revokes every existing ReadView before the database commit starts.
func (p Publisher) Begin() *Publication {
	if p.c == nil {
		return nil
	}
	c := p.c
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()

	previous := c.generation.Load()
	if previous != nil && !previous.active {
		panic("state cache publication already in progress")
	}
	transition := &cacheGeneration{}
	c.generation.Store(transition)
	return &Publication{c: c, previous: previous, transition: transition}
}

// Abort restores the unchanged cache when the database transaction rolls back.
func (p *Publication) Abort() {
	if p == nil || p.c == nil {
		return
	}
	p.c.admissionMu.Lock()
	defer p.c.admissionMu.Unlock()
	if p.c.generation.Load() != p.transition {
		panic("state cache publication changed before abort")
	}
	p.c.generation.Store(p.previous)
	p.c = nil
}

// Publish applies the committed batch and makes its state version visible.
// clear is used for canonical unwind because entries absent from the unwind
// callbacks may still belong to the discarded fork.
func (p *Publication) Publish(stateVersion uint64, updates []Update, clear bool) {
	if p == nil || p.c == nil {
		return
	}
	p.c.admissionMu.Lock()
	defer p.c.admissionMu.Unlock()
	if p.c.generation.Load() != p.transition {
		panic("state cache publication changed before publish")
	}
	if clear {
		p.c.clearLocked()
	}
	for i := range updates {
		p.c.applyLocked(updates[i])
	}
	p.c.generation.Store(&cacheGeneration{stateVersion: stateVersion, active: true})
	p.c = nil
}

func (p Publisher) Clear(stateVersion uint64) {
	publication := p.Begin()
	publication.Publish(stateVersion, nil, true)
}
