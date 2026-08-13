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
	"math"
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
	DefaultAccountCacheBytes = 150 * datasize.MB
	DefaultStorageCacheBytes = 1 * datasize.GB

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
// StateCache itself exposes no data methods: reads and admission-gated fills
// go through a ReadView bound to one tx's read view, committed updates through
// the Applier handle (see view.go).
//
// Account and Storage use GenericCache.
// Code uses CodeCache (two-level for deduplication).
type StateCache struct {
	caches [kv.DomainLen]Cache
	// applierMu serializes authoritative operations while a batch publication
	// releases admissionMu around cache writes.
	applierMu sync.Mutex
	// admissionMu protects fill eligibility and publication identity.
	// publishing disables admission-gated fills while an update is incomplete.
	admissionMu       sync.RWMutex
	publishing        bool
	appliedEnd        [kv.DomainLen]uint64
	stateVersion      uint64
	stateVersionKnown bool
	// readViewEpoch lets an unwind or state-version discontinuity revoke fill
	// authority from all older ReadViews. Per-cache entry epochs instead stamp
	// stored values and also advance on Clear; sharing them would make an
	// ordinary Clear revoke otherwise valid read views.
	readViewEpoch atomic.Uint64
	// disableFills (STATE_CACHE_FILLS=false) turns off every reader fill
	// (including the content-addressed ones), leaving canonical publication as
	// the only writer — an A/B lever and an operational kill switch.
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
		log.Info("[cache] STATE_CACHE_FILLS=false — read fills disabled, only post-commit publication populates the cache")
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
		GenericCache: NewGenericCacheWithAvg(capacityBytes, avgBytes, func(v []byte) int { return len(v) }, mode),
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

// get retrieves data for the given domain and key.
// Returns (value, true) on cache hit — including (nil, true) for cached negatives —
// and (nil, false) on cache miss.
func (c *StateCache) get(domain kv.Domain, key []byte) ([]byte, bool) {
	cache := c.caches[domain]
	if cache == nil {
		return nil, false
	}
	return cache.Get(key)
}

// getWithTxNum is get plus the txNum the cached value reflects, so the read
// path can bound a hit by step against an in-flight unwind's maxStep.
func (c *StateCache) getWithTxNum(domain kv.Domain, key []byte) ([]byte, uint64, bool) {
	cache := c.caches[domain]
	if cache == nil {
		return nil, 0, false
	}
	return cache.GetWithTxNum(key)
}

// getCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss or
// when the code domain cache is not a CodeCache (defensive fallback).
//
// Use when the caller has the codeHash in hand (post-account-load) — typical
// for EXTCODESIZE / EXTCODEHASH / CALL targets. Lets many-addrs-one-code
// patterns (proxies, factory clones, ERC-20 holders) share a single codeHashToCode
// entry.
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

// putCodeSizeByHash records the code size for a given codeHash. Useful when
// the caller has the size in hand (e.g. from an account-domain probe that
// resolved a sibling addr to the same code) but doesn't have the bytes.
func (c *StateCache) putCodeSizeByHash(codeHash []byte, size int, txNum uint64) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutCodeSizeByCodeHash(codeHash, size, txNum)
}

// FillsEnabled reports whether reader fills are active (STATE_CACHE_FILLS).
// Wire-up code uses it to decide whether the backing aggregator must forbid
// visibility lowering: fill admission relies on view frontiers never
// decreasing, and apply-only caches have nothing for a lowered frontier to
// poison.
func (c *StateCache) FillsEnabled() bool { return !c.disableFills }

// getAddrCodeHash returns the Ethereum codeHash for addr without an
// account-domain round-trip. The hash is zero when ok is false.
func (c *StateCache) getAddrCodeHash(addr []byte) ([32]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return [32]byte{}, false
	}
	return cc.GetAddrCodeHash(addr)
}

// seedAddrCodeHash conditionally records an addr → codeHash mapping.
// The mapping derives from an account record, so admission checks the accounts
// frontier even though the mapping lives in the code cache.
func (c *StateCache) seedAddrCodeHash(addr []byte, h [32]byte, txNum, visibleEnd,
	viewEpoch uint64) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.publishing ||
		viewEpoch != c.readViewEpoch.Load() ||
		visibleEnd < c.appliedEnd[kv.AccountsDomain] {
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

// fillIfFresh conditionally inserts an accounts or storage value read from a
// read view without replacing an authoritative entry. Negatives use the view's
// last included txNum. Code goes through fillCodeIfFresh.
func (c *StateCache) fillIfFresh(domain kv.Domain, key []byte, value []byte, readTxNum, visibleEnd,
	viewEpoch uint64) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	// Clone outside the lock so admission never waits on the copy.
	cloned := bytes.Clone(value)
	if len(value) == 0 {
		readTxNum = 0
		if visibleEnd > 0 {
			readTxNum = visibleEnd - 1
		}
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.publishing ||
		viewEpoch != c.readViewEpoch.Load() ||
		visibleEnd < c.appliedEnd[domain] {
		return
	}
	cache.PutIfAbsent(key, cloned, readTxNum)
}

// fillCodeIfFresh is fillIfFresh for the code domain. An addr-keyed code entry
// derives from the account — an account deletion drops it without advancing the
// code frontier — so admission also checks the accounts frontier. Code
// negatives are not cached here: "no code" is cached at the addr→codeHash
// mapping instead (the zero-hash sentinel seeded by SeedAddrCodeHash).
func (c *StateCache) fillCodeIfFresh(key []byte, value []byte, readTxNum, visibleEnd, accountsVisibleEnd,
	viewEpoch uint64) {
	codeCache, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok || len(value) == 0 {
		return
	}
	codeHash := crypto.Keccak256(value)
	cloned := bytes.Clone(value)
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	if c.publishing ||
		viewEpoch != c.readViewEpoch.Load() ||
		visibleEnd < c.appliedEnd[kv.CodeDomain] ||
		accountsVisibleEnd < c.appliedEnd[kv.AccountsDomain] {
		return
	}
	codeCache.PutWithCodeHashIfAbsent(key, cloned, codeHash, readTxNum)
}

// deleteKey removes the data for the given domain and key. The authoritative
// publication path advances the fill-admission frontier before calling it.
func (c *StateCache) deleteKey(domain kv.Domain, key []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	cache.Delete(key)
}

type preparedStateUpdate struct {
	domain   kv.Domain
	key      []byte
	value    []byte
	codeHash []byte
	txNum    uint64
}

func prepareStateUpdate(update StateUpdate) preparedStateUpdate {
	prepared := preparedStateUpdate{
		domain: update.Domain,
		key:    update.Key,
		value:  bytes.Clone(update.Value),
		txNum:  update.TxNum,
	}
	if update.Domain == kv.CodeDomain && len(update.Value) > 0 {
		prepared.codeHash = crypto.Keccak256(prepared.value)
	}
	return prepared
}

func (c *StateCache) applyPrepared(update preparedStateUpdate) {
	cache := c.caches[update.domain]
	if cache == nil {
		return
	}
	c.noteApplied(update.domain, update.txNum)

	switch update.domain {
	case kv.AccountsDomain:
		putOrDelete(cache, update.key, update.value, update.txNum)
		c.deleteAddrCodeHash(update.key)
		if len(update.value) == 0 {
			// SharedDomains pairs an account deletion with a code-domain apply;
			// that paired apply is what advances the code frontier — this cascade
			// only drops the entry. Code-fill admission also checks the accounts
			// frontier (fillCodeIfFresh), so the cache holds even for a caller
			// that does not pair the deletes.
			c.deleteKey(kv.CodeDomain, update.key)
		}
	case kv.CodeDomain:
		if len(update.value) == 0 {
			cache.Delete(update.key)
			c.deleteAddrCodeHash(update.key)
		} else if codeCache, ok := cache.(*CodeCache); ok {
			codeCache.PutWithCodeHash(update.key, update.value, update.codeHash, update.txNum)
		}
	default:
		putOrDelete(cache, update.key, update.value, update.txNum)
	}
}

func putOrDelete(cache Cache, key, value []byte, txNum uint64) {
	if len(value) == 0 {
		cache.Delete(key)
		return
	}
	cache.Put(key, value, txNum)
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

// clear removes all mutable entries from all caches. The admission frontier
// survives: clearing drops entries, it does not rewind canonical state, and a
// zeroed frontier would let a still-live older ReadView refill pre-apply data.
func (c *StateCache) clear() {
	c.applierMu.Lock()
	defer c.applierMu.Unlock()
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.clearLocked()
}

func (c *StateCache) clearLocked() {
	for _, cache := range c.caches {
		if cache != nil {
			cache.Clear()
		}
	}
}

func (c *StateCache) resetForStateVersionLocked() {
	// Clearing entries is not enough: views bound to the previous state could
	// otherwise refill them after continuity was lost.
	c.readViewEpoch.Add(1)
	c.clearLocked()
	clear(c.appliedEnd[:])
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

// unwind invalidates, across all caches, entries reflecting state above
// unwindToTxNum on a now-dead fork. Diffset-free and O(1): every cache (the
// GenericCaches and the CodeCache, all layers) bumps an epoch + lowers a floor
// and drops stale entries lazily on read. This is the sole cache-invalidation
// path on unwind — the executor never touches the cache during forward execution.
func (c *StateCache) unwind(unwindToTxNum uint64) {
	c.applierMu.Lock()
	defer c.applierMu.Unlock()
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.unwindLocked(unwindToTxNum)
}

func (c *StateCache) unwindLocked(unwindToTxNum uint64) {
	c.readViewEpoch.Add(1)
	for _, cache := range c.caches {
		if cache != nil {
			cache.Unwind(unwindToTxNum)
		}
	}
	for i := range c.appliedEnd {
		c.appliedEnd[i] = min(c.appliedEnd[i], unwindToTxNum)
	}
}

func (c *StateCache) canAdvanceStateVersionLocked(stateVersion uint64) bool {
	return !c.stateVersionKnown || stateVersion > c.stateVersion
}

func (c *StateCache) initialize(stateVersion uint64) {
	c.applierMu.Lock()
	defer c.applierMu.Unlock()
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	if !c.canAdvanceStateVersionLocked(stateVersion) {
		return
	}
	c.resetForStateVersionLocked()
	c.stateVersion = stateVersion
	c.stateVersionKnown = true
	c.publishing = false
}

func (c *StateCache) beginPublication(sourceStateVersion, committedStateVersion, unwindToTxNum uint64,
	hasUnwind bool) bool {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	if committedStateVersion <= sourceStateVersion || !c.canAdvanceStateVersionLocked(committedStateVersion) {
		// PublishUnwind follows a durable commit, so its invalidation remains
		// authoritative even when a newer cache generation rejects its updates.
		if hasUnwind {
			c.unwindLocked(unwindToTxNum)
		}
		return false
	}
	c.publishing = true
	discontinuous := !c.stateVersionKnown || sourceStateVersion != c.stateVersion
	if discontinuous {
		// The cache missed part of the source state. Incremental updates cannot
		// repair unknown retained entries, so publish into an empty generation.
		c.resetForStateVersionLocked()
	} else if hasUnwind {
		c.unwindLocked(unwindToTxNum)
	}
	return true
}

func (c *StateCache) finishPublication(committedStateVersion uint64) {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.stateVersion = committedStateVersion
	c.stateVersionKnown = true
	c.publishing = false
}

func (c *StateCache) publish(sourceStateVersion, committedStateVersion, unwindToTxNum uint64,
	hasUnwind bool, updates []StateUpdate) {
	prepared := make([]preparedStateUpdate, len(updates))
	for i := range updates {
		prepared[i] = prepareStateUpdate(updates[i])
	}

	c.applierMu.Lock()
	defer c.applierMu.Unlock()
	if !c.beginPublication(sourceStateVersion, committedStateVersion, unwindToTxNum, hasUnwind) {
		return
	}

	// Sub-caches synchronize their own reads and writes. While publishing is
	// true, admission-gated fills cannot mutate state entries or read appliedEnd,
	// so the serialized applier can install the batch without admissionMu.
	for i := range prepared {
		c.applyPrepared(prepared[i])
	}

	c.finishPublication(committedStateVersion)
}

// Caches reports whether the given domain has a cache attached.
func (c *StateCache) Caches(domain kv.Domain) bool {
	return domain < kv.DomainLen && c.caches[domain] != nil
}

func (c *StateCache) getCache(domain kv.Domain) Cache {
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
