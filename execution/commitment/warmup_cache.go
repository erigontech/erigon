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

package commitment

import (
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
)

type branchEntry struct {
	data      []byte
	isEvicted atomic.Bool
	// dirty signals "the canonical store has been written to since this
	// entry was populated; treat as stale until cleared." Read paths
	// MAY return miss when dirty is set; write paths MUST set dirty.
	// Used together with PutBranchIfClean to prevent late warmup writes
	// from clobbering fresh fold writes (TOCTOU race documented in the
	// reth-research §4 dirty-flag pattern). Ephemeral cache today does
	// not have the race because warmup completes before fold begins;
	// invariant is in place ahead of cross-block persistence work.
	dirty atomic.Bool
}

type accountEntry struct {
	update    *Update
	isEvicted atomic.Bool
}

type storageEntry struct {
	update    *Update
	isEvicted atomic.Bool
}

// WarmupCache stores pre-fetched data from the warmup phase to avoid
// repeated DB reads during trie processing. Uses maphash.Map for efficient
// byte slice key lookups without string allocations.
type WarmupCache struct {
	branches *maphash.Map[*branchEntry]
	accounts *maphash.Map[*accountEntry]
	storage  *maphash.Map[*storageEntry]

	// Observability counters. Updated by Get/Put paths; read by Stats().
	// Atomic so warmup-worker reads and main-trie reads don't race.
	branchHits, branchMisses, branchEvicted atomic.Uint64
	branchBytesServed                       atomic.Uint64
	accountHits, accountMisses              atomic.Uint64
	storageHits, storageMisses              atomic.Uint64
}

// NewWarmupCache creates a new warmup cache instance.
func NewWarmupCache() *WarmupCache {
	return &WarmupCache{
		branches: maphash.NewMap[*branchEntry](),
		accounts: maphash.NewMap[*accountEntry](),
		storage:  maphash.NewMap[*storageEntry](),
	}
}

// PutBranch stores branch data in the cache.
func (c *WarmupCache) PutBranch(prefix []byte, data []byte) {
	// Make a copy of the data to avoid issues with buffer reuse
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	c.branches.Set(prefix, &branchEntry{data: dataCopy})
}

// PutBranchIfClean stores branch data in the cache only if no existing entry
// is marked dirty. Returns true on store, false if a dirty entry was present
// (indicating the canonical store has been updated since the caller last
// read, and the caller's data is potentially stale).
//
// Use from warmup-style writers that may race with fold writes — the fold
// path marks branches dirty before its own write completes, so a warmup
// worker that reads pre-fold then attempts to write post-fold will see
// dirty=true and skip the store.
//
// Today's call sites (warmup workers in HexPatriciaHashed.Process) do not
// race with fold because warmup completes before HashSort begins. Invariant
// is in place for future cross-block persistence work where warmup-style
// writes can outlive their parent Process.
func (c *WarmupCache) PutBranchIfClean(prefix []byte, data []byte) bool {
	if existing, found := c.branches.Get(prefix); found && existing.dirty.Load() {
		return false
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.branches.Set(prefix, &branchEntry{data: dataCopy})
	return true
}

// MarkBranchDirty flags a branch entry as stale-until-cleared. The next
// PutBranchIfClean for this prefix will skip; reads (GetBranch /
// GetAndEvictBranch) currently return the entry regardless — the dirty
// signal is consumed only on the write path today.
//
// Use from fold/encoder paths that have decided to overwrite a branch but
// haven't yet captured the new bytes. The mark-dirty + later actual-write
// pattern is the deferred-encoding-friendly alternative to inline
// invalidate, motivated by the prototype investigation that found inline
// invalidate breaks update-in-place semantics under deferred encoding.
func (c *WarmupCache) MarkBranchDirty(prefix []byte) {
	if entry, found := c.branches.Get(prefix); found {
		entry.dirty.Store(true)
	}
}

// GetBranch retrieves branch data from the cache.
func (c *WarmupCache) GetBranch(prefix []byte) ([]byte, bool) {
	entry, found := c.branches.Get(prefix)
	if !found {
		c.branchMisses.Add(1)
		return nil, false
	}
	if entry.isEvicted.Load() {
		c.branchEvicted.Add(1)
		return nil, false
	}
	c.branchHits.Add(1)
	c.branchBytesServed.Add(uint64(len(entry.data)))
	return entry.data, true
}

// GetAndEvictBranch retrieves branch data and marks the entry as evicted in one operation.
func (c *WarmupCache) GetAndEvictBranch(prefix []byte) ([]byte, bool) {
	entry, found := c.branches.Get(prefix)
	if !found {
		c.branchMisses.Add(1)
		return nil, false
	}
	if entry.isEvicted.Load() {
		c.branchEvicted.Add(1)
		return nil, false
	}
	entry.isEvicted.Store(true)
	c.branchHits.Add(1)
	c.branchBytesServed.Add(uint64(len(entry.data)))
	return entry.data, true
}

// EvictBranch marks a branch entry as evicted without retrieving it.
func (c *WarmupCache) EvictBranch(prefix []byte) {
	entry, found := c.branches.Get(prefix)
	if found {
		entry.isEvicted.Store(true)
	}
}

// PutAccount stores account data in the cache.
func (c *WarmupCache) PutAccount(plainKey []byte, update *Update) {
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	c.accounts.Set(plainKey, &accountEntry{update: updateCopy})
}

// GetAccount retrieves account data from the cache.
func (c *WarmupCache) GetAccount(plainKey []byte) (*Update, bool) {
	entry, found := c.accounts.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		c.accountMisses.Add(1)
		return nil, false
	}
	c.accountHits.Add(1)
	return entry.update, true
}

// GetAndEvictAccount retrieves account data and marks the entry as evicted in one operation.
// Returns the entry pointer allowing the caller to read the data before it's considered evicted.
func (c *WarmupCache) GetAndEvictAccount(plainKey []byte) *accountEntry {
	entry, found := c.accounts.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil
	}
	entry.isEvicted.Store(true)
	return entry
}

// EvictAccount marks an account entry as evicted without retrieving it.
func (c *WarmupCache) EvictAccount(plainKey []byte) {
	entry, found := c.accounts.Get(plainKey)
	if found {
		entry.isEvicted.Store(true)
	}
}

// PutStorage stores storage data in the cache.
func (c *WarmupCache) PutStorage(plainKey []byte, update *Update) {
	var updateCopy *Update
	if update != nil {
		updateCopy = update.Copy()
	}

	c.storage.Set(plainKey, &storageEntry{update: updateCopy})
}

// GetStorage retrieves storage data from the cache.
func (c *WarmupCache) GetStorage(plainKey []byte) (*Update, bool) {
	entry, found := c.storage.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		c.storageMisses.Add(1)
		return nil, false
	}
	c.storageHits.Add(1)
	return entry.update, true
}

// GetAndEvictStorage retrieves storage data and marks the entry as evicted in one operation.
// Returns the entry pointer allowing the caller to read the data before it's considered evicted.
func (c *WarmupCache) GetAndEvictStorage(plainKey []byte) *storageEntry {
	entry, found := c.storage.Get(plainKey)
	if !found || entry.isEvicted.Load() {
		return nil
	}
	entry.isEvicted.Store(true)
	return entry
}

// EvictStorage marks a storage entry as evicted without retrieving it.
func (c *WarmupCache) EvictStorage(plainKey []byte) {
	entry, found := c.storage.Get(plainKey)
	if found {
		entry.isEvicted.Store(true)
	}
}

// EvictPlainKey evicts a key from both accounts and storage caches.
// Use this when you don't know if the key is an account or storage key.
func (c *WarmupCache) EvictPlainKey(plainKey []byte) {
	if entry, found := c.accounts.Get(plainKey); found {
		entry.isEvicted.Store(true)
	}
	if entry, found := c.storage.Get(plainKey); found {
		entry.isEvicted.Store(true)
	}
}

// Clear clears all cached data and resets stats counters.
func (c *WarmupCache) Clear() {
	c.branches = maphash.NewMap[*branchEntry]()
	c.accounts = maphash.NewMap[*accountEntry]()
	c.storage = maphash.NewMap[*storageEntry]()
	c.ResetStats()
}

// Stats returns a one-line summary of cache hit/miss counters.
// Format matches the per-block log line: branch, account, storage with
// hit-percentages and bytes served. Useful in commitment debug logs and
// for the per-Process LogCommitments line.
func (c *WarmupCache) Stats() string {
	bh, bm, be := c.branchHits.Load(), c.branchMisses.Load(), c.branchEvicted.Load()
	bb := c.branchBytesServed.Load()
	ah, am := c.accountHits.Load(), c.accountMisses.Load()
	sh, sm := c.storageHits.Load(), c.storageMisses.Load()
	pct := func(hit, miss uint64) float64 {
		total := hit + miss
		if total == 0 {
			return 0
		}
		return 100.0 * float64(hit) / float64(total)
	}
	return fmt.Sprintf(
		"branch hit=%d miss=%d evict=%d (%.1f%%, %.1f MiB) | acct hit=%d miss=%d (%.1f%%) | stor hit=%d miss=%d (%.1f%%)",
		bh, bm, be, pct(bh, bm), float64(bb)/1024/1024,
		ah, am, pct(ah, am),
		sh, sm, pct(sh, sm),
	)
}

// ResetStats zeros out all hit/miss/byte counters without touching cached data.
func (c *WarmupCache) ResetStats() {
	c.branchHits.Store(0)
	c.branchMisses.Store(0)
	c.branchEvicted.Store(0)
	c.branchBytesServed.Store(0)
	c.accountHits.Store(0)
	c.accountMisses.Store(0)
	c.storageHits.Store(0)
	c.storageMisses.Store(0)
}
