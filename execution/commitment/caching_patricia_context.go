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
	"github.com/erigontech/erigon/db/kv"
)

// branchCacheEntry holds cached branch data along with its step.
type branchCacheEntry struct {
	data []byte
	step kv.Step
}

// CachingPatriciaContext is a shared, concurrent read-through cache layer
// for PatriciaContext. It stores branch, account, and storage data in
// three maphash.Map instances which are safe for concurrent access.
//
// Usage: create one CachingPatriciaContext per commitment cycle, call
// Wrap(underlying) to get a cachedView for each worker, then Reset()
// after the cycle completes.
type CachingPatriciaContext struct {
	branches *maphash.Map[branchCacheEntry]
	accounts *maphash.Map[*Update]
	storage  *maphash.Map[*Update]

	// Hit/miss counters for observability (atomic for concurrent access).
	branchHits    atomic.Uint64
	branchMisses  atomic.Uint64
	accountHits   atomic.Uint64
	accountMisses atomic.Uint64
	storageHits   atomic.Uint64
	storageMisses atomic.Uint64
}

// CacheStats holds cache hit/miss statistics for a CachingPatriciaContext.
type CacheStats struct {
	BranchHits    uint64
	BranchMisses  uint64
	AccountHits   uint64
	AccountMisses uint64
	StorageHits   uint64
	StorageMisses uint64
}

// HitRate returns the overall hit rate across all three cache types (0.0 to 1.0).
// Returns 0 if no lookups have been performed.
func (s CacheStats) HitRate() float64 {
	hits := s.BranchHits + s.AccountHits + s.StorageHits
	total := hits + s.BranchMisses + s.AccountMisses + s.StorageMisses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// String returns a human-readable summary of the cache statistics.
func (s CacheStats) String() string {
	return fmt.Sprintf("branches(hit=%d miss=%d) accounts(hit=%d miss=%d) storage(hit=%d miss=%d) overall=%.1f%%",
		s.BranchHits, s.BranchMisses,
		s.AccountHits, s.AccountMisses,
		s.StorageHits, s.StorageMisses,
		s.HitRate()*100)
}

// NewCachingPatriciaContext creates a new CachingPatriciaContext with
// empty cache maps.
func NewCachingPatriciaContext() *CachingPatriciaContext {
	return &CachingPatriciaContext{
		branches: maphash.NewMap[branchCacheEntry](),
		accounts: maphash.NewMap[*Update](),
		storage:  maphash.NewMap[*Update](),
	}
}

// Stats returns a snapshot of the cache hit/miss counters.
func (c *CachingPatriciaContext) Stats() CacheStats {
	return CacheStats{
		BranchHits:    c.branchHits.Load(),
		BranchMisses:  c.branchMisses.Load(),
		AccountHits:   c.accountHits.Load(),
		AccountMisses: c.accountMisses.Load(),
		StorageHits:   c.storageHits.Load(),
		StorageMisses: c.storageMisses.Load(),
	}
}

// Reset clears all cached entries from all three maps and resets counters.
func (c *CachingPatriciaContext) Reset() {
	c.branches.Clear()
	c.accounts.Clear()
	c.storage.Clear()
	c.branchHits.Store(0)
	c.branchMisses.Store(0)
	c.accountHits.Store(0)
	c.accountMisses.Store(0)
	c.storageHits.Store(0)
	c.storageMisses.Store(0)
}

// Wrap returns a cachedView that implements PatriciaContext. Reads check
// the shared cache first, falling through to the underlying context on
// miss and populating the cache. Writes (PutBranch) pass through to the
// underlying context and invalidate the cache entry.
func (c *CachingPatriciaContext) Wrap(underlying PatriciaContext) PatriciaContext {
	return &cachedView{
		cache:      c,
		underlying: underlying,
	}
}

// cachedView implements PatriciaContext with read-through caching backed
// by the shared CachingPatriciaContext.
type cachedView struct {
	cache      *CachingPatriciaContext
	underlying PatriciaContext
}

func (v *cachedView) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if entry, ok := v.cache.branches.Get(prefix); ok {
		v.cache.branchHits.Add(1)
		return entry.data, entry.step, nil
	}
	v.cache.branchMisses.Add(1)

	data, step, err := v.underlying.Branch(prefix)
	if err != nil {
		return nil, 0, err
	}

	// Copy data to avoid issues with buffer reuse.
	var dataCopy []byte
	if data != nil {
		dataCopy = make([]byte, len(data))
		copy(dataCopy, data)
	}
	// Use LoadOrStore so that a concurrent PutBranch (which uses Set) is
	// never overwritten by a slower warmup worker that read stale data
	// from its read-only DB snapshot.
	entry, _ := v.cache.branches.LoadOrStore(prefix, branchCacheEntry{data: dataCopy, step: step})
	return entry.data, entry.step, nil
}

func (v *cachedView) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	err := v.underlying.PutBranch(prefix, data, prevData)
	if err != nil {
		return err
	}
	// Update the cache with the new data so the next Branch() read is a hit.
	var dataCopy []byte
	if data != nil {
		dataCopy = make([]byte, len(data))
		copy(dataCopy, data)
	}
	v.cache.branches.Set(prefix, branchCacheEntry{data: dataCopy, step: 0})
	return nil
}

func (v *cachedView) Account(plainKey []byte) (*Update, error) {
	if update, ok := v.cache.accounts.Get(plainKey); ok {
		v.cache.accountHits.Add(1)
		return update, nil
	}
	v.cache.accountMisses.Add(1)

	update, err := v.underlying.Account(plainKey)
	if err != nil {
		return nil, err
	}

	// Cache a copy to avoid aliasing with caller buffers.
	cached := update.Copy()
	v.cache.accounts.Set(plainKey, cached)
	return cached, nil
}

func (v *cachedView) Storage(plainKey []byte) (*Update, error) {
	if update, ok := v.cache.storage.Get(plainKey); ok {
		v.cache.storageHits.Add(1)
		return update, nil
	}
	v.cache.storageMisses.Add(1)

	update, err := v.underlying.Storage(plainKey)
	if err != nil {
		return nil, err
	}

	cached := update.Copy()
	v.cache.storage.Set(plainKey, cached)
	return cached, nil
}
