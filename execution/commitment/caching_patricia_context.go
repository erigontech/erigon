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

// Reset clears all cached entries from all three maps.
func (c *CachingPatriciaContext) Reset() {
	c.branches.Clear()
	c.accounts.Clear()
	c.storage.Clear()
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
		return entry.data, entry.step, nil
	}

	data, step, err := v.underlying.Branch(prefix)
	if err != nil {
		return nil, 0, err
	}

	// Cache the result. Copy data to avoid issues with buffer reuse.
	var dataCopy []byte
	if data != nil {
		dataCopy = make([]byte, len(data))
		copy(dataCopy, data)
	}
	v.cache.branches.Set(prefix, branchCacheEntry{data: dataCopy, step: step})
	return dataCopy, step, nil
}

func (v *cachedView) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	// Invalidate the cache entry so the next read sees fresh data.
	v.cache.branches.Delete(prefix)
	return v.underlying.PutBranch(prefix, data, prevData)
}

func (v *cachedView) Account(plainKey []byte) (*Update, error) {
	if update, ok := v.cache.accounts.Get(plainKey); ok {
		return update, nil
	}

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
		return update, nil
	}

	update, err := v.underlying.Storage(plainKey)
	if err != nil {
		return nil, err
	}

	cached := update.Copy()
	v.cache.storage.Set(plainKey, cached)
	return cached, nil
}
