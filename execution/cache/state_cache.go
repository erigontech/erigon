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

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

const (
	// DefaultAccountCacheBytes is the byte limit for account cache (1 GB)
	DefaultAccountCacheBytes = 1 * datasize.GB
	// DefaultStorageCacheBytes is the byte limit for storage cache (1 GB)
	DefaultStorageCacheBytes = 1 * datasize.GB
	// DefaultCommitmentCacheBytes is the byte limit for commitment cache (128 MB)
	DefaultCommitmentCacheBytes = 128 * datasize.MB
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
func NewStateCache(accountBytes, storageBytes, codeBytes, addrBytes, commitmentBytes datasize.ByteSize) *StateCache {
	sc := &StateCache{}
	sc.caches[kv.AccountsDomain] = NewDomainCache(accountBytes)
	sc.caches[kv.StorageDomain] = NewDomainCache(storageBytes)
	sc.caches[kv.CodeDomain] = NewCodeCache(codeBytes, addrBytes)
	//sc.caches[kv.CommitmentDomain] = NewDomainCache(commitmentBytes)
	return sc
}

// NewDefaultStateCache creates a new StateCache with default byte capacities.
// Account: 256MB, Storage: 512MB, Code: 512MB, Addr: 16MB
func NewDefaultStateCache() *StateCache {
	return NewStateCache(
		DefaultAccountCacheBytes,
		DefaultStorageCacheBytes,
		DefaultCodeCacheBytes,
		DefaultAddrCacheBytes,
		DefaultCommitmentCacheBytes,
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

// GetCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss or
// when the code domain cache is not a CodeCache (defensive fallback).
//
// Use when the caller has the codeHash in hand (post-account-load) — typical
// for EXTCODESIZE / EXTCODEHASH / CALL targets. Lets many-addrs-one-code
// patterns (proxies, factory clones, ERC-20 holders) share a single L2b
// entry.
func (c *StateCache) GetCodeByHash(ethHash []byte) ([]byte, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return nil, false
	}
	return cc.GetByEthHash(ethHash)
}

// PutCodeWithHash stores code populating both the addr-keyed path and the
// ethHash-keyed L2b layer. Callers should prefer this over Put when they
// have the codeHash from the account record — avoids a redundant keccak.
func (c *StateCache) PutCodeWithHash(addr, code, ethHash []byte) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutWithEthHash(addr, common.Copy(code), ethHash)
}

// GetCodeSizeByHash returns the size of code by its Ethereum codeHash
// without loading the bytes. Returns (0, false) when the size-only layer
// is not populated for this hash.
func (c *StateCache) GetCodeSizeByHash(ethHash []byte) (int, bool) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return 0, false
	}
	return cc.GetCodeSizeByEthHash(ethHash)
}

// PutCodeSizeByHash records the code size for a given ethHash. Useful when
// the caller has the size in hand (e.g. from an account-domain probe that
// resolved a sibling addr to the same code) but doesn't have the bytes.
func (c *StateCache) PutCodeSizeByHash(ethHash []byte, size int) {
	cc, ok := c.caches[kv.CodeDomain].(*CodeCache)
	if !ok {
		return
	}
	cc.PutCodeSizeByEthHash(ethHash, size)
}

// Put stores data for the given domain and key.
func (c *StateCache) Put(domain kv.Domain, key []byte, value []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	if domain == kv.CommitmentDomain && bytes.Equal(key, commitmentdb.KeyCommitmentState) {
		return
	}
	cache.Put(key, common.Copy(value))
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

// ValidateAndPrepare validates and prepares all caches for a new block.
// Returns true if all caches were valid, false if any were cleared.
func (c *StateCache) ValidateAndPrepare(parentHash common.Hash, incomingBlockHash common.Hash) bool {
	allValid := true
	for _, cache := range c.caches {
		if cache != nil {
			if !cache.ValidateAndPrepare(parentHash, incomingBlockHash) {
				allValid = false
			}
		}
	}
	return allValid
}

// ClearWithHash clears all caches and sets their block hash.
func (c *StateCache) ClearWithHash(hash common.Hash) {
	for _, cache := range c.caches {
		if cache != nil {
			cache.ClearWithHash(hash)
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
	if commit, ok := c.caches[kv.CommitmentDomain].(*DomainCache); ok {
		commit.PrintStatsAndReset("Commitment")
	}
	if code, ok := c.caches[kv.CodeDomain].(*CodeCache); ok {
		code.PrintStatsAndReset()
	}
}

func (c *StateCache) RevertWithDiffset(diffset *[6][]kv.DomainEntryDiff, revertFromHash, newBlockHash common.Hash) {
	// If the cache's block hash doesn't match the block we're unwinding from,
	// the cache was modified by a rolled-back tx (e.g. ValidatePayload).
	// Clear everything and set the new hash — surgical eviction can't fix stale data
	// from a different execution path.
	for _, cache := range c.caches {
		if cache != nil && cache.GetBlockHash() != revertFromHash {
			c.ClearWithHash(newBlockHash)
			return
		}
	}

	for _, entry := range diffset[kv.AccountsDomain] {
		k := []byte(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.CodeDomain, k)
		c.Delete(kv.AccountsDomain, k)
	}
	for _, entry := range diffset[kv.CodeDomain] {
		k := []byte(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.CodeDomain, k)
	}
	for _, entry := range diffset[kv.StorageDomain] {
		k := []byte(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.StorageDomain, k)
	}
	for _, entry := range diffset[kv.CommitmentDomain] {
		k := []byte(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.CommitmentDomain, k)
	}
	// Update block hash after successful surgical eviction
	for _, cache := range c.caches {
		if cache != nil {
			cache.SetBlockHash(newBlockHash)
		}
	}
}
