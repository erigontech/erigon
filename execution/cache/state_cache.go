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
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
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
	sc.caches[kv.AccountsDomain] = NewBytesCache(accountBytes)
	sc.caches[kv.StorageDomain] = NewBytesCache(storageBytes)
	sc.caches[kv.CodeDomain] = NewCodeCache(codeBytes, addrBytes)
	sc.caches[kv.CommitmentDomain] = NewBytesCache(commitmentBytes)
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
func (c *StateCache) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	cache := c.caches[domain]
	if kv.AccountsDomain == domain && bytes.Equal(key, common.Hex2Bytes("0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29")) {
		fmt.Println("READ 0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29")
	}
	if cache == nil {
		return nil, false
	}
	ret, ok := cache.Get(key)
	if kv.AccountsDomain == domain && bytes.Equal(key, common.Hex2Bytes("0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29")) {
		fmt.Println("GOT 0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29", "ok:", ok, "len:", len(ret), "ret:", common.Bytes2Hex(ret))
	}
	return ret, ok
}

// Put stores data for the given domain and key.
func (c *StateCache) Put(domain kv.Domain, key []byte, value []byte) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	if kv.AccountsDomain == domain && bytes.Equal(key, common.Hex2Bytes("0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29")) {
		fmt.Println("PUT 0xdBBE3D8c2d2b22A2611c5A94A9a12C2fCD49Eb29", "value len:", len(value), "value:", common.Bytes2Hex(value))
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

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

// PrintStatsAndReset prints cache statistics for all domains and resets counters.
func (c *StateCache) PrintStatsAndReset() {
	if acc, ok := c.caches[kv.AccountsDomain].(*GenericCache[[]byte]); ok {
		acc.PrintStatsAndReset("Account")
	}
	if stor, ok := c.caches[kv.StorageDomain].(*GenericCache[[]byte]); ok {
		stor.PrintStatsAndReset("Storage")
	}
	if stor, ok := c.caches[kv.CommitmentDomain].(*GenericCache[[]byte]); ok {
		stor.PrintStatsAndReset("Commitment")
	}
	if code, ok := c.caches[kv.CodeDomain].(*CodeCache); ok {
		code.PrintStatsAndReset()
	}
}

func (c *StateCache) RevertWithDiffset(diffset *[6][]kv.DomainEntryDiff, newBlockHash common.Hash) {
	for _, entry := range diffset[kv.AccountsDomain] {
		k := toBytesZeroCopy(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.CodeDomain, k)
		c.Put(kv.AccountsDomain, k, entry.Value)
	}
	for _, entry := range diffset[kv.CodeDomain] {
		k := toBytesZeroCopy(entry.Key[:len(entry.Key)-8])
		c.Delete(kv.CodeDomain, k)
	}
	for _, entry := range diffset[kv.StorageDomain] {
		k := toBytesZeroCopy(entry.Key[:len(entry.Key)-8])
		c.Put(kv.StorageDomain, k, entry.Value)
	}
	// still adding kv.CommitmentDomain for expandability.
	for _, entry := range diffset[kv.CommitmentDomain] {
		k := toBytesZeroCopy(entry.Key[:len(entry.Key)-8])
		c.Put(kv.CommitmentDomain, k, entry.Value)
	}
	// Update block hash on all caches after unwind so ValidateAndPrepare works correctly
	for _, cache := range c.caches {
		if cache != nil {
			cache.SetBlockHash(newBlockHash)
		}
	}
}
