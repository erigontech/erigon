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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
)

// Test helpers
func closeOnCleanup[T interface{ Close() }](tb testing.TB, c T) T {
	tb.Helper()
	tb.Cleanup(c.Close)
	return c
}

// These helpers bypass the public fill and publication protocols so tests can
// exercise the underlying entry and frontier mechanics directly.
func (c *StateCache) put(domain kv.Domain, key []byte, value []byte, txNum uint64) {
	cache := c.caches[domain]
	if cache == nil {
		return
	}
	cache.Put(key, bytes.Clone(value), txNum)
}

func (c *StateCache) apply(domain kv.Domain, key, value []byte, txNum uint64) {
	prepared := prepareStateUpdate(StateUpdate{Domain: domain, Key: key, Value: value, TxNum: txNum})
	c.applierMu.Lock()
	defer c.applierMu.Unlock()
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.applyPrepared(prepared)
}

func makeAddr(i int) []byte {
	addr := make([]byte, 20)
	addr[19] = byte(i)
	return addr
}

type blockingPutCache struct {
	started chan struct{}
	release chan struct{}
	filled  chan struct{}
	once    sync.Once
}

func (c *blockingPutCache) Get([]byte) ([]byte, bool)                  { return nil, false }
func (c *blockingPutCache) GetWithTxNum([]byte) ([]byte, uint64, bool) { return nil, 0, false }
func (c *blockingPutCache) Put([]byte, []byte, uint64) {
	c.once.Do(func() { close(c.started) })
	<-c.release
}
func (c *blockingPutCache) PutIfAbsent([]byte, []byte, uint64) { c.filled <- struct{}{} }
func (c *blockingPutCache) Delete([]byte)                      {}
func (c *blockingPutCache) Clear()                             {}
func (c *blockingPutCache) Unwind(uint64)                      {}
func (c *blockingPutCache) Close()                             {}
func (c *blockingPutCache) Len() int                           { return 0 }

func makeHash(i int) common.Hash {
	var h common.Hash
	h[31] = byte(i)
	return h
}

func makeCode(i int) []byte {
	return []byte{0x60, 0x00, byte(i)} // PUSH1 0x00 + unique byte
}

func makeValue(i int) []byte {
	return []byte{byte(i), byte(i + 1), byte(i + 2)}
}

func frontierAt(end uint64) Frontier {
	return FrontierFunc(func(kv.Domain) (uint64, bool) { return end, true })
}

func frontierAtVersion(end, stateVersion uint64) Frontier {
	return FrontierWithStateVersion(frontierAt(end), stateVersion)
}

// =============================================================================
// DomainCache Tests
// =============================================================================

func TestDomainCache_NewWithByteCapacity(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU)) // 1MB
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.SizeBytes())
	assert.Equal(t, 1*datasize.MB, c.CapacityBytes())
}

func TestDomainCache_GetPut(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))

	addr := makeAddr(1)
	value := makeValue(1)

	// Get non-existent
	v, ok := c.Get(addr)
	assert.False(t, ok)
	assert.Nil(t, v)

	// Put and Get
	c.Put(addr, value, 0)
	v, ok = c.Get(addr)
	assert.True(t, ok)
	assert.Equal(t, value, v)
	assert.Equal(t, 1, c.Len())
}

func TestDomainCache_PutUpdateValue(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))

	addr := makeAddr(1)
	value1 := []byte{1, 2, 3, 4, 5, 6, 7, 8} // 8 bytes
	value2 := []byte{9, 10, 11}              // 3 bytes

	c.Put(addr, value1, 0)

	// Update with different value
	c.Put(addr, value2, 0)
	v, ok := c.Get(addr)

	assert.True(t, ok)
	assert.Equal(t, value2, v)
}

func TestDomainCache_PutCapacityLimit_NoOpMode(t *testing.T) {
	// ModeNoOp keeps the historical fill-and-freeze behaviour: once
	// full, new keys are silently dropped. Counted via the dropped metric.
	// Entry overhead is 20 (addr key) + 3 (value) + 24 = 47 bytes per entry.
	// Two entries take 94 bytes; cap at 100 leaves no room for a third.
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeNoOp))

	c.Put(makeAddr(1), makeValue(1), 0)
	c.Put(makeAddr(2), makeValue(2), 0)
	assert.Equal(t, 2, c.Len())

	c.Put(makeAddr(3), makeValue(3), 0)
	assert.Equal(t, 2, c.Len())

	_, ok := c.Get(makeAddr(3))
	assert.False(t, ok)

	// Updating an existing key always succeeds in either mode.
	newValue := []byte{100, 101, 102}
	c.Put(makeAddr(1), newValue, 0)
	v, ok := c.Get(makeAddr(1))
	assert.True(t, ok)
	assert.Equal(t, newValue, v)
}

func TestDomainCache_PutEvictsWhenFull_EvictMode(t *testing.T) {
	// ModeEvictLRU lets the per-shard LRU evict on insert when its
	// entry-count cap is reached. Eviction is per-shard, not globally
	// LRU (a known trade-off of freelru.ShardedLRU; see policy.go).
	//
	// Build with capacityEntries=2 so that a third insert forces an
	// eviction event. Capacity-bytes is unused for the eviction
	// decision and is only carried for telemetry.
	c := &DomainCache{
		GenericCache: newGenericCacheEntries[[]byte](1<<20, 2, func(v []byte) int { return len(v) }, ModeEvictLRU),
	}

	for i := 1; i <= 64; i++ {
		c.Put(makeAddr(i), makeValue(i), 0)
	}
	// The newest entry must still be findable.
	v, ok := c.Get(makeAddr(64))
	assert.True(t, ok, "newest key must be present after eviction")
	assert.Equal(t, makeValue(64), v)

	// At least some early entries must have been evicted by now.
	missingCount := 0
	for i := 1; i <= 32; i++ {
		if _, ok := c.Get(makeAddr(i)); !ok {
			missingCount++
		}
	}
	assert.Positive(t, missingCount, "ModeEvictLRU should have evicted some early entries")
}

func TestDomainCache_Delete(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))

	addr := makeAddr(1)
	c.Put(addr, makeValue(1), 0)
	assert.Equal(t, 1, c.Len())

	c.Delete(addr)
	assert.Equal(t, 0, c.Len())

	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestDomainCache_Clear(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))

	c.Put(makeAddr(1), makeValue(1), 0)
	c.Put(makeAddr(2), makeValue(2), 0)
	assert.Equal(t, 2, c.Len())

	c.Clear()
	assert.Equal(t, 0, c.Len())
}

func TestDomainCache_PrintStatsAndReset(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))

	// Generate some hits and misses
	c.Put(makeAddr(1), makeValue(1), 0)
	c.Get(makeAddr(1)) // hit
	c.Get(makeAddr(1)) // hit
	c.Get(makeAddr(2)) // miss

	// Should not panic
	c.PrintStatsAndReset("test")

	// Stats should be reset - next Get should count fresh
	c.Get(makeAddr(1)) // hit after reset
}

func TestDomainCache_PrintStatsAndReset_NoOps(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(100, ModeEvictLRU))
	// No operations - should handle zero total gracefully
	c.PrintStatsAndReset("test")
}

func TestDomainCache_ImplementsInterface(t *testing.T) {
	var _ Cache = (*DomainCache)(nil)
}

// =============================================================================
// CodeCache Tests
// =============================================================================

func TestCodeCache_NewDefaultCodeCache(t *testing.T) {
	c := closeOnCleanup(t, NewDefaultCodeCache())
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 0, c.CodeLen())
}

func TestCodeCache_GetPut(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	addr := makeAddr(1)
	code := makeCode(1)

	// Get non-existent
	v, ok := c.Get(addr)
	assert.False(t, ok)
	assert.Nil(t, v)

	// Put and Get
	c.Put(addr, code, 0)
	v, ok = c.Get(addr)
	assert.True(t, ok)
	assert.Equal(t, code, v)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, 1, c.CodeLen())
}

func TestCodeCache_PutEmptyCode(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	addr := makeAddr(1)
	c.Put(addr, []byte{}, 0)

	// Should not store empty code
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 0, c.CodeLen())
}

func TestCodeCache_CodeDeduplication(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	code := makeCode(1)
	addr1 := makeAddr(1)
	addr2 := makeAddr(2)
	addr3 := makeAddr(3)

	// Three addresses with same code
	c.Put(addr1, code, 0)
	c.Put(addr2, code, 0)
	c.Put(addr3, code, 0)

	// Should have 3 address mappings but only 1 code entry
	assert.Equal(t, 3, c.Len())
	assert.Equal(t, 1, c.CodeLen())

	// All should return the same code
	v1, _ := c.Get(addr1)
	v2, _ := c.Get(addr2)
	v3, _ := c.Get(addr3)
	assert.Equal(t, code, v1)
	assert.Equal(t, code, v2)
	assert.Equal(t, code, v3)
}

func TestCodeCache_AddrCapacityLimit(t *testing.T) {
	// addrToHash is an LRU keyed by 20-byte address. Verify eviction is
	// LRU rather than no-op-when-full so fresh-address workloads warm up.
	// makeAddr / makeCode wrap at 256, so we generate addrs/codes from
	// a wider 16-bit space directly.
	wideAddr := func(i int) []byte {
		a := make([]byte, 20)
		a[18] = byte(i >> 8)
		a[19] = byte(i)
		return a
	}
	wideCode := func(i int) []byte {
		return []byte{0x60, byte(i >> 8), byte(i)}
	}

	c := closeOnCleanup(t, NewCodeCache(1024*1024, 1024*28)) // 1MB code, ~1024 addr LRU entries
	for i := range 1100 {
		c.Put(wideAddr(i), wideCode(i), 0)
	}

	// Len should be exactly the LRU cap (1024), not silently truncated to 0.
	assert.Equal(t, 1024, c.Len())

	// Oldest entries (addrs 0..75) must have been evicted.
	_, ok := c.Get(wideAddr(0))
	assert.False(t, ok, "oldest entry should be evicted by LRU")
	_, ok = c.Get(wideAddr(50))
	assert.False(t, ok, "second-oldest range should be evicted by LRU")

	// Most recent entry must be present.
	v, ok := c.Get(wideAddr(1099))
	assert.True(t, ok, "most recent entry should remain")
	assert.Equal(t, wideCode(1099), v)

	// hashToCode now LRU-evicts at its own entry cap (codeCapacityB /
	// avgCodeEntryBytes), so it holds far fewer than the 1100 distinct codes
	// rather than growing unbounded.
	assert.Less(t, c.CodeLen(), 1100)

	// Updating an existing addr re-writes the entry (LRU promotes to MRU).
	c.Put(wideAddr(1099), wideCode(4242), 0)
	v, ok = c.Get(wideAddr(1099))
	assert.True(t, ok)
	assert.Equal(t, wideCode(4242), v)
}

func TestCodeCache_CodeCapacityLimit(t *testing.T) {
	// Tiny byte budget → a 1-entry code layer cap. Successive distinct codes
	// LRU-evict the coldest rather than freezing the layer.
	c := closeOnCleanup(t, NewCodeCache(25, 1024*1024)) // 25 bytes code, 1MB addr

	c.Put(makeAddr(1), makeCode(1), 0)
	c.Put(makeAddr(2), makeCode(2), 0)
	c.Put(makeAddr(3), makeCode(3), 0)

	// Addr LRU keeps all three mappings (1MB); the code layer holds only the
	// most-recent code(s) after eviction.
	assert.Equal(t, 3, c.Len())
	assert.LessOrEqual(t, c.CodeLen(), 1)

	// Newest code is retrievable; the coldest was evicted from the code layer.
	v, ok := c.Get(makeAddr(3))
	assert.True(t, ok)
	assert.Equal(t, makeCode(3), v)
	_, ok = c.Get(makeAddr(1))
	assert.False(t, ok, "coldest code should have been evicted")
}

func TestCodeCache_Delete(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	addr := makeAddr(1)
	code := makeCode(1)
	c.Put(addr, code, 0)

	c.Delete(addr)
	assert.Equal(t, 0, c.Len())
	// Code should still exist (immutable)
	assert.Equal(t, 1, c.CodeLen())

	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestCodeCache_Clear(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	c.Put(makeAddr(1), makeCode(1), 0)
	c.Put(makeAddr(2), makeCode(2), 0)

	c.Clear()
	assert.Equal(t, 0, c.Len())
	// Clear hard-resets every layer: unwound/cleared code must not remain
	// discoverable, so the content layer is dropped too.
	assert.Equal(t, 0, c.CodeLen())
}

func TestCodeCache_PrintStatsAndReset(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))

	c.Put(makeAddr(1), makeCode(1), 0)
	c.Get(makeAddr(1)) // hit
	c.Get(makeAddr(2)) // miss

	// Should not panic
	c.PrintStatsAndReset()
}

func TestCodeCache_PrintStatsAndReset_NoOps(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(100, 200))
	// No operations - should handle zero total gracefully
	c.PrintStatsAndReset()
}

func TestCodeCache_GetMissingCode(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(1024*1024, 1024*1024)) // 1MB each

	// Manually set addr mapping without code (simulates capacity limit scenario)
	addr := makeAddr(1)
	code := makeCode(1)
	c.Put(addr, code, 0)

	// Clear the code cache but keep addr mapping
	c.hashToCode.Purge()
	c.codeSize.Store(0)

	// Get should fail at code lookup stage
	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestCodeCache_ImplementsInterface(t *testing.T) {
	var _ Cache = (*CodeCache)(nil)
}

// =============================================================================
// StateCache Tests
// =============================================================================

func TestStateCache_NewStateCache(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(10, 20, 30, 40))
	require.NotNil(t, c)

	// Account, Storage, Code should be initialized
	assert.NotNil(t, c.getCache(kv.AccountsDomain))
	assert.NotNil(t, c.getCache(kv.StorageDomain))
	assert.NotNil(t, c.getCache(kv.CodeDomain))

	// Other domains should be nil
	assert.Nil(t, c.getCache(kv.ReceiptDomain))
	assert.Nil(t, c.getCache(kv.RCacheDomain))
}

func TestStateCache_NewDefaultStateCache(t *testing.T) {
	c := closeOnCleanup(t, NewDefaultStateCache())
	require.NotNil(t, c)

	assert.NotNil(t, c.getCache(kv.AccountsDomain))
	assert.NotNil(t, c.getCache(kv.StorageDomain))
	assert.NotNil(t, c.getCache(kv.CodeDomain))
}

func TestStateCache_GetPut_Account(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	addr := makeAddr(1)
	value := makeValue(1)

	// Get non-existent
	v, ok := c.get(kv.AccountsDomain, addr)
	assert.False(t, ok)
	assert.Nil(t, v)

	// Put and Get
	c.put(kv.AccountsDomain, addr, value, 0)
	v, ok = c.get(kv.AccountsDomain, addr)
	assert.True(t, ok)
	assert.Equal(t, value, v)
}

func TestStateCache_GetPut_Storage(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	key := make([]byte, 52) // addr(20) + slot(32)
	copy(key, makeAddr(1))
	key[51] = 1
	value := makeValue(1)

	c.put(kv.StorageDomain, key, value, 0)
	v, ok := c.get(kv.StorageDomain, key)
	assert.True(t, ok)
	assert.Equal(t, value, v)
}

func TestStateCache_GetPut_Code(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB))

	addr := makeAddr(1)
	code := makeCode(1)

	c.put(kv.CodeDomain, addr, code, 0)
	v, ok := c.get(kv.CodeDomain, addr)
	assert.True(t, ok)
	assert.Equal(t, code, v)
}

func TestStateCache_GetPut_UnsupportedDomain(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	// ReceiptDomain is not supported
	c.put(kv.ReceiptDomain, makeAddr(1), makeValue(1), 0)
	v, ok := c.get(kv.ReceiptDomain, makeAddr(1))
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestStateCache_Delete(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	addr := makeAddr(1)
	c.put(kv.AccountsDomain, addr, makeValue(1), 0)
	c.deleteKey(kv.AccountsDomain, addr)

	_, ok := c.get(kv.AccountsDomain, addr)
	assert.False(t, ok)
}

// Put(key, nil) must be a cache hit, not a miss. SharedDomains.GetLatest
// caches deleted keys via Put(key, nil); if Get treats that as "not found",
// the caller unnecessarily falls through to the DB on every read.
func TestStateCache_PutEmpty_ThenGet_IsCacheHit(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	key := make([]byte, 52) // addr(20) + slot(32)
	key[0] = 0x1d
	key[51] = 0xa2

	c.put(kv.StorageDomain, key, nil, 0)

	v, ok := c.get(kv.StorageDomain, key)
	assert.True(t, ok, "Get after Put(nil) must be a cache hit, not a miss")
	assert.Empty(t, v, "cached value for a deleted key must be empty")
}

// Same test for []byte{} (zero-length but non-nil).
func TestStateCache_PutEmptySlice_ThenGet_IsCacheHit(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	key := make([]byte, 52)
	key[0] = 0x1d
	key[51] = 0xa2

	c.put(kv.StorageDomain, key, []byte{}, 0)

	v, ok := c.get(kv.StorageDomain, key)
	assert.True(t, ok, "Get after Put([]byte{}) must be a cache hit")
	assert.Empty(t, v)
}

func TestStateCache_Delete_UnsupportedDomain(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	// Should not panic
	c.deleteKey(kv.ReceiptDomain, makeAddr(1))
}

func TestStateCache_Clear(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	c.put(kv.AccountsDomain, makeAddr(1), makeValue(1), 0)
	c.put(kv.StorageDomain, makeAddr(2), makeValue(2), 0)
	c.put(kv.CodeDomain, makeAddr(3), makeCode(3), 0)

	c.clear()

	_, ok1 := c.get(kv.AccountsDomain, makeAddr(1))
	_, ok2 := c.get(kv.StorageDomain, makeAddr(2))
	_, ok3 := c.get(kv.CodeDomain, makeAddr(3))

	assert.False(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)
}

func TestStateCache_GetCache_OutOfBounds(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(100, 100, 100, 100))

	// Domain >= DomainLen should return nil
	cache := c.getCache(kv.DomainLen)
	assert.Nil(t, cache)

	cache = c.getCache(kv.Domain(100))
	assert.Nil(t, cache)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestDomainCache_ConcurrentAccess(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(10000, ModeEvictLRU))

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := range 100 {
			c.Put(makeAddr(i), makeValue(i), 0)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := range 100 {
			c.Get(makeAddr(i))
		}
		done <- true
	}()

	<-done
	<-done
}

func TestCodeCache_ConcurrentAccess(t *testing.T) {
	c := closeOnCleanup(t, NewCodeCache(1000, 1000))

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := range 100 {
			c.Put(makeAddr(i), makeCode(i), 0)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := range 100 {
			c.Get(makeAddr(i))
		}
		done <- true
	}()

	<-done
	<-done
}

// =============================================================================
// Data Isolation Tests
// =============================================================================

func TestStateCache_DomainIsolation(t *testing.T) {
	c := closeOnCleanup(t, NewStateCache(1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB))

	addr := makeAddr(1)
	accountData := []byte("account")
	storageData := []byte("storage")
	codeData := []byte{0x60, 0x00, 0x60, 0x00} // valid code

	c.put(kv.AccountsDomain, addr, accountData, 0)
	c.put(kv.StorageDomain, addr, storageData, 0)
	c.put(kv.CodeDomain, addr, codeData, 0)

	v1, ok1 := c.get(kv.AccountsDomain, addr)
	v2, ok2 := c.get(kv.StorageDomain, addr)
	v3, ok3 := c.get(kv.CodeDomain, addr)

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.True(t, ok3)

	assert.True(t, bytes.Equal(v1, accountData))
	assert.True(t, bytes.Equal(v2, storageData))
	assert.True(t, bytes.Equal(v3, codeData))
}

// =============================================================================
// Block Continuity Tests
// =============================================================================

// Fork-validation (engine_newPayload) of a block building on the canonical tip
// must NOT purge the hot cache when that block is subsequently applied
// canonically. Regression for the tip purge_rate bug: fork-validation advancing
// blockHash to the speculative block made the canonical apply mismatch & purge.
// Fork-validation of a block on a DIFFERENT parent (reorg proposal) must still
// purge, since cache-as-of-canonical-tip would serve incoherent reads, but it
// must not advance blockHash (canonical continues cleanly afterward).
// =============================================================================
// RevertWithDiffset Tests
// =============================================================================

// makeDiffKey creates a domain entry key with an 8-byte step suffix, matching
// the format used by DomainEntryDiff (full key = base key + inverted step).
func makeDiffKey(baseKey []byte, step uint64) string {
	k := make([]byte, len(baseKey)+8)
	copy(k, baseKey)
	// Store inverted step in the suffix (same encoding as domain tables).
	k[len(k)-8] = byte(^step >> 56)
	k[len(k)-7] = byte(^step >> 48)
	k[len(k)-6] = byte(^step >> 40)
	k[len(k)-5] = byte(^step >> 32)
	k[len(k)-4] = byte(^step >> 24)
	k[len(k)-3] = byte(^step >> 16)
	k[len(k)-2] = byte(^step >> 8)
	k[len(k)-1] = byte(^step)
	return string(k)
}

// --- txNum/epoch unwind invalidation (replaces the blockHash/diffset model) ---

// Entries stamped at/below the unwind point survive (warm hot set kept); entries
// above it from the now-dead epoch are dropped lazily on read.
func TestUnwind_KeepsBelowFloor_EvictsAbove(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	below := makeAddr(1)
	above := makeAddr(2)
	c.Put(below, makeValue(1), 50)  // predates the unwind
	c.Put(above, makeValue(2), 150) // written in the unwound range

	c.Unwind(100) // floor=100 (first unwound txNum): keep <100, drop >=100

	v, ok := c.Get(below)
	assert.True(t, ok, "entry below the unwind point must stay warm")
	assert.Equal(t, makeValue(1), v)

	_, ok = c.Get(above)
	assert.False(t, ok, "entry above the unwind point must be invalidated")
	assert.Equal(t, 1, c.Len(), "the stale entry is evicted lazily on its read")
}

// Pins the unwind floor boundary: unwindToTxNum is the FIRST rolled-back txNum,
// so an entry stamped at exactly that txNum is dead-fork state and must be
// evicted — the drop rule is txNum >= floor, not txNum > floor.
func TestUnwind_EvictsEntryAtFloor(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	atFloor := makeAddr(1)
	belowFloor := makeAddr(2)
	c.Put(atFloor, makeValue(1), 100)   // first txNum of the first unwound block
	c.Put(belowFloor, makeValue(2), 99) // last txNum of the surviving block

	c.Unwind(100) // floor=100, epoch->1

	_, ok := c.Get(atFloor)
	assert.False(t, ok, "entry at txNum==floor is on the dead fork and must be evicted")

	v, ok := c.Get(belowFloor)
	assert.True(t, ok, "entry at txNum==floor-1 predates the unwind and must stay warm")
	assert.Equal(t, makeValue(2), v)
}

// The reused-txNum case: after an unwind, the live fork re-writes a key at the
// SAME txNum as the dead fork's write. The epoch — not the txNum — distinguishes
// them, so the dead entry reads stale and the re-written one reads valid.
func TestUnwind_ReusedTxNumDisambiguatedByEpoch(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	k := makeAddr(1)
	c.Put(k, makeValue(1), 150) // dead fork, epoch 0

	c.Unwind(100) // epoch -> 1, floor -> 100

	_, ok := c.Get(k)
	assert.False(t, ok, "dead-fork entry (old epoch, above floor) reads stale")

	c.Put(k, makeValue(2), 150) // live fork re-writes at the same txNum, epoch 1
	v, ok := c.Get(k)
	assert.True(t, ok, "live-fork entry at the same txNum is valid (current epoch)")
	assert.Equal(t, makeValue(2), v)
}

// A straggler the live fork never re-writes must not resurrect: it stays in a
// dead epoch above the floor and reads stale no matter how far execution
// advances afterwards (there is no rising high-water mark to re-validate it).
func TestUnwind_StragglerNeverResurrects(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	straggler := makeAddr(1)
	c.Put(straggler, makeValue(1), 150) // epoch 0

	c.Unwind(100) // epoch 1

	// Advance the live fork far past the straggler's txNum (no re-write of it).
	for i := 2; i < 50; i++ {
		c.Put(makeAddr(i), makeValue(i), uint64(200+i))
	}
	_, ok := c.Get(straggler)
	assert.False(t, ok, "straggler in a dead epoch must stay stale, never resurrect")
}

// A second, shallower unwind must not resurrect entries a deeper earlier unwind
// invalidated (floor only moves down).
func TestUnwind_FloorOnlyMovesDown(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	k := makeAddr(1)
	c.Put(k, makeValue(1), 70) // epoch 0

	c.Unwind(50)  // floor 50, epoch 1 — k(70>50, epoch0) now stale
	c.Unwind(100) // shallower; floor must stay 50, not rise to 100

	_, ok := c.Get(k)
	assert.False(t, ok, "deeper unwind's floor must not be raised by a later shallower one")
}

func TestDomainCache_PutIfAbsent(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.KB, ModeEvictLRU))
	addr := makeAddr(1)
	fresh := []byte("fresh")
	stale := []byte("stale")

	// Absent → inserts.
	c.PutIfAbsent(addr, stale, 10)
	v, ok := c.Get(addr)
	require.True(t, ok)
	assert.Equal(t, stale, v)

	// Live entry → left untouched.
	c.Put(addr, fresh, 20)
	c.PutIfAbsent(addr, stale, 10)
	v, ok = c.Get(addr)
	require.True(t, ok)
	assert.Equal(t, fresh, v, "PutIfAbsent must not replace a live entry")

	// Entry below the unwind floor survives the unwind and still blocks PutIfAbsent.
	low := makeAddr(2)
	c.Put(low, fresh, 3)
	c.Unwind(5)
	c.PutIfAbsent(low, stale, 4)
	v, ok = c.Get(low)
	require.True(t, ok)
	assert.Equal(t, fresh, v)

	// Stale entry (at/above the floor, superseded epoch) → replaced.
	c.PutIfAbsent(addr, stale, 10) // addr's entry was stamped txNum 20 >= floor 5
	v, ok = c.Get(addr)
	require.True(t, ok)
	assert.Equal(t, stale, v, "PutIfAbsent must replace a stale entry")
}

func TestCodeCache_PutIfAbsentKeepsLiveAddrBinding(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(1*datasize.MB, 1*datasize.MB))
	addr := makeAddr(1)
	fresh := []byte{0xaa, 1, 2, 3}
	stale := []byte{0xbb, 4, 5, 6}

	cc.PutIfAbsent(addr, stale, 10)
	v, ok := cc.Get(addr)
	require.True(t, ok)
	assert.Equal(t, stale, v)

	cc.Put(addr, fresh, 20)
	cc.PutIfAbsent(addr, stale, 10)
	v, ok = cc.Get(addr)
	require.True(t, ok)
	assert.Equal(t, fresh, v, "PutIfAbsent must not rebind a live addr entry")

	// After an unwind marks the binding stale, PutIfAbsent may rebind.
	cc.Unwind(5)
	cc.PutIfAbsent(addr, stale, 4)
	v, ok = cc.Get(addr)
	require.True(t, ok)
	assert.Equal(t, stale, v)
}

func TestCodeCache_PutWithCodeHashIfAbsent(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(1*datasize.MB, 1*datasize.MB))
	addr := makeAddr(1)
	fresh := []byte{0xaa, 1, 2, 3}
	stale := []byte{0xbb, 4, 5, 6}
	freshHash := crypto.Keccak256(fresh)
	staleHash := crypto.Keccak256(stale)

	cc.PutWithCodeHash(addr, fresh, freshHash, 20)
	cc.PutWithCodeHashIfAbsent(addr, stale, staleHash, 10)

	v, ok := cc.Get(addr)
	require.True(t, ok)
	assert.Equal(t, fresh, v, "addr must stay bound to the fresher code")

	// The content-addressed layers are per-key-immutable and still populated.
	v, ok = cc.GetByCodeHash(staleHash)
	require.True(t, ok)
	assert.Equal(t, stale, v)
	size, ok := cc.GetCodeSizeByCodeHash(staleHash)
	require.True(t, ok)
	assert.Equal(t, len(stale), size)
}

// A conditional put must be atomic w.r.t. a concurrent unconditional Put of
// the same key: without a shared critical section the conditional writer can
// check (absent), lose the CPU to the authoritative writer's insert, then
// clobber it — the prefetch-vs-flush staleness this cache guards against.
func TestDomainCache_PutIfAbsentAtomicWithPut(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	fresh := []byte("fresh")
	stale := []byte("stale")
	addr := make([]byte, 20)
	for round := range 20000 {
		// Full-width round: the race only has teeth on a never-seen key, and
		// makeAddr would truncate it to a byte.
		binary.BigEndian.PutUint64(addr[1:], uint64(round))
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(addr, fresh, 20) })
		wg.Go(func() { c.PutIfAbsent(addr, stale, 10) })
		wg.Wait()
		v, ok := c.Get(addr)
		require.True(t, ok)
		require.Equal(t, fresh, v, "round %d: PutIfAbsent raced past a concurrent Put", round)
	}
}

func TestStateCache_AppliedEndLifecycle(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)
	require.Zero(t, sc.appliedEnd[kv.AccountsDomain])

	sc.apply(kv.AccountsDomain, makeAddr(1), makeValue(1), 20)
	sc.apply(kv.AccountsDomain, makeAddr(2), makeValue(2), 10)
	require.Equal(t, uint64(21), sc.appliedEnd[kv.AccountsDomain])
	require.Zero(t, sc.appliedEnd[kv.StorageDomain])

	sc.unwind(15)
	require.Equal(t, uint64(15), sc.appliedEnd[kv.AccountsDomain])

	sc.clear()
	require.Equal(t, uint64(15), sc.appliedEnd[kv.AccountsDomain],
		"clear drops entries, not admission history")
}

func TestStateCache_StaleViewCannotFillAfterDelete(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	stale := makeValue(1)
	sc.apply(kv.AccountsDomain, key, stale, 10)
	sc.apply(kv.AccountsDomain, key, nil, 20)
	_, ok := sc.get(kv.AccountsDomain, key)
	require.False(t, ok, "an authoritative deletion must physically remove the entry")

	sc.View(frontierAt(11)).Fill(kv.AccountsDomain, key, stale, 10)
	_, ok = sc.get(kv.AccountsDomain, key)
	require.False(t, ok, "a view older than the deletion must not fill afterward")
}

// SharedDomains commits the tx and only then walks `pending` into the cache, so
// between those steps a reader opening a new tx legitimately sees txNums the
// cache has not applied yet: its frontier is ahead of appliedEnd. Rejecting
// "ahead" would drop fills on every flush for the length of the apply loop.
func TestStateCache_ReaderAheadOfApplyWindowCanFill(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	sc.apply(kv.AccountsDomain, makeAddr(1), makeValue(1), 100)

	key := makeAddr(2)
	sc.View(frontierAt(201)).Fill(kv.AccountsDomain, key, makeValue(2), 200)

	_, ok := sc.get(kv.AccountsDomain, key)
	require.True(t, ok,
		"a reader ahead of appliedEnd is the normal commit-then-apply window, not a dead fork")
}

func TestStateCacheReconcileFilesClearsAndFencesOlderViews(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "true")
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	older := sc.View(frontierAt(100))
	older.Fill(kv.AccountsDomain, key, makeValue(1), 90)
	_, ok := sc.get(kv.AccountsDomain, key)
	require.True(t, ok)

	sc.Applier().ReconcileFiles(frontierAt(150))
	_, ok = sc.get(kv.AccountsDomain, key)
	require.False(t, ok)

	older.Fill(kv.AccountsDomain, key, makeValue(1), 90)
	_, ok = sc.get(kv.AccountsDomain, key)
	require.False(t, ok)

	sc.View(frontierAt(150)).Fill(kv.AccountsDomain, key, makeValue(2), 140)
	value, ok := sc.get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, makeValue(2), value)
}

func TestStateCacheReconcileFilesPreservesAppliedState(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	sc.apply(kv.AccountsDomain, key, makeValue(1), 50)
	sc.Applier().AdvanceCommit(149)
	sc.Applier().ReconcileFiles(frontierAt(150))

	value, ok := sc.get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, makeValue(1), value)
}

func TestStateCacheInitializePreservesFileFrontier(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "true")
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	sc.Applier().ReconcileFiles(frontierAt(150))
	sc.Applier().Initialize(1)
	key := makeAddr(1)
	sc.View(frontierAtVersion(100, 1)).Fill(kv.AccountsDomain, key, makeValue(1), 90)

	_, ok := sc.get(kv.AccountsDomain, key)
	require.False(t, ok)
}

func TestStateCache_PreReorgViewCannotFillAfterUnwind(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	canonical, fork := makeValue(1), makeValue(2)
	sc.apply(kv.AccountsDomain, key, canonical, 40)
	sc.apply(kv.AccountsDomain, key, fork, 100)

	preReorg := sc.View(frontierAt(101))
	preReorg.Fill(kv.AccountsDomain, key, fork, 100)
	sc.unwind(50)
	_, ok := sc.get(kv.AccountsDomain, key)
	require.False(t, ok, "the unwind must evict the fork's value")

	preReorg.Fill(kv.AccountsDomain, key, fork, 100)
	_, ok = sc.get(kv.AccountsDomain, key)
	require.False(t, ok, "a pre-reorg view must not reinstate the discarded fork's value")
}

func TestStateCache_InitializeDoesNotMoveStateVersionBackward(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	sc.Applier().Initialize(3)
	sc.Applier().Initialize(2)

	staleKey := makeAddr(1)
	sc.View(frontierAtVersion(11, 2)).Fill(kv.AccountsDomain, staleKey, makeValue(1), 10)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, staleKey)
	require.False(t, ok, "an older initializer must not reactivate stale fills")

	currentKey := makeAddr(2)
	sc.View(frontierAtVersion(11, 3)).Fill(kv.AccountsDomain, currentKey, makeValue(2), 10)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, currentKey)
	require.True(t, ok, "the accepted state version must remain active")
}

func TestStateCache_InitializeClearsUnversionedEntries(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	view := sc.View(frontierAt(11))
	view.Fill(kv.AccountsDomain, key, makeValue(1), 10)
	sc.Applier().Initialize(1)

	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "initialization cannot vouch for entries admitted without a state version")
	view.Fill(kv.AccountsDomain, key, makeValue(1), 10)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "initialization must revoke views bound before the state version was known")
}

func TestStateCache_PublishRejectsOlderStateVersion(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	newer := makeValue(3)
	sc.Applier().Initialize(1)
	sc.Applier().Publish(1, 3, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: newer, TxNum: 30}})
	sc.Applier().Publish(1, 2, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: makeValue(2), TxNum: 20}})

	got, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, newer, got, "a delayed older publication must not overwrite newer state")

	staleKey := makeAddr(2)
	sc.View(frontierAtVersion(31, 2)).Fill(kv.AccountsDomain, staleKey, makeValue(2), 30)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, staleKey)
	require.False(t, ok, "a rejected publication must not move fill admission backward")
}

func TestStateCache_PublicationDoesNotBlockViewBinding(t *testing.T) {
	cache := &blockingPutCache{
		started: make(chan struct{}),
		release: make(chan struct{}),
		filled:  make(chan struct{}, 1),
	}
	sc := &StateCache{}
	sc.caches[kv.AccountsDomain] = cache
	sc.Applier().Initialize(1)
	existingView := sc.View(frontierAtVersion(21, 1))

	published := make(chan struct{})
	go func() {
		sc.Applier().Publish(1, 2, []StateUpdate{{
			Domain: kv.AccountsDomain,
			Key:    makeAddr(1),
			Value:  makeValue(1),
			TxNum:  20,
		}})
		close(published)
	}()
	<-cache.started
	publicationDone := false
	defer func() {
		if !publicationDone {
			close(cache.release)
			<-published
		}
	}()

	existingView.Fill(kv.AccountsDomain, makeAddr(2), makeValue(2), 10)
	select {
	case <-cache.filled:
		t.Fatal("cache fill was admitted during publication")
	default:
	}

	viewBound := make(chan ReadView, 1)
	go func() {
		viewBound <- sc.View(frontierAtVersion(21, 2))
	}()
	var duringPublication ReadView
	select {
	case view := <-viewBound:
		duringPublication = view
		require.False(t, view.CanFill(), "a view bound during publication must not fill partial state")
		require.True(t, view.NeedsFrontier(), "publication is temporary, so the view may retry binding afterward")
	case <-time.After(time.Second):
		t.Fatal("cache publication blocked view binding")
	}

	close(cache.release)
	<-published
	publicationDone = true
	require.False(t, duringPublication.CanFill(), "an inert view must be rebound explicitly")
	duringPublication = duringPublication.WithFrontier(frontierAtVersion(21, 2))
	require.True(t, duringPublication.CanFill(), "an explicitly rebound view may fill after publication")
	require.True(t, sc.View(frontierAtVersion(21, 2)).CanFill(), "the committed version must admit new views")
	existingView.Fill(kv.AccountsDomain, makeAddr(2), makeValue(2), 10)
	select {
	case <-cache.filled:
	case <-time.After(time.Second):
		t.Fatal("continuous publication did not restore fill admission")
	}
}

func TestStateCache_OnlyRetryPotentiallyEligibleFrontier(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)
	sc.Applier().Initialize(2)

	require.True(t, sc.View(nil).NeedsFrontier(), "an unbound view may acquire a frontier later")
	stale := sc.View(frontierAtVersion(21, 1))
	require.False(t, stale.CanFill(), "a stale transaction must remain fill-inert")
	require.False(t, stale.NeedsFrontier(),
		"a stale transaction cannot become current as state versions advance")
	require.False(t, sc.View(frontierAtVersion(21, 2)).NeedsFrontier(),
		"an accepted frontier needs no retry")
	require.True(t, sc.View(frontierAtVersion(21, 3)).NeedsFrontier(),
		"a transaction ahead of the cache may become eligible when publication catches up")
}

func TestStateCache_PublishClearsOnSkippedStateVersion(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	sc.Applier().Initialize(1)
	sc.apply(kv.AccountsDomain, key, makeValue(1), 10)
	staleView := sc.View(frontierAtVersion(11, 1))
	sc.Applier().Publish(2, 3, nil)

	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a skipped publication may omit the update that made an old entry stale")
	staleView.Fill(kv.AccountsDomain, key, makeValue(1), 10)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a skipped publication must revoke previously bound views")
}

func TestStateCache_PublishKeepsEntriesWhenOneCommitAdvancesVersionMoreThanOnce(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	sc.Applier().Initialize(1)
	sc.apply(kv.AccountsDomain, key, makeValue(1), 10)
	sc.Applier().Publish(1, 3, nil)

	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a complete publication must preserve unchanged entries")
}

func TestStateCache_BoundViewCanFillAcrossContinuousPublication(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	sc.Applier().Initialize(1)
	view := sc.View(frontierAtVersion(11, 1))
	sc.Applier().Publish(1, 2, nil)
	view.Fill(kv.AccountsDomain, key, makeValue(1), 10)

	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a continuous forward publication must not revoke an already-eligible view")
}

func TestStateCache_PublishUnwindSerializesWithFill(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)
	sc.Applier().Initialize(0)

	for committedStateVersion := uint64(1); committedStateVersion <= 100; committedStateVersion++ {
		key := makeAddr(int(committedStateVersion))
		sc.Applier().Unwind(10)
		view := sc.View(frontierAtVersion(11, committedStateVersion-1))

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			view.Fill(kv.AccountsDomain, key, makeValue(1), 10)
		}()
		go func() {
			defer wg.Done()
			<-start
			sc.Applier().PublishUnwind(committedStateVersion-1, committedStateVersion, 10, nil)
		}()
		close(start)
		wg.Wait()

		_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
		require.False(t, ok, "a fill from the pre-commit state must not survive unwind publication")
	}
}

func TestStateCache_RejectedPublishUnwindStillInvalidates(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	deadForkValue := makeValue(1)
	applier := sc.Applier()
	applier.Initialize(10)
	applier.Publish(10, 11, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: deadForkValue, TxNum: 100}})
	applier.Unwind(50)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok)

	inWindow := sc.View(frontierAtVersion(101, 11))
	inWindow.Fill(kv.AccountsDomain, key, deadForkValue, 100)
	got, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, deadForkValue, got)
	applier.Publish(11, 20, nil)
	applier.PublishUnwind(11, 12, 50, nil)

	_, ok = sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "the durable unwind must invalidate fills even when its publication is older")
	require.True(t, sc.View(frontierAtVersion(51, 20)).CanFill(), "the rejected publication must not move the cache generation backwards")
}

func TestStateCache_FileEndViewCannotFillAtAppliedTx(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	stale := makeValue(1)
	sc.apply(kv.AccountsDomain, key, nil, 100)

	sc.View(frontierAt(100)).Fill(kv.AccountsDomain, key, stale, 99)
	_, ok := sc.get(kv.AccountsDomain, key)
	require.False(t, ok, "a [0,100) view does not contain the applied tx 100")

	fresh := makeValue(2)
	sc.View(frontierAt(101)).Fill(kv.AccountsDomain, key, fresh, 100)
	got, ok := sc.get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, fresh, got)
}

func TestStateCache_ApplyDeleteAtomicWithFill(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	progressKey := makeAddr(1)
	key := makeAddr(2)
	value := makeValue(1)
	for round := range 20000 {
		appliedTxNum := uint64(round*2 + 1)
		visibleEnd := appliedTxNum + 1
		sc.apply(kv.AccountsDomain, progressKey, value, appliedTxNum)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sc.apply(kv.AccountsDomain, key, nil, visibleEnd)
		}()
		go func() {
			defer wg.Done()
			sc.View(frontierAt(visibleEnd)).Fill(kv.AccountsDomain, key, value, appliedTxNum)
		}()
		wg.Wait()

		_, ok := sc.get(kv.AccountsDomain, key)
		require.False(t, ok, "round %d: stale fill survived the authoritative delete", round)
	}
}

func TestStateCache_ApplyCodeDeleteDropsAddrCodeHash(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	addr := makeAddr(1)
	var h [32]byte
	h[0] = 0xaa
	sc.View(frontierAt(0)).SeedAddrCodeHash(addr, h, 10)
	_, ok := sc.getAddrCodeHash(addr)
	require.True(t, ok)

	sc.apply(kv.CodeDomain, addr, nil, 20)
	_, ok = sc.getAddrCodeHash(addr)
	require.False(t, ok, "a code deletion must drop the derived addr→codeHash mapping")
}

func TestStateCache_AccountDeleteDropsCodeBinding(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	addr := makeAddr(1)
	code := makeCode(1)

	sc.apply(kv.CodeDomain, addr, code, 10)
	_, ok := sc.get(kv.CodeDomain, addr)
	require.True(t, ok)

	sc.apply(kv.AccountsDomain, addr, nil, 20)
	_, ok = sc.get(kv.CodeDomain, addr)
	require.False(t, ok, "an account deletion must drop the addr→code binding")
}

// A Delete racing an update-in-place put must not double-subtract the
// displaced entry's size: freelru's OnEvict subtracts it for the Remove, and
// put's update delta subtracts it again unless the two writers share the
// key's stripe.
func TestDomainCache_DeleteAtomicWithPut_NoSizeDrift(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	addr := makeAddr(1)
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	for round := range 20000 {
		c.Put(addr, v1, 10)
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(addr, v2, 20) })
		wg.Go(func() { c.Delete(addr) })
		wg.Wait()
		c.Delete(addr)
		require.Zero(t, c.SizeBytes(), "round %d: size accounting drifted", round)
	}
}

// The lazy stale-drop inside GetWithTxNum removes entries; an unstriped
// Remove racing put's read-modify-write double-subtracts the displaced
// entry's size. Exactly one live entry remains after every round, so drift
// shows as a size mismatch.
func TestDomainCache_StaleDropAtomicWithPut_NoSizeDrift(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	addr := makeAddr(1)
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	wantSize := int64(len(addr) + len(v1) + 24)
	for round := range 20000 {
		c.Put(addr, v1, 10)
		c.Unwind(5)
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(addr, v2, 20) })
		wg.Go(func() { c.Get(addr) })
		wg.Wait()
		require.Equal(t, wantSize, c.SizeBytes(), "round %d: size accounting drifted", round)
	}
}

// A Clear racing a put must not leave phantom bytes: unless Clear excludes
// writers via the put stripes, a put that loaded the retiring generation
// lands its entry where no reader sees it and adds the entry's size after
// Clear zeroed the counter — inflating SizeBytes for an invisible entry.
func TestDomainCache_ClearAtomicWithPut_NoSizeDrift(t *testing.T) {
	c := closeOnCleanup(t, NewDomainCacheMode(1*datasize.MB, ModeEvictLRU))
	addr := makeAddr(1)
	v1 := []byte("value-one")
	entrySize := int64(len(addr) + len(v1) + 24)
	for round := range 20000 {
		var wg sync.WaitGroup
		wg.Go(func() { c.Put(addr, v1, 10) })
		wg.Go(func() { c.Clear() })
		wg.Wait()
		wantSize := int64(0)
		if _, ok := c.Get(addr); ok {
			wantSize = entrySize
		}
		require.Equal(t, wantSize, c.SizeBytes(), "round %d: size accounting drifted", round)
	}
}

// STATE_CACHE_FILLS=false turns off the admission-gated read fills (apply-only
// mode): the A/B lever for measuring what fills contribute, and the ops kill
// switch. Canonical publication keeps working.
func TestStateCacheFillsSwitchDisablesReadFills(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	defer c.Close()

	key := make([]byte, 20)
	key[0] = 0xaa
	view := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))

	view.Fill(kv.AccountsDomain, key, []byte("value"), 10)
	_, ok := c.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "fills must be disabled")

	view.SeedAddrCodeHash(key, [32]byte{1}, 10)
	_, ok = c.View(nil).GetAddrCodeHash(key)
	require.False(t, ok, "mapping seeds must be disabled")

	codeHash := crypto.Keccak256([]byte{0xaa, 1, 2, 3})
	view.FillCodeSize(codeHash, 4, 10)
	_, ok = c.View(nil).GetCodeSizeByHash(codeHash)
	require.False(t, ok, "content-addressed fills must be disabled too: the switch means no reader writes at all")

	c.Applier().Publish(0, 1, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: []byte("applied"), TxNum: 20}})
	got, ok := c.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "canonical publication must keep working")
	require.Equal(t, []byte("applied"), got)
}

// Clearing entries does not rewind canonical state, so the admission frontier
// must survive Clear: a still-live older ReadView must not refill pre-apply
// data into the emptied cache.
func TestStateCache_StaleViewCannotFillAfterClear(t *testing.T) {
	b := 1 * datasize.MB
	sc := NewStateCache(b, b, b, b)
	t.Cleanup(sc.Close)

	key := makeAddr(1)
	applier := sc.Applier()
	applier.Initialize(1)
	oldView := sc.View(FrontierWithStateVersion(
		FrontierFunc(func(kv.Domain) (uint64, bool) { return 11, true }),
		1,
	))

	applier.Publish(1, 2, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, TxNum: 20}})
	applier.Clear()

	oldView.Fill(kv.AccountsDomain, key, []byte("pre-delete"), 10)
	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a pre-apply view must not resurrect the deleted value through Clear")

	freshView := sc.View(FrontierWithStateVersion(
		FrontierFunc(func(kv.Domain) (uint64, bool) { return 21, true }),
		2,
	))
	freshView.Fill(kv.AccountsDomain, key, []byte("current"), 20)
	got, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a view at the applied frontier must still fill after Clear")
	require.Equal(t, []byte("current"), got)
}

// An addr-keyed code entry derives from the account: an account deletion drops
// it without advancing the code frontier, so code-fill admission must check the
// accounts frontier too — otherwise a pre-deletion view refills the dead code.
func TestStateCache_AccountDeletionGatesStaleCodeFill(t *testing.T) {
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	addr, code := makeAddr(1), makeCode(1)
	other, otherCode := makeAddr(2), makeCode(2)

	applier := c.Applier()
	applier.Initialize(1)
	stale := c.View(FrontierWithStateVersion(
		FrontierFunc(func(kv.Domain) (uint64, bool) { return 101, true }),
		1,
	))
	applier.Publish(1, 2, []StateUpdate{
		{Domain: kv.CodeDomain, Key: addr, Value: code, TxNum: 100},
		{Domain: kv.AccountsDomain, Key: addr, TxNum: 200},
	})

	stale.Fill(kv.CodeDomain, addr, code, 100)
	_, ok := c.View(nil).Get(kv.CodeDomain, addr)
	require.False(t, ok, "code of a deleted account must not be refillable from a pre-deletion view")

	fresh := c.View(FrontierWithStateVersion(
		FrontierFunc(func(d kv.Domain) (uint64, bool) {
			if d == kv.AccountsDomain {
				return 201, true
			}
			return 101, true
		}),
		2,
	))
	fresh.Fill(kv.CodeDomain, other, otherCode, 100)
	_, ok = c.View(nil).Get(kv.CodeDomain, other)
	require.True(t, ok, "unrelated code fills from a current view must stay admitted")
}

func TestStateCache_CodeHashHitBindsAddress(t *testing.T) {
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	firstAddr, secondAddr, code := makeAddr(1), makeAddr(2), makeCode(1)
	codeHash := crypto.Keccak256Hash(code)
	view := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))
	view.Fill(kv.CodeDomain, firstAddr, code, 10)
	view.SeedAddrCodeHash(secondAddr, codeHash, 11)
	_, ok := c.View(nil).Get(kv.CodeDomain, secondAddr)
	require.False(t, ok, "the second address must start without an addr-keyed code binding")
	got, ok := view.GetCodeByAddressHash(secondAddr)
	require.True(t, ok)
	require.Equal(t, code, got)
	got, ok = c.View(nil).Get(kv.CodeDomain, secondAddr)
	require.True(t, ok, "the hash hit must populate the addr-keyed code binding")
	require.Equal(t, code, got)
	c.Applier().Unwind(11)
	_, ok = c.View(nil).Get(kv.CodeDomain, secondAddr)
	require.False(t, ok, "the derived binding must keep the mapping stamp for unwind invalidation")
}

func TestStateCache_FillCodeUsesKnownHash(t *testing.T) {
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	addr := makeAddr(1)
	code := makeCode(1)
	codeHash := makeHash(1)
	view := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))

	view.FillCode(addr, code, codeHash[:], 10)
	code[0]++

	got, ok := view.GetCodeByHash(codeHash[:])
	require.True(t, ok)
	require.Equal(t, makeCode(1), got)
}

func TestStateCache_EmptyCodeHashUsesViewFrontierStamp(t *testing.T) {
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	addr := makeAddr(1)
	view := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))
	view.SeedAddrCodeHash(addr, [32]byte{}, 4)
	codeHash, txNum, ok := c.getAddrCodeHashWithTxNum(addr)
	require.True(t, ok)
	require.Zero(t, codeHash)
	require.Equal(t, uint64(99), txNum, "a negative mapping reflects the view, not a nonexistent value's reported step")
	c.Applier().Unwind(99)
	_, _, ok = c.getAddrCodeHashWithTxNum(addr)
	require.False(t, ok)
}

// An apply-only cache (STATE_CACHE_FILLS=false) has no fill for a lowered
// frontier to poison; wire-up code keys the aggregator forbid on this.
func TestApplyOnlyCacheReportsFillsDisabled(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")
	b := 1 * datasize.MB
	c := NewStateCache(b, b, b, b)
	t.Cleanup(c.Close)
	require.False(t, c.FillsEnabled())

	t.Setenv("STATE_CACHE_FILLS", "true")
	c2 := NewStateCache(b, b, b, b)
	t.Cleanup(c2.Close)
	require.True(t, c2.FillsEnabled())
}

// BenchmarkStateCachePublicationUnderLoad measures what a commit costs the
// readers running beside it. b.N counts publications; the reported
// reads/s and fill-reject ratio come from reader goroutines that run for the
// whole timed region, so a publication that stalls readers shows up as reads/s
// collapsing rather than as ns/op moving.
//
// version=current models a reader bound to the state the cache just published.
// version=stale repeatedly constructs views for a transaction opened before
// the last commit. Production getters retain this rejection; constructing each
// view here deliberately measures the worst-case binding contention.
func BenchmarkStateCachePublicationUnderLoad(b *testing.B) {
	const keySpace = 4096

	mkKey := func(i int) []byte {
		return []byte{byte(i), byte(i >> 8), 0x5A}
	}

	for _, batch := range []int{1, 1000, 20000} {
		for _, readers := range []int{0, 8, 32} {
			for _, mix := range []string{"current", "stale", "half"} {
				if readers == 0 && mix != "current" {
					continue // reader mix is meaningless with no readers
				}
				b.Run(fmt.Sprintf("batch=%d/readers=%d/version=%s", batch, readers, mix), func(b *testing.B) {
					c := NewStateCache(64<<20, 64<<20, 16<<20, 8<<20)
					defer c.Close()
					ap := c.Applier()

					var version atomic.Uint64
					version.Store(1)
					ap.Initialize(1)

					// Seed so readers mostly hit.
					seed := make([]StateUpdate, keySpace)
					for i := range seed {
						seed[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i),
							Value: []byte{byte(i), 0xEE}, TxNum: uint64(i)}
					}
					ap.Publish(1, 2, seed)
					version.Store(2)

					updates := make([]StateUpdate, batch)
					for i := range updates {
						updates[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i % keySpace),
							Value: []byte{byte(i), 0xFF}, TxNum: uint64(i)}
					}

					var reads, fillsOffered, fillsLanded atomic.Uint64
					stop := make(chan struct{})
					var wg sync.WaitGroup

					for r := range readers {
						wg.Add(1)
						go func(r int) {
							defer wg.Done()
							useStale := mix == "stale" || (mix == "half" && r%2 == 0)
							n := uint64(r * 7919)
							for {
								select {
								case <-stop:
									return
								default:
								}
								for range 64 {
									n = n*1103515245 + 12345
									idx := int(n>>16) % keySpace
									key := mkKey(idx)

									sv := version.Load()
									if useStale {
										sv = 1 // the version the cache has moved past
									}
									v := c.View(FrontierWithStateVersion(
										FrontierFunc(func(kv.Domain) (uint64, bool) { return uint64(keySpace), true }), sv))

									if _, ok := v.Get(kv.AccountsDomain, key); !ok {
										fillsOffered.Add(1)
										v.Fill(kv.AccountsDomain, key, []byte{byte(idx), 0xEE}, uint64(idx))
										if _, ok := c.View(nil).Get(kv.AccountsDomain, key); ok {
											fillsLanded.Add(1)
										}
									}
									reads.Add(1)
								}
							}
						}(r)
					}

					b.ResetTimer()
					start := time.Now()
					for i := 0; b.Loop(); i++ {
						src := version.Load()
						ap.Publish(src, src+1, updates)
						version.Store(src + 1)
					}
					elapsed := time.Since(start)
					b.StopTimer()

					close(stop)
					wg.Wait()

					if readers > 0 {
						b.ReportMetric(float64(reads.Load())/elapsed.Seconds()/1e6, "Mreads/s")
						if off := fillsOffered.Load(); off > 0 {
							b.ReportMetric(float64(fillsLanded.Load())/float64(off)*100, "%fills-landed")
						}
					}
					b.ReportMetric(float64(batch), "updates/publish")
				})
			}
		}
	}
}

// BenchmarkPublishVsViewBindLock isolates what admissionMu costs a publication.
// Readers do identical work; only the bind differs. View(nil) returns without
// touching admissionMu, so the delta is the read-lock's contribution to both
// the publisher's cost and reader throughput.
func BenchmarkPublishVsViewBindLock(b *testing.B) {
	const keySpace = 4096
	mkKey := func(i int) []byte { return []byte{byte(i), byte(i >> 8), 0x5A} }

	for _, bind := range []string{"frontier-RLock", "nil-nolock"} {
		b.Run(bind, func(b *testing.B) {
			c := NewStateCache(64<<20, 64<<20, 16<<20, 8<<20)
			defer c.Close()
			ap := c.Applier()
			ap.Initialize(1)

			seed := make([]StateUpdate, keySpace)
			for i := range seed {
				seed[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i), Value: []byte{byte(i), 0xEE}, TxNum: uint64(i)}
			}
			ap.Publish(1, 2, seed)
			var version atomic.Uint64
			version.Store(2)

			updates := make([]StateUpdate, 20000)
			for i := range updates {
				updates[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i % keySpace), Value: []byte{byte(i), 0xFF}, TxNum: uint64(i)}
			}

			var reads atomic.Uint64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for r := range 32 {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					n := uint64(r * 7919)
					for {
						select {
						case <-stop:
							return
						default:
						}
						for range 64 {
							n = n*1103515245 + 12345
							key := mkKey(int(n>>16) % keySpace)
							var v ReadView
							if bind == "frontier-RLock" {
								v = c.View(FrontierWithStateVersion(
									FrontierFunc(func(kv.Domain) (uint64, bool) { return keySpace, true }), version.Load()))
							} else {
								v = c.View(nil)
							}
							v.Get(kv.AccountsDomain, key)
							reads.Add(1)
						}
					}
				}(r)
			}

			b.ResetTimer()
			start := time.Now()
			for b.Loop() {
				src := version.Load()
				ap.Publish(src, src+1, updates)
				version.Store(src + 1)
			}
			el := time.Since(start)
			b.StopTimer()
			close(stop)
			wg.Wait()
			b.ReportMetric(float64(reads.Load())/el.Seconds()/1e6, "Mreads/s")
		})
	}
}
