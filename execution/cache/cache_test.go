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
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// Test helpers
func makeAddr(i int) []byte {
	addr := make([]byte, 20)
	addr[19] = byte(i)
	return addr
}

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

// =============================================================================
// DomainCache Tests
// =============================================================================

func TestDomainCache_NewWithByteCapacity(t *testing.T) {
	c := NewDomainCache(1 * datasize.MB) // 1MB
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.SizeBytes())
	assert.Equal(t, 1*datasize.MB, c.CapacityBytes())
}

func TestDomainCache_GetPut(t *testing.T) {
	c := NewDomainCache(100)

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
	c := NewDomainCache(100)

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
	c := NewDomainCacheMode(100, ModeNoOp)

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
	c := NewDomainCache(100)

	addr := makeAddr(1)
	c.Put(addr, makeValue(1), 0)
	assert.Equal(t, 1, c.Len())

	c.Delete(addr)
	assert.Equal(t, 0, c.Len())

	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestDomainCache_Clear(t *testing.T) {
	c := NewDomainCache(100)

	c.Put(makeAddr(1), makeValue(1), 0)
	c.Put(makeAddr(2), makeValue(2), 0)
	assert.Equal(t, 2, c.Len())

	c.Clear()
	assert.Equal(t, 0, c.Len())
}

func TestDomainCache_PrintStatsAndReset(t *testing.T) {
	c := NewDomainCache(100)

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
	c := NewDomainCache(100)
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
	c := NewDefaultCodeCache()
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 0, c.CodeLen())
}

func TestCodeCache_GetPut(t *testing.T) {
	c := NewCodeCache(100, 200)

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
	c := NewCodeCache(100, 200)

	addr := makeAddr(1)
	c.Put(addr, []byte{}, 0)

	// Should not store empty code
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 0, c.CodeLen())
}

func TestCodeCache_CodeDeduplication(t *testing.T) {
	c := NewCodeCache(100, 200)

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
	// LRU rather than no-op-when-full — fresh-address workloads must
	// warm up (geth's lru.Cache pattern, mirroring core/state/database_code.go).
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

	c := NewCodeCache(1024*1024, 1024*28) // 1MB code, ~1024 addr LRU entries
	for i := 0; i < 1100; i++ {
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

	// hashToCode stores all 1100 distinct codes (content-addressed,
	// independent of addr LRU eviction).
	assert.Equal(t, 1100, c.CodeLen())

	// Updating an existing addr re-writes the entry (LRU promotes to MRU).
	c.Put(wideAddr(1099), wideCode(4242), 0)
	v, ok = c.Get(wideAddr(1099))
	assert.True(t, ok)
	assert.Equal(t, wideCode(4242), v)
}

func TestCodeCache_CodeCapacityLimit(t *testing.T) {
	// Each code entry is 8 (hash) + 3 (code bytes) = 11 bytes
	// Set code capacity to 25 bytes - enough for 2 entries but not 3
	c := NewCodeCache(25, 1024*1024) // 25 bytes code, 1MB addr

	// Fill code capacity
	c.Put(makeAddr(1), makeCode(1), 0)
	c.Put(makeAddr(2), makeCode(2), 0)
	assert.Equal(t, 2, c.CodeLen())

	// Try to add more code - addr mapping added, but code not stored
	c.Put(makeAddr(3), makeCode(3), 0)
	assert.Equal(t, 3, c.Len())     // addr mapping added
	assert.Equal(t, 2, c.CodeLen()) // code not added (at capacity)

	// Get for addr3 should fail (code not in cache)
	_, ok := c.Get(makeAddr(3))
	assert.False(t, ok)
}

func TestCodeCache_Delete(t *testing.T) {
	c := NewCodeCache(100, 200)

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
	c := NewCodeCache(100, 200)

	c.Put(makeAddr(1), makeCode(1), 0)
	c.Put(makeAddr(2), makeCode(2), 0)

	c.Clear()
	assert.Equal(t, 0, c.Len())
	// Code should still exist (immutable)
	assert.Equal(t, 2, c.CodeLen())
}

func TestCodeCache_PrintStatsAndReset(t *testing.T) {
	c := NewCodeCache(100, 200)

	c.Put(makeAddr(1), makeCode(1), 0)
	c.Get(makeAddr(1)) // hit
	c.Get(makeAddr(2)) // miss

	// Should not panic
	c.PrintStatsAndReset()
}

func TestCodeCache_PrintStatsAndReset_NoOps(t *testing.T) {
	c := NewCodeCache(100, 200)
	// No operations - should handle zero total gracefully
	c.PrintStatsAndReset()
}

func TestCodeCache_GetMissingCode(t *testing.T) {
	c := NewCodeCache(1024*1024, 1024*1024) // 1MB each

	// Manually set addr mapping without code (simulates capacity limit scenario)
	addr := makeAddr(1)
	code := makeCode(1)
	c.Put(addr, code, 0)

	// Clear the code cache but keep addr mapping
	c.hashToCode.Clear()
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
	c := NewStateCache(10, 20, 30, 40, 40)
	require.NotNil(t, c)

	// Account, Storage, Code, Commitment should be initialized
	assert.NotNil(t, c.GetCache(kv.AccountsDomain))
	assert.NotNil(t, c.GetCache(kv.StorageDomain))
	assert.NotNil(t, c.GetCache(kv.CodeDomain))

	// Other domains should be nil
	assert.Nil(t, c.GetCache(kv.ReceiptDomain))
	assert.Nil(t, c.GetCache(kv.RCacheDomain))
}

func TestStateCache_NewDefaultStateCache(t *testing.T) {
	c := NewDefaultStateCache()
	require.NotNil(t, c)

	assert.NotNil(t, c.GetCache(kv.AccountsDomain))
	assert.NotNil(t, c.GetCache(kv.StorageDomain))
	assert.NotNil(t, c.GetCache(kv.CodeDomain))
}

func TestStateCache_GetPut_Account(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	addr := makeAddr(1)
	value := makeValue(1)

	// Get non-existent
	v, ok := c.Get(kv.AccountsDomain, addr)
	assert.False(t, ok)
	assert.Nil(t, v)

	// Put and Get
	c.Put(kv.AccountsDomain, addr, value, 0)
	v, ok = c.Get(kv.AccountsDomain, addr)
	assert.True(t, ok)
	assert.Equal(t, value, v)
}

func TestStateCache_GetPut_Storage(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	key := make([]byte, 52) // addr(20) + slot(32)
	copy(key, makeAddr(1))
	key[51] = 1
	value := makeValue(1)

	c.Put(kv.StorageDomain, key, value, 0)
	v, ok := c.Get(kv.StorageDomain, key)
	assert.True(t, ok)
	assert.Equal(t, value, v)
}

func TestStateCache_GetPut_Code(t *testing.T) {
	c := NewStateCache(1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB)

	addr := makeAddr(1)
	code := makeCode(1)

	c.Put(kv.CodeDomain, addr, code, 0)
	v, ok := c.Get(kv.CodeDomain, addr)
	assert.True(t, ok)
	assert.Equal(t, code, v)
}

func TestStateCache_GetPut_UnsupportedDomain(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// ReceiptDomain is not supported
	c.Put(kv.ReceiptDomain, makeAddr(1), makeValue(1), 0)
	v, ok := c.Get(kv.ReceiptDomain, makeAddr(1))
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestStateCache_Delete(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	addr := makeAddr(1)
	c.Put(kv.AccountsDomain, addr, makeValue(1), 0)
	c.Delete(kv.AccountsDomain, addr)

	_, ok := c.Get(kv.AccountsDomain, addr)
	assert.False(t, ok)
}

// Put(key, nil) must be a cache hit, not a miss. SharedDomains.GetLatest
// caches deleted keys via Put(key, nil); if Get treats that as "not found",
// the caller unnecessarily falls through to the DB on every read.
func TestStateCache_PutEmpty_ThenGet_IsCacheHit(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	key := make([]byte, 52) // addr(20) + slot(32)
	key[0] = 0x1d
	key[51] = 0xa2

	c.Put(kv.StorageDomain, key, nil, 0)

	v, ok := c.Get(kv.StorageDomain, key)
	assert.True(t, ok, "Get after Put(nil) must be a cache hit, not a miss")
	assert.Empty(t, v, "cached value for a deleted key must be empty")
}

// Same test for []byte{} (zero-length but non-nil).
func TestStateCache_PutEmptySlice_ThenGet_IsCacheHit(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	key := make([]byte, 52)
	key[0] = 0x1d
	key[51] = 0xa2

	c.Put(kv.StorageDomain, key, []byte{}, 0)

	v, ok := c.Get(kv.StorageDomain, key)
	assert.True(t, ok, "Get after Put([]byte{}) must be a cache hit")
	assert.Empty(t, v)
}

func TestStateCache_Delete_UnsupportedDomain(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// Should not panic
	c.Delete(kv.ReceiptDomain, makeAddr(1))
}

func TestStateCache_Clear(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	c.Put(kv.AccountsDomain, makeAddr(1), makeValue(1), 0)
	c.Put(kv.StorageDomain, makeAddr(2), makeValue(2), 0)
	c.Put(kv.CodeDomain, makeAddr(3), makeCode(3), 0)

	c.Clear()

	_, ok1 := c.Get(kv.AccountsDomain, makeAddr(1))
	_, ok2 := c.Get(kv.StorageDomain, makeAddr(2))
	_, ok3 := c.Get(kv.CodeDomain, makeAddr(3))

	assert.False(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)
}

func TestStateCache_GetCache_OutOfBounds(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// Domain >= DomainLen should return nil
	cache := c.GetCache(kv.DomainLen)
	assert.Nil(t, cache)

	cache = c.GetCache(kv.Domain(100))
	assert.Nil(t, cache)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestDomainCache_ConcurrentAccess(t *testing.T) {
	c := NewDomainCache(10000)

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			c.Put(makeAddr(i), makeValue(i), 0)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			c.Get(makeAddr(i))
		}
		done <- true
	}()

	<-done
	<-done
}

func TestCodeCache_ConcurrentAccess(t *testing.T) {
	c := NewCodeCache(1000, 1000)

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			c.Put(makeAddr(i), makeCode(i), 0)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
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
	c := NewStateCache(1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB)

	addr := makeAddr(1)
	accountData := []byte("account")
	storageData := []byte("storage")
	codeData := []byte{0x60, 0x00, 0x60, 0x00} // valid code

	c.Put(kv.AccountsDomain, addr, accountData, 0)
	c.Put(kv.StorageDomain, addr, storageData, 0)
	c.Put(kv.CodeDomain, addr, codeData, 0)

	v1, ok1 := c.Get(kv.AccountsDomain, addr)
	v2, ok2 := c.Get(kv.StorageDomain, addr)
	v3, ok3 := c.Get(kv.CodeDomain, addr)

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
	c := NewDomainCache(1 * datasize.MB)
	below := makeAddr(1)
	above := makeAddr(2)
	c.Put(below, makeValue(1), 50)  // predates the unwind
	c.Put(above, makeValue(2), 150) // written in the unwound range

	c.Unwind(100) // keep <=100, drop >100

	v, ok := c.Get(below)
	assert.True(t, ok, "entry below the unwind point must stay warm")
	assert.Equal(t, makeValue(1), v)

	_, ok = c.Get(above)
	assert.False(t, ok, "entry above the unwind point must be invalidated")
	assert.Equal(t, 1, c.Len(), "the stale entry is evicted lazily on its read")
}

// The reused-txNum case: after an unwind, the live fork re-writes a key at the
// SAME txNum as the dead fork's write. The epoch — not the txNum — distinguishes
// them, so the dead entry reads stale and the re-written one reads valid.
func TestUnwind_ReusedTxNumDisambiguatedByEpoch(t *testing.T) {
	c := NewDomainCache(1 * datasize.MB)
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
	c := NewDomainCache(1 * datasize.MB)
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
	c := NewDomainCache(1 * datasize.MB)
	k := makeAddr(1)
	c.Put(k, makeValue(1), 70) // epoch 0

	c.Unwind(50)  // floor 50, epoch 1 — k(70>50, epoch0) now stale
	c.Unwind(100) // shallower; floor must stay 50, not rise to 100

	_, ok := c.Get(k)
	assert.False(t, ok, "deeper unwind's floor must not be raised by a later shallower one")
}
