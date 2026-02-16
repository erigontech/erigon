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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestDomainCache_NewDomainCache(t *testing.T) {
	c := NewDomainCache(100)
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, common.Hash{}, c.GetBlockHash())
}

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
	c.Put(addr, value)
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

	c.Put(addr, value1)

	// Update with different value
	c.Put(addr, value2)
	v, ok := c.Get(addr)

	assert.True(t, ok)
	assert.Equal(t, value2, v)
}

func TestDomainCache_PutCapacityLimit(t *testing.T) {
	// Each entry is 8 (overhead) + 3 (value) = 11 bytes
	// Set capacity to 22 bytes - enough for 2 entries but not 3
	c := NewDomainCache(22)

	// Fill to capacity
	c.Put(makeAddr(1), makeValue(1))
	c.Put(makeAddr(2), makeValue(2))
	assert.Equal(t, 2, c.Len())

	// Try to add more - should be ignored (no-op when full)
	c.Put(makeAddr(3), makeValue(3))
	assert.Equal(t, 2, c.Len())

	// New key should not exist
	_, ok := c.Get(makeAddr(3))
	assert.False(t, ok)

	// But updating existing key should still work
	newValue := []byte{100, 101, 102}
	c.Put(makeAddr(1), newValue)
	v, ok := c.Get(makeAddr(1))
	assert.True(t, ok)
	assert.Equal(t, newValue, v)
}

func TestDomainCache_Delete(t *testing.T) {
	c := NewDomainCache(100)

	addr := makeAddr(1)
	c.Put(addr, makeValue(1))
	assert.Equal(t, 1, c.Len())

	c.Delete(addr)
	assert.Equal(t, 0, c.Len())

	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestDomainCache_Clear(t *testing.T) {
	c := NewDomainCache(100)

	c.Put(makeAddr(1), makeValue(1))
	c.Put(makeAddr(2), makeValue(2))
	assert.Equal(t, 2, c.Len())

	c.Clear()
	assert.Equal(t, 0, c.Len())
}

func TestDomainCache_BlockHash(t *testing.T) {
	c := NewDomainCache(100)

	assert.Equal(t, common.Hash{}, c.GetBlockHash())

	hash := makeHash(1)
	c.SetBlockHash(hash)
	assert.Equal(t, hash, c.GetBlockHash())
}

func TestDomainCache_ValidateAndPrepare_EmptyCache(t *testing.T) {
	c := NewDomainCache(100)

	parentHash := makeHash(1)
	incomingHash := makeHash(2)

	// Empty cache should always be valid
	valid := c.ValidateAndPrepare(parentHash, incomingHash)
	assert.True(t, valid)
	assert.Equal(t, incomingHash, c.GetBlockHash())
}

func TestDomainCache_ValidateAndPrepare_MatchingParent(t *testing.T) {
	c := NewDomainCache(100)

	block1 := makeHash(1)
	block2 := makeHash(2)

	c.SetBlockHash(block1)
	c.Put(makeAddr(1), makeValue(1))

	// Parent matches current block hash
	valid := c.ValidateAndPrepare(block1, block2)
	assert.True(t, valid)
	assert.Equal(t, block2, c.GetBlockHash())
	// Data should be preserved
	assert.Equal(t, 1, c.Len())
}

func TestDomainCache_ValidateAndPrepare_Mismatch(t *testing.T) {
	c := NewDomainCache(100)

	block1 := makeHash(1)
	block2 := makeHash(2)
	block3 := makeHash(3)

	c.SetBlockHash(block1)
	c.Put(makeAddr(1), makeValue(1))

	// Parent doesn't match - should clear
	valid := c.ValidateAndPrepare(block2, block3)
	assert.False(t, valid)
	assert.Equal(t, block3, c.GetBlockHash())
	// Data should be cleared
	assert.Equal(t, 0, c.Len())
}

func TestDomainCache_ClearWithHash(t *testing.T) {
	c := NewDomainCache(100)

	c.Put(makeAddr(1), makeValue(1))
	c.SetBlockHash(makeHash(1))

	newHash := makeHash(2)
	c.ClearWithHash(newHash)

	assert.Equal(t, 0, c.Len())
	assert.Equal(t, newHash, c.GetBlockHash())
}

func TestDomainCache_PrintStatsAndReset(t *testing.T) {
	c := NewDomainCache(100)

	// Generate some hits and misses
	c.Put(makeAddr(1), makeValue(1))
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

func TestCodeCache_NewCodeCache(t *testing.T) {
	c := NewCodeCache(100, 200)
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 0, c.CodeLen())
	assert.Equal(t, common.Hash{}, c.GetBlockHash())
}

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
	c.Put(addr, code)
	v, ok = c.Get(addr)
	assert.True(t, ok)
	assert.Equal(t, code, v)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, 1, c.CodeLen())
}

func TestCodeCache_PutEmptyCode(t *testing.T) {
	c := NewCodeCache(100, 200)

	addr := makeAddr(1)
	c.Put(addr, []byte{})

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
	c.Put(addr1, code)
	c.Put(addr2, code)
	c.Put(addr3, code)

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
	// Each addr entry is 20 (addr) + 8 (uint64 hash) = 28 bytes
	// Set addr capacity to 60 bytes - enough for 2 entries but not 3
	c := NewCodeCache(1024*1024, 60) // 1MB code, 60 bytes addr

	// Fill to addr capacity
	c.Put(makeAddr(1), makeCode(1))
	c.Put(makeAddr(2), makeCode(2))
	assert.Equal(t, 2, c.Len())

	// Adding more should be no-op for addr (at capacity)
	c.Put(makeAddr(3), makeCode(3))
	assert.Equal(t, 2, c.Len()) // addr not added

	// Code cache should have all 3 codes (code capacity not reached)
	assert.Equal(t, 3, c.CodeLen())

	// Can't get addr3 since it wasn't added
	_, ok := c.Get(makeAddr(3))
	assert.False(t, ok)

	// But updating existing addr should work
	c.Put(makeAddr(1), makeCode(4))
	v, ok := c.Get(makeAddr(1))
	assert.True(t, ok)
	assert.Equal(t, makeCode(4), v)
}

func TestCodeCache_CodeCapacityLimit(t *testing.T) {
	// Each code entry is 8 (hash) + 3 (code bytes) = 11 bytes
	// Set code capacity to 25 bytes - enough for 2 entries but not 3
	c := NewCodeCache(25, 1024*1024) // 25 bytes code, 1MB addr

	// Fill code capacity
	c.Put(makeAddr(1), makeCode(1))
	c.Put(makeAddr(2), makeCode(2))
	assert.Equal(t, 2, c.CodeLen())

	// Try to add more code - addr mapping added, but code not stored
	c.Put(makeAddr(3), makeCode(3))
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
	c.Put(addr, code)

	c.Delete(addr)
	assert.Equal(t, 0, c.Len())
	// Code should still exist (immutable)
	assert.Equal(t, 1, c.CodeLen())

	_, ok := c.Get(addr)
	assert.False(t, ok)
}

func TestCodeCache_Clear(t *testing.T) {
	c := NewCodeCache(100, 200)

	c.Put(makeAddr(1), makeCode(1))
	c.Put(makeAddr(2), makeCode(2))

	c.Clear()
	assert.Equal(t, 0, c.Len())
	// Code should still exist (immutable)
	assert.Equal(t, 2, c.CodeLen())
}

func TestCodeCache_BlockHash(t *testing.T) {
	c := NewCodeCache(100, 200)

	assert.Equal(t, common.Hash{}, c.GetBlockHash())

	hash := makeHash(1)
	c.SetBlockHash(hash)
	assert.Equal(t, hash, c.GetBlockHash())
}

func TestCodeCache_ValidateAndPrepare_EmptyCache(t *testing.T) {
	c := NewCodeCache(100, 200)

	parentHash := makeHash(1)
	incomingHash := makeHash(2)

	valid := c.ValidateAndPrepare(parentHash, incomingHash)
	assert.True(t, valid)
	assert.Equal(t, incomingHash, c.GetBlockHash())
}

func TestCodeCache_ValidateAndPrepare_MatchingParent(t *testing.T) {
	c := NewCodeCache(100, 200)

	block1 := makeHash(1)
	block2 := makeHash(2)

	c.SetBlockHash(block1)
	c.Put(makeAddr(1), makeCode(1))

	valid := c.ValidateAndPrepare(block1, block2)
	assert.True(t, valid)
	assert.Equal(t, block2, c.GetBlockHash())
	// Addr mapping should be preserved
	assert.Equal(t, 1, c.Len())
}

func TestCodeCache_ValidateAndPrepare_Mismatch(t *testing.T) {
	c := NewCodeCache(100, 200)

	block1 := makeHash(1)
	block2 := makeHash(2)
	block3 := makeHash(3)

	c.SetBlockHash(block1)
	c.Put(makeAddr(1), makeCode(1))

	valid := c.ValidateAndPrepare(block2, block3)
	assert.False(t, valid)
	assert.Equal(t, block3, c.GetBlockHash())
	// Addr mapping should be cleared
	assert.Equal(t, 0, c.Len())
	// Code should still exist (immutable)
	assert.Equal(t, 1, c.CodeLen())
}

func TestCodeCache_ClearWithHash(t *testing.T) {
	c := NewCodeCache(100, 200)

	c.Put(makeAddr(1), makeCode(1))
	c.SetBlockHash(makeHash(1))

	newHash := makeHash(2)
	c.ClearWithHash(newHash)

	assert.Equal(t, 0, c.Len())
	assert.Equal(t, newHash, c.GetBlockHash())
	// Code should still exist
	assert.Equal(t, 1, c.CodeLen())
}

func TestCodeCache_PrintStatsAndReset(t *testing.T) {
	c := NewCodeCache(100, 200)

	c.Put(makeAddr(1), makeCode(1))
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
	c.Put(addr, code)

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
	assert.NotNil(t, c.GetCache(kv.CommitmentDomain))

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
	c.Put(kv.AccountsDomain, addr, value)
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

	c.Put(kv.StorageDomain, key, value)
	v, ok := c.Get(kv.StorageDomain, key)
	assert.True(t, ok)
	assert.Equal(t, value, v)
}

func TestStateCache_GetPut_Code(t *testing.T) {
	c := NewStateCache(1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB, 1*datasize.MB)

	addr := makeAddr(1)
	code := makeCode(1)

	c.Put(kv.CodeDomain, addr, code)
	v, ok := c.Get(kv.CodeDomain, addr)
	assert.True(t, ok)
	assert.Equal(t, code, v)
}

func TestStateCache_GetPut_UnsupportedDomain(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// ReceiptDomain is not supported
	c.Put(kv.ReceiptDomain, makeAddr(1), makeValue(1))
	v, ok := c.Get(kv.ReceiptDomain, makeAddr(1))
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestStateCache_Delete(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	addr := makeAddr(1)
	c.Put(kv.AccountsDomain, addr, makeValue(1))
	c.Delete(kv.AccountsDomain, addr)

	_, ok := c.Get(kv.AccountsDomain, addr)
	assert.False(t, ok)
}

func TestStateCache_Delete_UnsupportedDomain(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// Should not panic
	c.Delete(kv.ReceiptDomain, makeAddr(1))
}

func TestStateCache_Clear(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	c.Put(kv.AccountsDomain, makeAddr(1), makeValue(1))
	c.Put(kv.StorageDomain, makeAddr(2), makeValue(2))
	c.Put(kv.CodeDomain, makeAddr(3), makeCode(3))

	c.Clear()

	_, ok1 := c.Get(kv.AccountsDomain, makeAddr(1))
	_, ok2 := c.Get(kv.StorageDomain, makeAddr(2))
	_, ok3 := c.Get(kv.CodeDomain, makeAddr(3))

	assert.False(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)
}

func TestStateCache_ValidateAndPrepare_AllValid(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	block1 := makeHash(1)
	block2 := makeHash(2)

	// Set same block hash on all caches
	c.GetCache(kv.AccountsDomain).SetBlockHash(block1)
	c.GetCache(kv.StorageDomain).SetBlockHash(block1)
	c.GetCache(kv.CodeDomain).SetBlockHash(block1)

	valid := c.ValidateAndPrepare(block1, block2)
	assert.True(t, valid)
}

func TestStateCache_ValidateAndPrepare_SomeMismatch(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	block1 := makeHash(1)
	block2 := makeHash(2)
	block3 := makeHash(3)

	// Set different block hashes
	c.GetCache(kv.AccountsDomain).SetBlockHash(block1)
	c.GetCache(kv.StorageDomain).SetBlockHash(block2) // mismatch
	c.GetCache(kv.CodeDomain).SetBlockHash(block1)

	valid := c.ValidateAndPrepare(block1, block3)
	assert.False(t, valid)
}

func TestStateCache_ClearWithHash(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	c.Put(kv.AccountsDomain, makeAddr(1), makeValue(1))
	c.Put(kv.StorageDomain, makeAddr(2), makeValue(2))

	newHash := makeHash(5)
	c.ClearWithHash(newHash)

	assert.Equal(t, newHash, c.GetCache(kv.AccountsDomain).GetBlockHash())
	assert.Equal(t, newHash, c.GetCache(kv.StorageDomain).GetBlockHash())
	assert.Equal(t, newHash, c.GetCache(kv.CodeDomain).GetBlockHash())
}

func TestStateCache_GetCache_OutOfBounds(t *testing.T) {
	c := NewStateCache(100, 100, 100, 100, 100)

	// Domain >= DomainLen should return nil
	cache := c.GetCache(kv.DomainLen)
	assert.Nil(t, cache)

	cache = c.GetCache(kv.Domain(100))
	assert.Nil(t, cache)
}

func TestStateCache_OnlyAccountStorageCodeCommitmentSupported(t *testing.T) {
	c := NewDefaultStateCache()

	supportedDomains := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain}
	unsupportedDomains := []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain}

	for _, d := range supportedDomains {
		assert.NotNil(t, c.GetCache(d), "domain %d should be supported", d)
	}

	for _, d := range unsupportedDomains {
		assert.Nil(t, c.GetCache(d), "domain %d should not be supported", d)
	}
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
			c.Put(makeAddr(i), makeValue(i))
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
			c.Put(makeAddr(i), makeCode(i))
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

	c.Put(kv.AccountsDomain, addr, accountData)
	c.Put(kv.StorageDomain, addr, storageData)
	c.Put(kv.CodeDomain, addr, codeData)

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

func TestBlockContinuity_Sequential(t *testing.T) {
	c := NewDefaultStateCache()

	block0 := common.Hash{}
	block1 := makeHash(1)
	block2 := makeHash(2)
	block3 := makeHash(3)

	// Block 1 (parent = empty)
	valid := c.ValidateAndPrepare(block0, block1)
	assert.True(t, valid)
	c.Put(kv.AccountsDomain, makeAddr(1), makeValue(1))

	// Block 2 (parent = block1)
	valid = c.ValidateAndPrepare(block1, block2)
	assert.True(t, valid)
	// Data from block 1 should still be there
	_, ok := c.Get(kv.AccountsDomain, makeAddr(1))
	assert.True(t, ok)

	// Block 3 (parent = block2)
	valid = c.ValidateAndPrepare(block2, block3)
	assert.True(t, valid)
}

func TestBlockContinuity_Reorg(t *testing.T) {
	c := NewDefaultStateCache()

	block1 := makeHash(1)
	block2 := makeHash(2)
	block2prime := makeHash(22) // fork

	// Build on block 1
	c.ValidateAndPrepare(common.Hash{}, block1)
	c.Put(kv.AccountsDomain, makeAddr(1), makeValue(1))

	// Continue to block 2
	c.ValidateAndPrepare(block1, block2)

	// Reorg: new block with different parent
	valid := c.ValidateAndPrepare(block1, block2prime)
	assert.False(t, valid) // Should detect mismatch
}
