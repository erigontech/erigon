// Copyright 2026 The Erigon Authors
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
	"bytes"
	"crypto/rand"
	"testing"
)

// TestWarmupCache_Basic tests basic put/get operations
func TestWarmupCache_Basic(t *testing.T) {
	cache := NewWarmupCache()

	// Test branch operations
	branchKey := []byte("test-branch-key-12345678901234567890")
	branchData := []byte("branch-data-content")
	cache.PutBranch(branchKey, branchData)

	gotData, found := cache.GetBranch(branchKey)
	if !found {
		t.Fatal("expected to find branch")
	}
	if !bytes.Equal(gotData, branchData) {
		t.Errorf("branch data mismatch: got %x, want %x", gotData, branchData)
	}

	// Test account operations
	accountKey := []byte("12345678901234567890") // 20 bytes
	accountUpdate := &Update{Flags: BalanceUpdate}
	cache.PutAccount(accountKey, accountUpdate)

	gotUpdate, found := cache.GetAccount(accountKey)
	if !found {
		t.Fatal("expected to find account")
	}
	if gotUpdate.Flags != BalanceUpdate {
		t.Errorf("account flags mismatch: got %v, want %v", gotUpdate.Flags, BalanceUpdate)
	}

	// Test storage operations
	storageKey := make([]byte, 52)
	rand.Read(storageKey)
	storageUpdate := &Update{Flags: StorageUpdate, StorageLen: 5}
	copy(storageUpdate.Storage[:], "hello")
	cache.PutStorage(storageKey, storageUpdate)

	gotStorage, found := cache.GetStorage(storageKey)
	if !found {
		t.Fatal("expected to find storage")
	}
	if gotStorage.Flags != StorageUpdate {
		t.Errorf("storage flags mismatch")
	}
	if !bytes.Equal(gotStorage.Storage[:gotStorage.StorageLen], []byte("hello")) {
		t.Errorf("storage data mismatch")
	}
}

// TestWarmupCache_NotFound tests cache misses
func TestWarmupCache_NotFound(t *testing.T) {
	cache := NewWarmupCache()

	_, found := cache.GetBranch([]byte("nonexistent"))
	if found {
		t.Error("expected not to find nonexistent branch")
	}

	_, found = cache.GetAccount([]byte("12345678901234567890"))
	if found {
		t.Error("expected not to find nonexistent account")
	}

	_, found = cache.GetStorage(make([]byte, 52))
	if found {
		t.Error("expected not to find nonexistent storage")
	}
}

// TestWarmupCache_Eviction tests key eviction
func TestWarmupCache_Eviction(t *testing.T) {
	cache := NewWarmupCache()

	key := []byte("12345678901234567890")
	cache.PutAccount(key, &Update{Flags: BalanceUpdate})

	// Should find before eviction
	_, found := cache.GetAccount(key)
	if !found {
		t.Fatal("expected to find account before eviction")
	}

	// Evict the key
	cache.EvictAccount(key)

	// Should not find after eviction
	_, found = cache.GetAccount(key)
	if found {
		t.Error("expected not to find account after eviction")
	}
}

// TestWarmupCache_Enable tests enable/disable
func TestWarmupCache_Enable(t *testing.T) {
	cache := NewWarmupCache()

	key := []byte("12345678901234567890")
	cache.PutAccount(key, &Update{Flags: BalanceUpdate})

	// Should find when enabled
	_, found := cache.GetAccount(key)
	if !found {
		t.Fatal("expected to find account when enabled")
	}

	// Disable cache
	cache.Enable(false)

	// Should not find when disabled
	_, found = cache.GetAccount(key)
	if found {
		t.Error("expected not to find account when disabled")
	}

	// Re-enable
	cache.Enable(true)

	// Should find again
	_, found = cache.GetAccount(key)
	if !found {
		t.Error("expected to find account after re-enabling")
	}
}

// TestWarmupCache_Clear tests clearing the cache
func TestWarmupCache_Clear(t *testing.T) {
	cache := NewWarmupCache()

	// Add some data
	cache.PutAccount([]byte("12345678901234567890"), &Update{})
	cache.PutStorage(make([]byte, 52), &Update{})
	cache.PutBranch([]byte("branch"), []byte("data"))

	// Clear
	cache.Clear()

	// Should not find anything
	_, found := cache.GetAccount([]byte("12345678901234567890"))
	if found {
		t.Error("expected not to find account after clear")
	}
}

// TestWarmupCache_KeyPadding tests that shorter keys work correctly
func TestWarmupCache_KeyPadding(t *testing.T) {
	cache := NewWarmupCache()

	// Test with a key shorter than the fixed size
	shortKey := []byte("short")
	cache.PutBranch(shortKey, []byte("data"))

	gotData, found := cache.GetBranch(shortKey)
	if !found {
		t.Fatal("expected to find branch with short key")
	}
	if !bytes.Equal(gotData, []byte("data")) {
		t.Errorf("data mismatch")
	}

	// Ensure different short keys don't collide
	shortKey2 := []byte("other")
	cache.PutBranch(shortKey2, []byte("data2"))

	gotData, found = cache.GetBranch(shortKey)
	if !found || !bytes.Equal(gotData, []byte("data")) {
		t.Error("first key affected by second key")
	}

	gotData2, found := cache.GetBranch(shortKey2)
	if !found || !bytes.Equal(gotData2, []byte("data2")) {
		t.Error("second key not found or wrong data")
	}
}

// generateTestKeys creates random keys of the given size
func generateTestKeys(n int, size int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = make([]byte, size)
		rand.Read(keys[i])
	}
	return keys
}

// BenchmarkWarmupCache_Branch benchmarks branch key operations
func BenchmarkWarmupCache_Branch(b *testing.B) {
	cache := NewWarmupCache()

	// Pre-populate with some data
	keys := generateTestKeys(10000, 52)
	data := make([]byte, 100)
	rand.Read(data)

	for _, key := range keys {
		cache.PutBranch(key, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		cache.GetBranch(key)
	}
}

// BenchmarkWarmupCache_Branch_Put benchmarks Put operations
func BenchmarkWarmupCache_Branch_Put(b *testing.B) {
	cache := NewWarmupCache()
	keys := generateTestKeys(b.N, 52)
	data := make([]byte, 100)
	rand.Read(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.PutBranch(keys[i], data)
	}
}

// BenchmarkWarmupCache_Account benchmarks account key operations (20 bytes)
func BenchmarkWarmupCache_Account(b *testing.B) {
	cache := NewWarmupCache()
	keys := generateTestKeys(10000, 20)
	update := &Update{}

	for _, key := range keys {
		cache.PutAccount(key, update)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		cache.GetAccount(key)
	}
}

// BenchmarkWarmupCache_Storage benchmarks storage key operations (52 bytes)
func BenchmarkWarmupCache_Storage(b *testing.B) {
	cache := NewWarmupCache()
	keys := generateTestKeys(10000, 52)
	update := &Update{}

	for _, key := range keys {
		cache.PutStorage(key, update)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		cache.GetStorage(key)
	}
}

// BenchmarkWarmupCache_Mixed simulates realistic mixed workload
func BenchmarkWarmupCache_Mixed(b *testing.B) {
	cache := NewWarmupCache()
	accountKeys := generateTestKeys(1000, 20)
	storageKeys := generateTestKeys(5000, 52)
	branchKeys := generateTestKeys(2000, 32)
	update := &Update{}
	data := make([]byte, 100)

	// Pre-populate
	for _, key := range accountKeys {
		cache.PutAccount(key, update)
	}
	for _, key := range storageKeys {
		cache.PutStorage(key, update)
	}
	for _, key := range branchKeys {
		cache.PutBranch(key, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			cache.GetAccount(accountKeys[i%len(accountKeys)])
		case 1:
			cache.GetStorage(storageKeys[i%len(storageKeys)])
		case 2:
			cache.GetBranch(branchKeys[i%len(branchKeys)])
		}
	}
}

// BenchmarkComparison_Map_100k benchmarks map with 100k entries
func BenchmarkComparison_Map_100k(b *testing.B) {
	cache := NewWarmupCache()
	keys := generateTestKeys(100000, 52)
	update := &Update{}

	for _, key := range keys {
		cache.PutStorage(key, update)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.GetStorage(keys[i%len(keys)])
	}
}
