// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is a modified copy of github.com/elastic/go-freelru v0.16.0.
// Changed by The Erigon Authors: the element array is held as fixed-size slabs
// appended on demand instead of one array sized from capacity, so the table is
// never resized; the sharded and synced variants are not vendored.

package slablru

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

const (
	// FNV-1a
	offset32 = uint32(2166136261)
	prime32  = uint32(16777619)

	// Init32 is what 32 bits hash values should be initialized with.
	Init32 = offset32
)

func hashUint64(i uint64) uint32 {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], i)
	h := Init32
	h = (h ^ uint32(b[0])) * prime32
	h = (h ^ uint32(b[1])) * prime32
	h = (h ^ uint32(b[2])) * prime32
	h = (h ^ uint32(b[3])) * prime32
	h = (h ^ uint32(b[4])) * prime32
	h = (h ^ uint32(b[5])) * prime32
	h = (h ^ uint32(b[6])) * prime32
	h = (h ^ uint32(b[7])) * prime32
	return h
}

func setupCache(t *testing.T, cache Cache[uint64, uint64],
	evictCounter *uint64) Cache[uint64, uint64] {
	onEvict := func(k uint64, v uint64) {
		FatalIf(t, k+1 != v, "Evict value not matching (%v+1 != %v)", k, v)
		if evictCounter != nil {
			*evictCounter++
		}
	}

	cache.SetOnEvict(onEvict)

	return cache
}

func makeCache(t *testing.T, capacity uint32, evictCounter *uint64) Cache[uint64, uint64] {
	cache, err := New[uint64, uint64](capacity, hashUint64)
	FatalIf(t, err != nil, "Failed to create LRU: %v", err)

	return setupCache(t, cache, evictCounter)
}

// Only the single LRU is vendored here, so the upstream suite's synced variant
// runs against the same implementation.
func makeSyncedLRU(t *testing.T, capacity uint32, evictCounter *uint64) Cache[uint64, uint64] {
	return makeCache(t, capacity, evictCounter)
}

func TestLRU(t *testing.T) {
	const CAP = 32

	evictCounter := uint64(0)
	testCache(t, CAP, makeCache(t, CAP, &evictCounter), &evictCounter)
}

func TestSyncedLRU(t *testing.T) {
	const CAP = 32

	evictCounter := uint64(0)
	testCache(t, CAP, makeSyncedLRU(t, CAP, &evictCounter), &evictCounter)
}

func testCache(t *testing.T, cAP uint64, cache Cache[uint64, uint64], evictCounter *uint64) {
	for i := uint64(0); i < cAP*2; i++ {
		cache.Add(i, i+1)
	}
	FatalIf(t, cache.Len() != int(cAP), "Unexpected number of entries: %v (!= %d)", cache.Len(),
		cAP)
	FatalIf(t, *evictCounter != cAP, "Unexpected number of evictions: %v (!= %d)", evictCounter,
		cAP)

	keys := cache.Keys()
	for i, k := range keys {
		if v, ok := cache.Get(k); !ok || v != k+1 || v != uint64(i+int(cAP)+1) {
			t.Fatalf("Mismatch of key %v (ok=%v v=%v)\n%v", k, ok, v, keys)
		}
	}

	for i := range cAP {
		if _, ok := cache.Get(i); ok {
			t.Fatalf("Missing eviction of %d", i)
		}
	}

	for i := cAP; i < cAP*2; i++ {
		if _, ok := cache.Get(i); !ok {
			t.Fatalf("Unexpected eviction of %d", i)
		}
	}

	FatalIf(t, cache.Remove(cAP*2), "Unexpected success removing %d", cAP*2)
	FatalIf(t, !cache.Remove(cAP*2-1), "Failed to remove most recent entry %d", cAP*2-1)
	FatalIf(t, !cache.Remove(cAP), "Failed to remove oldest entry %d", cAP)
	FatalIf(t, *evictCounter != cAP+2, "Unexpected # of evictions: %d (!= %d)", evictCounter, cAP+2)
	FatalIf(t, cache.Len() != int(cAP-2), "Unexpected # of entries: %d (!= %d)", cache.Len(), cAP-2)
}

func TestLRU_Add(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeCache(t, 1, &evictCounter)

	FatalIf(t, cache.Add(1, 2) == true || evictCounter != 0, "Unexpected eviction")
	FatalIf(t, cache.Add(3, 4) == false || evictCounter != 1, "Missing eviction")
}

func TestSyncedLRU_Add(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeSyncedLRU(t, 1, &evictCounter)

	FatalIf(t, cache.Add(1, 2) == true || evictCounter != 0, "Unexpected eviction")
	FatalIf(t, cache.Add(3, 4) == false || evictCounter != 1, "Missing eviction")
}

func TestLRU_Purge(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeCache(t, 3, &evictCounter)

	FatalIf(t, cache.Add(1, 2) == true || evictCounter != 0, "Unexpected eviction")
	FatalIf(t, cache.Add(3, 4) == true || evictCounter != 0, "Unexpected eviction")
	FatalIf(t, cache.Add(4, 5) == true || evictCounter != 0, "Unexpected eviction")
	FatalIf(t, cache.Len() != 3, "Unexpected length")

	cache.Purge()
	FatalIf(t, cache.Len() != 0, "Unexpected length")
}

func TestLRU_Remove(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeCache(t, 2, &evictCounter)
	cache.Add(1, 2)
	cache.Add(3, 4)

	FatalIf(t, !cache.Remove(1), "Failed to remove most recent entry %d", 1)
	FatalIf(t, !cache.Remove(3), "Failed to remove most recent entry %d", 3)
	FatalIf(t, evictCounter != 2, "Unexpected # of evictions: %d (!= %d)", evictCounter, 2)
	FatalIf(t, cache.Len() != 0, "Unexpected # of entries: %d (!= %d)", cache.Len(), 0)
}

func TestSyncedLRU_Remove(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeSyncedLRU(t, 2, &evictCounter)
	cache.Add(1, 2)
	cache.Add(3, 4)

	FatalIf(t, !cache.Remove(1), "Failed to remove most recent entry %d", 1)
	FatalIf(t, !cache.Remove(3), "Failed to remove most recent entry %d", 3)
	FatalIf(t, evictCounter != 2, "Unexpected # of evictions: %d (!= %d)", evictCounter, 2)
	FatalIf(t, cache.Len() != 0, "Unexpected # of entries: %d (!= %d)", cache.Len(), 0)
}

func TestLRU_RemoveOldest(t *testing.T) {
	evictCounter := uint64(0)
	cache := makeCache(t, 2, &evictCounter)
	cache.Add(1, 2)
	cache.Add(3, 4)

	k, v, ok := cache.RemoveOldest()
	FatalIf(t, !ok, "Failed to remove oldest entry")
	FatalIf(t, k != 1, "Unexpected key=%d (!= %d)", k, 1)
	FatalIf(t, v != 2, "Unexpected value: %d (!= %d)", v, 2)

	k, v, ok = cache.RemoveOldest()
	FatalIf(t, !ok, "Failed to remove oldest entry")
	FatalIf(t, k != 3, "Unexpected key=%d (!= %d)", k, 3)
	FatalIf(t, v != 4, "Unexpected value: %d (!= %d)", v, 4)

	_, _, ok = cache.RemoveOldest()
	FatalIf(t, ok, "Unexpectedly removing oldest entry was ok")

	FatalIf(t, evictCounter != 2, "Unexpected # of evictions: %d (!= %d)", evictCounter, 2)
	FatalIf(t, cache.Len() != 0, "Unexpected # of entries: %d (!= %d)", cache.Len(), 0)
}

func testCacheAddWithExpire(t *testing.T, cache Cache[uint64, uint64]) {
	// check for LRU default lifetime + element specific override
	cache.SetLifetime(100 * time.Millisecond)
	cache.Add(1, 2)
	cache.AddWithLifetime(3, 4, 200*time.Millisecond)
	_, ok := cache.Get(1)
	FatalIf(t, !ok, "Failed to get")
	time.Sleep(101 * time.Millisecond)
	_, ok = cache.Get(1)
	FatalIf(t, ok, "Expected expiration did not happen")
	_, ok = cache.Get(3)
	FatalIf(t, !ok, "Failed to get")
	time.Sleep(100 * time.Millisecond)
	_, ok = cache.Get(3)
	FatalIf(t, ok, "Expected expiration did not happen")
	FatalIf(t, cache.Len() != 0, "Cache not empty")

	cache.Add(1, 2)
	cache.Purge()
	FatalIf(t, cache.Len() != 0, "Cache not empty")

	cache.AddWithLifetime(1, 2, 100*time.Millisecond)
	cache.PurgeExpired() // should be a no-op
	FatalIf(t, cache.Len() != 1, "Expected PurgeExpired to be a no-op")
	time.Sleep(101 * time.Millisecond)
	cache.PurgeExpired()
	FatalIf(t, cache.Len() != 0, "Cache not empty")

	// check for element specific lifetime
	cache.SetLifetime(0)
	cache.AddWithLifetime(1, 2, 100*time.Millisecond)
	_, ok = cache.Get(1)
	FatalIf(t, !ok, "Failed to get")
	time.Sleep(101 * time.Millisecond)
	_, ok = cache.Get(1)
	FatalIf(t, ok, "Expected expiration did not happen")
}

func TestLRU_AddWithExpire(t *testing.T) {
	testCacheAddWithExpire(t, makeCache(t, 2, nil))
}

func TestSyncedLRU_AddWithExpire(t *testing.T) {
	testCacheAddWithExpire(t, makeSyncedLRU(t, 2, nil))
}

func testCacheAddWithRefresh(t *testing.T, cache Cache[uint64, uint64]) {
	cache.AddWithLifetime(1, 2, 100*time.Millisecond)
	cache.AddWithLifetime(2, 3, 100*time.Millisecond)
	_, ok := cache.Get(1)
	FatalIf(t, !ok, "Failed to get")

	time.Sleep(101 * time.Millisecond)
	_, ok = cache.GetAndRefresh(1, 0)
	FatalIf(t, !ok, "Unexpected expiration")
	_, ok = cache.GetAndRefresh(2, 0)
	FatalIf(t, !ok, "Unexpected expiration")
}

func TestLRU_AddWithRefresh(t *testing.T) {
	testCacheAddWithRefresh(t, makeCache(t, 2, nil))
}

func TestSyncedLRU_AddWithRefresh(t *testing.T) {
	testCacheAddWithRefresh(t, makeSyncedLRU(t, 2, nil))
}

func TestLRUMatch(t *testing.T) {
	testCacheMatch(t, makeCache(t, 2, nil), 128)
}

func TestSyncedLRUMatch(t *testing.T) {
	testCacheMatch(t, makeSyncedLRU(t, 2, nil), 128)
}

// Test that Go map and the Cache stay in sync when adding
// and randomly removing elements.
func testCacheMatch(t *testing.T, cache Cache[uint64, uint64], cAP int) {
	backup := make(map[uint64]uint64, cAP)

	onEvict := func(k uint64, v uint64) {
		FatalIf(t, k != v, "Evict value not matching (%v != %v)", k, v)
		delete(backup, k)
	}

	cache.SetOnEvict(onEvict)

	for n := range 100000 {
		i := uint64(n)
		cache.Add(i, i)
		backup[i] = i

		// ~33% chance to remove a random element
		r := i - uint64(rand.Int()%(cAP*3)) // nolint:gosec
		cache.Remove(r)

		FatalIf(t, cache.Len() != len(backup), "Len does not match (%d vs %d)",
			cache.Len(), len(backup))

		keys := cache.Keys()
		FatalIf(t, len(keys) != len(backup), "Number of keys does not match (%d vs %d)",
			len(keys), len(backup))

		for _, key := range keys {
			backupVal, ok := backup[key]
			FatalIf(t, !ok, "Failed to find key %d in map", key)

			val, ok := cache.Peek(key)
			FatalIf(t, !ok, "Failed to find key %d in Cache", key)

			FatalIf(t, backupVal != val, "Unexpected mismatch of values: %#v %#v",
				backupVal, val)
		}

		for k, v := range backup {
			val, ok := cache.Peek(k)
			FatalIf(t, !ok, "Failed to find key %d in Cache (i=%d)", k, i)
			FatalIf(t, v != val, "Unexpected mismatch of values: %#v %#v", v, val)
		}
	}
}

func FatalIf(t *testing.T, fail bool, fmt string, args ...any) {
	if fail {
		t.Logf(fmt, args...)
		panic(fail)
	}
}

// This following tests are for memory comparison.
// See also TestSimpleLRUAdd in the bench/ directory.

const count = 1000

// GOGC=off go test -memprofile=mem.out -test.memprofilerate=1 -count 1 -run TestMapAdd
// go tool pprof mem.out
// (then check the top10)
func TestMapAdd(_ *testing.T) {
	cache := make(map[uint64]uint64, count)

	var val uint64
	for i := range count {
		cache[uint64(i)] = val
	}
}

// GOGC=off go test -memprofile=mem.out -test.memprofilerate=1 -count 1 -run TestLRUAdd
// go tool pprof mem.out
// (then check the top10)
func TestLRUAdd(t *testing.T) {
	cache := makeCache(t, count, nil)

	var val uint64
	for i := range count {
		cache.Add(uint64(i), val)
	}
}

// GOGC=off go test -memprofile=mem.out -test.memprofilerate=1 -count 1 -run TestSyncedLRUAdd
// go tool pprof mem.out
// (then check the top10)
func TestSyncedLRUAdd(t *testing.T) {
	cache := makeSyncedLRU(t, count, nil)

	var val uint64
	for i := range count {
		cache.Add(uint64(i), val)
	}
}

func TestLRUMetrics(t *testing.T) {
	cache := makeCache(t, 1, nil)
	testMetrics(t, cache)
}

func testMetrics(t *testing.T, cache Cache[uint64, uint64]) {
	cache.Add(1, 2) // insert
	cache.Add(3, 4) // insert and eviction
	cache.Get(1)    // miss
	cache.Get(3)    // hit
	cache.Remove(3) // removal

	m := cache.Metrics()
	FatalIf(t, m.Inserts != 2, "Unexpected inserts: %d (!= %d)", m.Inserts, 2)
	FatalIf(t, m.Hits != 1, "Unexpected hits: %d (!= %d)", m.Hits, 1)
	FatalIf(t, m.Misses != 1, "Unexpected misses: %d (!= %d)", m.Misses, 1)
	FatalIf(t, m.Evictions != 1, "Unexpected evictions: %d (!= %d)", m.Evictions, 1)
	FatalIf(t, m.Removals != 1, "Unexpected evictions: %d (!= %d)", m.Removals, 1)
	FatalIf(t, m.Collisions != 0, "Unexpected collisions: %d (!= %d)", m.Collisions, 0)
}

// The table costs what it holds, not what it may hold: elements arrive one slab
// at a time, so a cache configured for a large capacity but holding little is
// cheap and never has to be resized.
func TestSlabsGrowOnDemand(t *testing.T) {
	const capacity = 1 << 20
	lru, err := New[uint64, uint64](capacity, hashUint64)
	FatalIf(t, err != nil, "Failed to create LRU: %v", err)

	if len(lru.slabs) != 0 {
		t.Fatalf("empty cache allocated %d slabs", len(lru.slabs))
	}
	for i := range uint64(slabEntries) {
		lru.Add(i, i)
	}
	if len(lru.slabs) != 1 {
		t.Fatalf("%d entries: got %d slabs, want 1", slabEntries, len(lru.slabs))
	}
	lru.Add(slabEntries, slabEntries)
	if len(lru.slabs) != 2 {
		t.Fatalf("%d entries: got %d slabs, want 2", slabEntries+1, len(lru.slabs))
	}
	// Everything is still addressable across the slab boundary.
	for i := range uint64(slabEntries + 1) {
		v, ok := lru.Get(i)
		FatalIf(t, !ok, "key %d lost across a slab boundary", i)
		FatalIf(t, v != i, "key %d: got %d", i, v)
	}
}
