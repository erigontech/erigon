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

package eip8297

import (
	"encoding/binary"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
)

// pbinTestKeccak is an independent Keccak-256 (x/crypto, not the fastkeccak the
// engine uses), so the vectors below are pinned against the spec rather than
// against the code under test.
func referenceKeccak(t *testing.T, parts ...[]byte) []byte {
	t.Helper()
	h := sha3.NewLegacyKeccak256()
	for _, p := range parts {
		_, err := h.Write(p)
		require.NoError(t, err)
	}
	return h.Sum(nil)
}

func referenceAddressHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	require.Len(t, b, 20)
	return b
}

// pbinTestAddress32 is the spec's address20_to_address32 (eip:"Tree embedding").
func referenceAddress32(addr []byte) []byte {
	a := make([]byte, 32)
	copy(a[32-len(addr):], addr)
	return a
}

func referenceBE32(v uint64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], v)
	return b
}

func referenceSlotBytes(v uint64) []byte { return referenceBE32(v) }

func referenceConcat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// Pins the derivation against the spec's test cases (eip:"Test Cases").
func TestPBinTreeKeyEIPVectors(t *testing.T) {
	t.Parallel()

	addr := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")
	addr32 := referenceAddress32(addr)
	stem := referenceKeccak(t, addr32)

	t.Run("basic-data", func(t *testing.T) {
		got := TreeKeyAccount(addr, BasicDataLeafKey)
		require.Len(t, got, AccountKeyLength)
		require.Equal(t, referenceConcat([]byte{0x00}, stem, []byte{0x00}), got)
	})

	t.Run("code-hash", func(t *testing.T) {
		got := TreeKeyAccount(addr, CodeHashLeafKey)
		require.Len(t, got, AccountKeyLength)
		require.Equal(t, referenceConcat([]byte{0x00}, stem, []byte{0x01}), got)
	})

	t.Run("slot-5-in-header", func(t *testing.T) {
		got := TreeKeyStorage(addr, referenceSlotBytes(5))
		require.Len(t, got, AccountKeyLength)
		require.Equal(t, referenceConcat([]byte{0x00}, stem, []byte{0x45}), got)
	})

	t.Run("slot-1000-in-storage-zone", func(t *testing.T) {
		suffix := referenceKeccak(t, addr32, referenceBE32(3))
		got := TreeKeyStorage(addr, referenceSlotBytes(1000))
		require.Len(t, got, StorageKeyLength)
		require.Equal(t, referenceConcat([]byte{0xFF}, stem, suffix, []byte{0xE8}), got)
	})
}

// Walks the header/storage-zone boundary and the group boundary. A mis-route
// there stays internally consistent, so a root-equality test cannot see it.
func TestPBinStorageZoneRouting(t *testing.T) {
	t.Parallel()

	addr := referenceAddressHex(t, "cafebabe000000000000000000000000deadbeef")
	addr32 := referenceAddress32(addr)
	stem := referenceKeccak(t, addr32)

	for _, tc := range []struct {
		name      string
		slot      uint64
		treeIndex uint64 // storage zone only
		subIndex  byte
		inHeader  bool
	}{
		{name: "slot-0", slot: 0, subIndex: 64, inHeader: true},
		{name: "slot-63-last-in-header", slot: 63, subIndex: 127, inHeader: true},
		{name: "slot-64-first-in-storage-zone", slot: 64, treeIndex: 0, subIndex: 64},
		{name: "slot-255-last-in-group-0", slot: 255, treeIndex: 0, subIndex: 255},
		{name: "slot-256-first-in-group-1", slot: 256, treeIndex: 1, subIndex: 0},
		{name: "slot-257", slot: 257, treeIndex: 1, subIndex: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := TreeKeyStorage(addr, referenceSlotBytes(tc.slot))
			if tc.inHeader {
				require.Len(t, got, AccountKeyLength)
				require.Equal(t, referenceConcat([]byte{0x00}, stem, []byte{tc.subIndex}), got)
				return
			}
			suffix := referenceKeccak(t, addr32, referenceBE32(tc.treeIndex))
			require.Len(t, got, StorageKeyLength)
			require.Equal(t, referenceConcat([]byte{0xFF}, stem, suffix, []byte{tc.subIndex}), got)
		})
	}
}

func TestPBinStorageZoneKeysAreDistinct(t *testing.T) {
	t.Parallel()

	addr := referenceAddressHex(t, "cafebabe000000000000000000000000deadbeef")
	seen := make(map[string]uint64)
	for _, slot := range []uint64{0, 1, 62, 63, 64, 65, 254, 255, 256, 257, 511, 512, 1000} {
		key := string(TreeKeyStorage(addr, referenceSlotBytes(slot)))
		if prev, ok := seen[key]; ok {
			t.Fatalf("slots %d and %d derive the same tree key", prev, slot)
		}
		seen[key] = slot
	}
}

// For slots too large for a uint64 the tree index is a 31-byte shift of the
// slot, not arithmetic on it.
func TestPBinHighSlotRouting(t *testing.T) {
	t.Parallel()

	addr := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")
	addr32 := referenceAddress32(addr)
	stem := referenceKeccak(t, addr32)

	slot := make([]byte, 32)
	for i := range slot {
		slot[i] = byte(i + 1)
	}
	treeIndex := append([]byte{0x00}, slot[:31]...)
	suffix := referenceKeccak(t, addr32, treeIndex)

	got := TreeKeyStorage(addr, slot)
	require.Len(t, got, StorageKeyLength)
	require.Equal(t, referenceConcat([]byte{0xFF}, stem, suffix, []byte{slot[31]}), got)
}

// The stem digest covers the 32-byte address, not the 20-byte one.
func TestPBinAddr32Padding(t *testing.T) {
	t.Parallel()

	addr := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")
	a32 := RightAlign32(addr)
	require.Equal(t, make([]byte, 12), a32[:12])
	require.Equal(t, addr, a32[12:])

	key := TreeKeyAccount(addr, BasicDataLeafKey)
	require.Equal(t, referenceKeccak(t, referenceAddress32(addr)), key[1:33])
	require.NotEqual(t, referenceKeccak(t, addr), key[1:33])
}

func TestPBinDigestCacheFollowsSelectedHashSuite(t *testing.T) {
	previous := HashSuiteName()
	t.Cleanup(func() { require.NoError(t, SetHashSuite(previous)) })
	address := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")
	cache := DigestCache{}

	require.NoError(t, SetHashSuite(HashKeccak))
	keccakKey := cache.AccountKey(address, BasicDataLeafKey)
	require.NoError(t, SetHashSuite(HashBlake3))
	got := cache.AccountKey(address, BasicDataLeafKey)
	expectedCache := DigestCache{Sum: func(data []byte) common.Hash { return common.Hash(blake3.Sum256(data)) }}
	want := expectedCache.AccountKey(address, BasicDataLeafKey)
	require.NotEqual(t, keccakKey, got)
	require.Equal(t, want, got)

	slot := referenceSlotBytes(256)
	cache = DigestCache{}
	require.NoError(t, SetHashSuite(HashKeccak))
	keccakStorageKey := cache.StorageKey(address, slot)
	require.NoError(t, SetHashSuite(HashBlake3))
	gotStorageKey := cache.StorageKey(address, slot)
	expectedStorageKey := expectedCache.StorageKey(address, slot)
	require.NotEqual(t, keccakStorageKey, gotStorageKey)
	require.Equal(t, expectedStorageKey, gotStorageKey)

	hasher := DigestCache{}
	require.NoError(t, SetHashSuite(HashKeccak))
	keccakPooledKey := hasher.AccountKey(address, BasicDataLeafKey)
	require.NoError(t, SetHashSuite(HashBlake3))
	gotPooledKey := hasher.AccountKey(address, BasicDataLeafKey)
	require.Equal(t, want, gotPooledKey)
	require.NotEqual(t, keccakPooledKey, gotPooledKey)
}

// The keyHasher contract: the primary leaf's tree key, sized 34 or 66 by zone.
func TestPBinKeyHasherPrimaryLeaf(t *testing.T) {
	t.Parallel()

	cache := DigestCache{}
	addr := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")

	got := cache.AccountKey(addr, BasicDataLeafKey)
	require.Len(t, got, AccountKeyLength)
	require.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), got)

	got = cache.StorageKey(addr, referenceSlotBytes(1000))
	require.Len(t, got, StorageKeyLength)
	require.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(1000)), got)
}

func TestPBinKeyHasherRejectsMalformedPlainKey(t *testing.T) {
	t.Parallel()

	cache := DigestCache{}
	require.Panics(t, func() { cache.AccountKey(make([]byte, 33), BasicDataLeafKey) })
	require.Panics(t, func() { cache.StorageKey(nil, make([]byte, 33)) })
}

func TestPBinKeyHasherSharedAcrossBuffers(t *testing.T) {
	t.Parallel()

	addrs := [][]byte{
		referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314"),
		referenceAddressHex(t, "cafebabe000000000000000000000000deadbeef"),
	}
	slots := []uint64{0, 64, 256, 1000}

	caches := []*DigestCache{{}, {}}

	var wg sync.WaitGroup
	for _, cache := range caches {
		wg.Go(func() {
			for range 50 {
				for _, addr := range addrs {
					assert.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), cache.AccountKey(addr, BasicDataLeafKey))
					for _, slot := range slots {
						plainKey := referenceConcat(addr, referenceSlotBytes(slot))
						assert.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(slot)), cache.StorageKey(plainKey[:20], plainKey[20:]),
							"addr %x slot %d", addr, slot)
					}
				}
			}
		})
	}
	wg.Wait()
}

// Interleaves addresses and slot groups through one hasher: a cache entry kept
// past its address or tree index would place a leaf under the wrong stem.
func TestPBinDigestCacheMatchesFreshDerivation(t *testing.T) {
	t.Parallel()

	addrs := [][]byte{
		referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314"),
		referenceAddressHex(t, "cafebabe000000000000000000000000deadbeef"),
	}
	slots := []uint64{0, 63, 64, 255, 256, 257, 1000, 100000}

	cache := DigestCache{}
	for range 3 {
		for _, addr := range addrs {
			require.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), cache.AccountKey(addr, BasicDataLeafKey))
			for _, slot := range slots {
				plainKey := referenceConcat(addr, referenceSlotBytes(slot))
				require.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(slot)), cache.StorageKey(plainKey[:20], plainKey[20:]),
					"addr %x slot %d", addr, slot)
			}
		}
	}
}
