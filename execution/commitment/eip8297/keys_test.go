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

	require.NoError(t, SetHashSuite(HashKeccak))
	keccakKey := TreeKeyAccount(address, BasicDataLeafKey)
	require.NoError(t, SetHashSuite(HashBlake3))
	got := TreeKeyAccount(address, BasicDataLeafKey)
	cache := DigestCache{Sum: func(data []byte) common.Hash { return common.Hash(blake3.Sum256(data)) }}
	want := cache.AccountKey(address, BasicDataLeafKey)
	require.NotEqual(t, keccakKey, got)
	require.Equal(t, want, got)
}

// The keyHasher contract: the primary leaf's tree key, sized 34 or 66 by zone.
func TestPBinKeyHasherPrimaryLeaf(t *testing.T) {
	t.Parallel()

	hasher := KeyHasher()
	addr := referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314")

	got := hasher(addr)
	require.Len(t, got, AccountKeyLength)
	require.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), got)

	got = hasher(referenceConcat(addr, referenceSlotBytes(1000)))
	require.Len(t, got, StorageKeyLength)
	require.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(1000)), got)
}

func TestPBinKeyHasherRejectsMalformedPlainKey(t *testing.T) {
	t.Parallel()

	hasher := KeyHasher()
	require.Panics(t, func() { hasher(make([]byte, 33)) })
	require.Panics(t, func() { hasher(nil) })
}

func TestPBinKeyHasherSharedAcrossBuffers(t *testing.T) {
	t.Parallel()

	addrs := [][]byte{
		referenceAddressHex(t, "0102030405060708090a0b0c0d0e0f1011121314"),
		referenceAddressHex(t, "cafebabe000000000000000000000000deadbeef"),
	}
	slots := []uint64{0, 64, 256, 1000}

	hashers := []KeyHasherFunc{KeyHasher(), KeyHasher()}

	var wg sync.WaitGroup
	for _, hasher := range hashers {
		wg.Go(func() {
			for range 50 {
				for _, addr := range addrs {
					assert.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), hasher(addr))
					for _, slot := range slots {
						plainKey := referenceConcat(addr, referenceSlotBytes(slot))
						assert.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(slot)), hasher(plainKey),
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

	hasher := KeyHasher()
	for range 3 {
		for _, addr := range addrs {
			require.Equal(t, TreeKeyAccount(addr, BasicDataLeafKey), hasher(addr))
			for _, slot := range slots {
				plainKey := referenceConcat(addr, referenceSlotBytes(slot))
				require.Equal(t, TreeKeyStorage(addr, referenceSlotBytes(slot)), hasher(plainKey),
					"addr %x slot %d", addr, slot)
			}
		}
	}
}

func TestPBinLeafSuffixBits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		zone          byte
		recordKeyBits int
		want          int
	}{
		{name: "account start", zone: AccountZone, recordKeyBits: 0, want: 271},
		{name: "account header slot zero", zone: AccountZone, recordKeyBits: 265, want: 6},
		{name: "account seven bits", zone: AccountZone, recordKeyBits: 264, want: 7},
		{name: "account last branch", zone: AccountZone, recordKeyBits: 271, want: 0},
		{name: "code start", zone: CodeZone, recordKeyBits: 0, want: 271},
		{name: "code last branch", zone: CodeZone, recordKeyBits: 271, want: 0},
		{name: "storage start", zone: StorageZone, recordKeyBits: 0, want: 527},
		{name: "storage around record depth", zone: StorageZone, recordKeyBits: 275, want: 252},
		{name: "storage two hundred forty-eight bits", zone: StorageZone, recordKeyBits: 279, want: 248},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := LeafSuffixBits(tc.zone, tc.recordKeyBits)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			keyLen, known := ZoneKeyLength(tc.zone)
			require.True(t, known)
			key := make([]byte, keyLen)
			for i := range key {
				key[i] = byte(i*17 + 3)
			}
			key[0] = tc.zone
			path := PathFromBytes(key)
			prefix := path.Slice(0, int16(tc.recordKeyBits))
			branch := path.Slice(int16(tc.recordKeyBits), int16(tc.recordKeyBits+1))
			suffix := path.Slice(int16(tc.recordKeyBits+1), int16(tc.recordKeyBits+1+got))
			prefix.Append(&branch)
			prefix.Append(&suffix)
			require.Equal(t, path, prefix)
		})
	}
}

func TestPBinLeafSuffixBitsRejectsInvalidDepth(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		zone          byte
		recordKeyBits int
	}{
		{name: "account at key length", zone: AccountZone, recordKeyBits: AccountKeyLength * 8},
		{name: "account past key length", zone: AccountZone, recordKeyBits: AccountKeyLength*8 + 1},
		{name: "code at key length", zone: CodeZone, recordKeyBits: CodeKeyLength * 8},
		{name: "storage at key length", zone: StorageZone, recordKeyBits: StorageKeyLength * 8},
		{name: "negative depth", zone: AccountZone, recordKeyBits: -1},
		{name: "unknown zone", zone: 0x02, recordKeyBits: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := LeafSuffixBits(tc.zone, tc.recordKeyBits)
			require.Error(t, err)
		})
	}
}
