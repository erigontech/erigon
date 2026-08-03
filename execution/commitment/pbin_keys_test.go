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
	"encoding/binary"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
)

// pbinTestKeccak is an independent Keccak-256 (x/crypto, not the fastkeccak the
// engine uses), so the vectors below are pinned against the spec rather than
// against the code under test.
func pbinTestKeccak(t *testing.T, parts ...[]byte) []byte {
	t.Helper()
	h := sha3.NewLegacyKeccak256()
	for _, p := range parts {
		_, err := h.Write(p)
		require.NoError(t, err)
	}
	return h.Sum(nil)
}

func pbinTestAddr(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	require.Len(t, b, 20)
	return b
}

// pbinTestAddress32 is the spec's address20_to_address32 (eip:291-296).
func pbinTestAddress32(addr []byte) []byte {
	a := make([]byte, 32)
	copy(a[32-len(addr):], addr)
	return a
}

func pbinTestBE32(v uint64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], v)
	return b
}

func pbinTestSlot(v uint64) []byte { return pbinTestBE32(v) }

func pbinTestConcat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// Pins the derivation against the spec's test cases (eip:583-630).
func TestPBinTreeKeyEIPVectors(t *testing.T) {
	t.Parallel()

	addr := pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314")
	addr32 := pbinTestAddress32(addr)
	stem := pbinTestKeccak(t, addr32)

	t.Run("basic-data", func(t *testing.T) {
		got := pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)
		require.Len(t, got, pbinAccountKeyLength)
		require.Equal(t, pbinTestConcat([]byte{0x00}, stem, []byte{0x00}), got)
	})

	t.Run("code-hash", func(t *testing.T) {
		got := pbinTreeKeyAccount(addr, pbinCodeHashLeafKey)
		require.Len(t, got, pbinAccountKeyLength)
		require.Equal(t, pbinTestConcat([]byte{0x00}, stem, []byte{0x01}), got)
	})

	t.Run("slot-5-in-header", func(t *testing.T) {
		got := pbinTreeKeyStorage(addr, pbinTestSlot(5))
		require.Len(t, got, pbinAccountKeyLength)
		require.Equal(t, pbinTestConcat([]byte{0x00}, stem, []byte{0x45}), got)
	})

	t.Run("slot-1000-in-storage-zone", func(t *testing.T) {
		suffix := pbinTestKeccak(t, addr32, pbinTestBE32(3))
		got := pbinTreeKeyStorage(addr, pbinTestSlot(1000))
		require.Len(t, got, pbinStorageKeyLength)
		require.Equal(t, pbinTestConcat([]byte{0xFF}, stem, suffix, []byte{0xE8}), got)
	})
}

// Walks the header/storage-zone boundary and the group boundary. A mis-route
// there stays internally consistent, so a root-equality test cannot see it.
func TestPBinStorageZoneRouting(t *testing.T) {
	t.Parallel()

	addr := pbinTestAddr(t, "cafebabe000000000000000000000000deadbeef")
	addr32 := pbinTestAddress32(addr)
	stem := pbinTestKeccak(t, addr32)

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
			got := pbinTreeKeyStorage(addr, pbinTestSlot(tc.slot))
			if tc.inHeader {
				require.Len(t, got, pbinAccountKeyLength)
				require.Equal(t, pbinTestConcat([]byte{0x00}, stem, []byte{tc.subIndex}), got)
				return
			}
			suffix := pbinTestKeccak(t, addr32, pbinTestBE32(tc.treeIndex))
			require.Len(t, got, pbinStorageKeyLength)
			require.Equal(t, pbinTestConcat([]byte{0xFF}, stem, suffix, []byte{tc.subIndex}), got)
		})
	}
}

func TestPBinStorageZoneKeysAreDistinct(t *testing.T) {
	t.Parallel()

	addr := pbinTestAddr(t, "cafebabe000000000000000000000000deadbeef")
	seen := make(map[string]uint64)
	for _, slot := range []uint64{0, 1, 62, 63, 64, 65, 254, 255, 256, 257, 511, 512, 1000} {
		key := string(pbinTreeKeyStorage(addr, pbinTestSlot(slot)))
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

	addr := pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314")
	addr32 := pbinTestAddress32(addr)
	stem := pbinTestKeccak(t, addr32)

	slot := make([]byte, 32)
	for i := range slot {
		slot[i] = byte(i + 1)
	}
	treeIndex := append([]byte{0x00}, slot[:31]...)
	suffix := pbinTestKeccak(t, addr32, treeIndex)

	got := pbinTreeKeyStorage(addr, slot)
	require.Len(t, got, pbinStorageKeyLength)
	require.Equal(t, pbinTestConcat([]byte{0xFF}, stem, suffix, []byte{slot[31]}), got)
}

// The stem digest covers the 32-byte address, not the 20-byte one.
func TestPBinAddr32Padding(t *testing.T) {
	t.Parallel()

	addr := pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314")
	a32 := pbinAddr32(addr)
	require.Equal(t, make([]byte, 12), a32[:12])
	require.Equal(t, addr, a32[12:])

	key := pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)
	require.Equal(t, pbinTestKeccak(t, pbinTestAddress32(addr)), key[1:33])
	require.NotEqual(t, pbinTestKeccak(t, addr), key[1:33])
}

// The keyHasher contract: the primary leaf's tree key, sized 34 or 66 by zone.
func TestPBinKeyHasherPrimaryLeaf(t *testing.T) {
	t.Parallel()

	hasher := pbinKeyHasher()
	addr := pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314")

	got := hasher(addr)
	require.Len(t, got, pbinAccountKeyLength)
	require.Equal(t, pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), got)

	got = hasher(pbinTestConcat(addr, pbinTestSlot(1000)))
	require.Len(t, got, pbinStorageKeyLength)
	require.Equal(t, pbinTreeKeyStorage(addr, pbinTestSlot(1000)), got)
}

func TestPBinKeyHasherRejectsMalformedPlainKey(t *testing.T) {
	t.Parallel()

	hasher := pbinKeyHasher()
	require.Panics(t, func() { hasher(make([]byte, 33)) })
	require.Panics(t, func() { hasher(nil) })
}

// Two Updates buffers share one hasher value, since Updates.NewEmpty copies it.
// Under -race this fails if the hasher keeps a cache both copies can write.
func TestPBinKeyHasherSharedAcrossBuffers(t *testing.T) {
	t.Parallel()

	addrs := [][]byte{
		pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314"),
		pbinTestAddr(t, "cafebabe000000000000000000000000deadbeef"),
	}
	slots := []uint64{0, 64, 256, 1000}

	base := NewUpdates(ModeDirect, t.TempDir(), pbinKeyHasher())
	clone := base.NewEmpty()

	var wg sync.WaitGroup
	for _, buf := range []*Updates{base, clone} {
		wg.Go(func() {
			for range 50 {
				for _, addr := range addrs {
					assert.Equal(t, pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), buf.hashKey(addr))
					for _, slot := range slots {
						plainKey := pbinTestConcat(addr, pbinTestSlot(slot))
						assert.Equal(t, pbinTreeKeyStorage(addr, pbinTestSlot(slot)), buf.hashKey(plainKey),
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
		pbinTestAddr(t, "0102030405060708090a0b0c0d0e0f1011121314"),
		pbinTestAddr(t, "cafebabe000000000000000000000000deadbeef"),
	}
	slots := []uint64{0, 63, 64, 255, 256, 257, 1000, 100000}

	hasher := pbinKeyHasher()
	for range 3 {
		for _, addr := range addrs {
			require.Equal(t, pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), hasher(addr))
			for _, slot := range slots {
				plainKey := pbinTestConcat(addr, pbinTestSlot(slot))
				require.Equal(t, pbinTreeKeyStorage(addr, pbinTestSlot(slot)), hasher(plainKey),
					"addr %x slot %d", addr, slot)
			}
		}
	}
}
