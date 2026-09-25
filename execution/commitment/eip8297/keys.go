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
	"fmt"
	"sync"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// EIP-8297 embedding constants (eip:"Tree embedding").
const (
	BasicDataLeafKey    = 0
	CodeHashLeafKey     = 1
	DelegationLeafKey   = 2
	HeaderStorageOffset = 64
	HeaderStorageSlots  = 64
	StemSubtreeWidth    = 256

	AccountZone = 0x00
	CodeZone    = 0x01
	StorageZone = 0xFF

	AccountKeyLength = 34
	CodeKeyLength    = 34
	StorageKeyLength = 66
)

// ZoneKeyLength gives the single key length a zone admits, which is what
// keeps that zone's keys prefix-free (eip:"Tree embedding"). Zones 0x02..0xFE are
// unallocated and have no length.
func ZoneKeyLength(zone byte) (int, bool) {
	switch zone {
	case AccountZone:
		return AccountKeyLength, true
	case CodeZone:
		return CodeKeyLength, true
	case StorageZone:
		return StorageKeyLength, true
	default:
		return 0, false
	}
}

func LeafSuffixBits(zone byte, recordKeyBits int) (int, error) {
	keyBytes, known := ZoneKeyLength(zone)
	if !known {
		return 0, fmt.Errorf("pbin: zone %#x names no key space", zone)
	}
	zoneBits := keyBytes * 8
	if recordKeyBits < 0 || recordKeyBits >= zoneBits {
		return 0, fmt.Errorf("pbin: record key depth %d is outside zone %#x key length %d", recordKeyBits, zone, zoneBits)
	}
	return zoneBits - recordKeyBits - 1, nil
}

// RightAlign32 widens a legacy address or storage slot to the spec's Address32 (eip:"Tree embedding").
func RightAlign32(b []byte) [32]byte {
	if len(b) > 32 {
		panic(fmt.Sprintf("pbin: key component of %d bytes exceeds 32", len(b)))
	}
	var out [32]byte
	copy(out[32-len(b):], b)
	return out
}

// TreeKey assembles zone || treePosition || subIndex. The length assert is
// what enforces the prefix-free invariant (see ZoneKeyLength).
func TreeKey(zone byte, treePosition []byte, subIndex byte) []byte {
	key := make([]byte, 0, len(treePosition)+2)
	key = append(key, zone)
	key = append(key, treePosition...)
	key = append(key, subIndex)

	want, known := ZoneKeyLength(zone)
	if !known {
		panic(fmt.Sprintf("pbin: zone %#x names no key space", zone))
	}
	if len(key) != want {
		panic(fmt.Sprintf("pbin: zone %#x key of %d bytes, want %d", zone, len(key), want))
	}
	return key
}

// TreeKeyAccount returns the account-header key at subIndex (eip:"Header values").
func TreeKeyAccount(addr []byte, subIndex byte) []byte {
	var c DigestCache
	return c.AccountKey(addr, subIndex)
}

// TreeKeyStorage returns the key for a storage slot: slots below 64 live in
// the account header, the rest in the storage zone (eip:"Storage"). slot is
// big-endian and at most 32 bytes.
func TreeKeyStorage(addr, slot []byte) []byte {
	var c DigestCache
	return c.StorageKey(addr, slot)
}

// TreeKeyCodeChunk returns the code-zone key of a chunk (eip:"Code").
// Chunks are content-addressed by code hash, so accounts running the same
// bytecode share the leaves and no address takes part in the derivation.
func TreeKeyCodeChunk(codeHash common.Hash, chunkID int) []byte {
	var c DigestCache
	return c.CodeChunkKey(codeHash, chunkID)
}

// KeyHasher returns a keyHasher deriving the primary leaf's tree key:
// BASIC_DATA for an account, the slot's own leaf for storage. The CODE_HASH
// sibling shares the stem and is written by the engine during the same visit, so
// it needs no key of its own here.
type KeyHasherFunc func(key []byte) []byte

func KeyHasher() KeyHasherFunc { return KeyHasherWith(nil) }

// KeyHasherWith derives keys under sum, nil meaning Keccak-256. Callers swap
// the hash here and on node hashing together through setHashSuite.
//
// The digest cache is pooled rather than captured because Updates.NewEmpty copies
// the hasher value: a captured cache would be written by two buffers hashing
// concurrently. Every hit is validated against the address it was built from, so
// borrowing another goroutine's cache stays correct.
func KeyHasherWith(sum HashFn) KeyHasherFunc {
	var pool sync.Pool
	return func(plainKey []byte) []byte {
		c, _ := pool.Get().(*DigestCache)
		if c == nil {
			c = &DigestCache{Sum: sum}
		}
		key := c.TreeKey(plainKey)
		pool.Put(c)
		return key
	}
}

// DigestCache memoizes the two hash-derived key components: key_hash(addr32)
// per address and key_hash(addr32||tree_index) per 256-slot storage group
// (eip:"Storage"). The group entry is bound to the address as well as the index, so
// a changed address cannot yield a stale hit.
type DigestCache struct {
	Sum HashFn

	addr32 [32]byte
	stem   [32]byte
	valid  bool

	groupIndex [31]byte
	groupHash  [32]byte
	groupValid bool

	buf [64]byte
}

func (c *DigestCache) hash(preimage []byte) [32]byte {
	if c.Sum != nil {
		return c.Sum(preimage)
	}
	return keccak.Sum256(preimage)
}

func (c *DigestCache) stemDigest(addr32 *[32]byte) *[32]byte {
	if c.valid && c.addr32 == *addr32 {
		return &c.stem
	}
	c.stem = c.hash(addr32[:])
	c.addr32 = *addr32
	c.valid = true
	c.groupValid = false
	return &c.stem
}

// groupDigest hashes addr32 || tree_index, where tree_index is slot>>8 as a
// 32-byte big-endian value: a zero byte followed by the slot's top 31 bytes.
func (c *DigestCache) groupDigest(addr32, slot32 *[32]byte) *[32]byte {
	idx := (*[31]byte)(slot32[:31])
	if c.groupValid && c.addr32 == *addr32 && c.groupIndex == *idx {
		return &c.groupHash
	}
	copy(c.buf[:32], addr32[:])
	c.buf[32] = 0
	copy(c.buf[33:], idx[:])
	c.groupHash = c.hash(c.buf[:])
	c.groupIndex = *idx
	c.groupValid = true
	return &c.groupHash
}

func (c *DigestCache) AccountKey(addr []byte, subIndex byte) []byte {
	addr32 := RightAlign32(addr)
	return TreeKey(AccountZone, c.stemDigest(&addr32)[:], subIndex)
}

// accountHeaderStem and accountStoragePrefix are the two key-space regions an
// account owns, both fixed by its address. Removing an account is removing these
// two subtrees (eip:"Zero values and deletion").
func (c *DigestCache) AccountHeaderStem(addr []byte) []byte {
	addr32 := RightAlign32(addr)
	return append([]byte{AccountZone}, c.stemDigest(&addr32)[:]...)
}

func (c *DigestCache) AccountStoragePrefix(addr []byte) []byte {
	addr32 := RightAlign32(addr)
	return append([]byte{StorageZone}, c.stemDigest(&addr32)[:]...)
}

// codeChunkKey derives the code-zone key of a chunk. The digest is not
// memoized: one contract spans at most a handful of tree indexes, and the
// cache's entries are bound to an address these keys do not have.
func (c *DigestCache) CodeChunkKey(codeHash common.Hash, chunkID int) []byte {
	if chunkID < 0 {
		panic(fmt.Sprintf("pbin: code chunk %d is negative", chunkID))
	}
	var preimage [2 * length.Hash]byte
	copy(preimage[:], codeHash[:])
	binary.BigEndian.PutUint64(preimage[2*length.Hash-8:], uint64(chunkID/StemSubtreeWidth))
	position := c.hash(preimage[:])
	return TreeKey(CodeZone, position[:], byte(chunkID%StemSubtreeWidth))
}

func (c *DigestCache) StorageKey(addr, slot []byte) []byte {
	addr32 := RightAlign32(addr)
	slot32 := RightAlign32(slot)
	if SlotInHeader(&slot32) {
		return TreeKey(AccountZone, c.stemDigest(&addr32)[:], HeaderStorageOffset+slot32[31])
	}
	var position [64]byte
	copy(position[:32], c.stemDigest(&addr32)[:])
	copy(position[32:], c.groupDigest(&addr32, &slot32)[:])
	return TreeKey(StorageZone, position[:], slot32[31])
}

func (c *DigestCache) TreeKey(plainKey []byte) []byte {
	switch len(plainKey) {
	case length.Addr:
		return c.AccountKey(plainKey, BasicDataLeafKey)
	case length.Addr + length.Hash:
		return c.StorageKey(plainKey[:length.Addr], plainKey[length.Addr:])
	default:
		panic(fmt.Sprintf("pbin: plain key of %d bytes is neither an account nor a storage key", len(plainKey)))
	}
}

func SlotInHeader(slot *[32]byte) bool {
	return [31]byte(slot[:31]) == [31]byte{} && slot[31] < HeaderStorageSlots
}
