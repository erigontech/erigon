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
	"fmt"
	"sync"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// EIP-8297 embedding constants (eip:261-278).
const (
	pbinBasicDataLeafKey    = 0
	pbinCodeHashLeafKey     = 1
	pbinHeaderStorageOffset = 64
	pbinCodeOffset          = 128
	pbinStemSubtreeWidth    = 256

	pbinAccountZone = 0x00
	pbinCodeZone    = 0x01
	pbinStorageZone = 0xFF

	pbinAccountKeyLength = 34
	pbinCodeKeyLength    = 34
	pbinStorageKeyLength = 66
)

// pbinZoneKeyLength gives the single key length a zone admits, which is what
// keeps that zone's keys prefix-free (eip:284-288). Zones 0x02..0xFE are
// unallocated and have no length.
func pbinZoneKeyLength(zone byte) (int, bool) {
	switch zone {
	case pbinAccountZone:
		return pbinAccountKeyLength, true
	case pbinCodeZone:
		return pbinCodeKeyLength, true
	case pbinStorageZone:
		return pbinStorageKeyLength, true
	default:
		return 0, false
	}
}

// pbinAddr32 widens a legacy address to the spec's Address32 (eip:291-296).
func pbinAddr32(addr []byte) [32]byte {
	if len(addr) > 32 {
		panic(fmt.Sprintf("pbin: address of %d bytes exceeds 32", len(addr)))
	}
	var a32 [32]byte
	copy(a32[32-len(addr):], addr)
	return a32
}

// pbinTreeKey assembles zone || treePosition || subIndex. The length assert is
// what enforces the prefix-free invariant (see pbinZoneKeyLength).
func pbinTreeKey(zone byte, treePosition []byte, subIndex byte) []byte {
	key := make([]byte, 0, len(treePosition)+2)
	key = append(key, zone)
	key = append(key, treePosition...)
	key = append(key, subIndex)

	want, known := pbinZoneKeyLength(zone)
	if !known {
		panic(fmt.Sprintf("pbin: zone %#x names no key space", zone))
	}
	if len(key) != want {
		panic(fmt.Sprintf("pbin: zone %#x key of %d bytes, want %d", zone, len(key), want))
	}
	return key
}

// pbinTreeKeyAccount returns the account-header key at subIndex (eip:311-320).
func pbinTreeKeyAccount(addr []byte, subIndex byte) []byte {
	var c pbinDigestCache
	return c.accountKey(addr, subIndex)
}

// pbinTreeKeyStorage returns the key for a storage slot: slots below 64 live in
// the account header, the rest in the storage zone (eip:415-437). slot is
// big-endian and at most 32 bytes.
func pbinTreeKeyStorage(addr, slot []byte) []byte {
	var c pbinDigestCache
	return c.storageKey(addr, slot)
}

// pbinTreeKeyCodeChunk returns the key for a code chunk the account header holds,
// sharing the account's own stem (eip:355-367). Higher chunks go through
// pbinTreeKeyCodeOverflow.
func pbinTreeKeyCodeChunk(addr []byte, chunkID int) []byte {
	var c pbinDigestCache
	return c.codeChunkKey(addr, chunkID)
}

// pbinTreeKeyCodeOverflow returns the code-zone key for a chunk past the account
// header (eip:355-367). These chunks are content-addressed by code hash, so
// accounts running the same bytecode share the leaves and no address can derive
// the key.
func pbinTreeKeyCodeOverflow(codeHash common.Hash, chunkID int) []byte {
	var c pbinDigestCache
	return c.codeOverflowKey(codeHash, chunkID)
}

// pbinKeyHasher returns a keyHasher deriving the primary leaf's tree key:
// BASIC_DATA for an account, the slot's own leaf for storage. The CODE_HASH
// sibling shares the stem and is written by the engine during the same visit, so
// it needs no key of its own here.
func pbinKeyHasher() keyHasher { return pbinKeyHasherWith(nil) }

// pbinKeyHasherWith derives keys under sum, nil meaning Keccak-256. Callers swap
// the hash here and on node hashing together through setHashSuite.
//
// The digest cache is pooled rather than captured because Updates.NewEmpty copies
// the hasher value: a captured cache would be written by two buffers hashing
// concurrently. Every hit is validated against the address it was built from, so
// borrowing another goroutine's cache stays correct.
func pbinKeyHasherWith(sum pbinHashFn) keyHasher {
	var pool sync.Pool
	return func(plainKey []byte) []byte {
		c, _ := pool.Get().(*pbinDigestCache)
		if c == nil {
			c = &pbinDigestCache{sum: sum}
		}
		key := c.treeKey(plainKey)
		pool.Put(c)
		return key
	}
}

// pbinDigestCache memoizes the two hash-derived key components: key_hash(addr32)
// per address and key_hash(addr32||tree_index) per 256-slot storage group
// (eip:411-414). The group entry is bound to the address as well as the index, so
// a changed address cannot yield a stale hit.
type pbinDigestCache struct {
	sum pbinHashFn

	addr32 [32]byte
	stem   [32]byte
	valid  bool

	groupIndex [31]byte
	groupHash  [32]byte
	groupValid bool

	buf [64]byte
}

func (c *pbinDigestCache) hash(preimage []byte) [32]byte {
	if c.sum != nil {
		return c.sum(preimage)
	}
	return keccak.Sum256(preimage)
}

func (c *pbinDigestCache) stemDigest(addr32 *[32]byte) *[32]byte {
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
func (c *pbinDigestCache) groupDigest(addr32, slot32 *[32]byte) *[32]byte {
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

func (c *pbinDigestCache) accountKey(addr []byte, subIndex byte) []byte {
	addr32 := pbinAddr32(addr)
	return pbinTreeKey(pbinAccountZone, c.stemDigest(&addr32)[:], subIndex)
}

// accountHeaderStem and accountStoragePrefix are the two key-space regions an
// account owns, both fixed by its address. Removing an account is removing these
// two subtrees (eip:608-641).
func (c *pbinDigestCache) accountHeaderStem(addr []byte) []byte {
	addr32 := pbinAddr32(addr)
	return append([]byte{pbinAccountZone}, c.stemDigest(&addr32)[:]...)
}

func (c *pbinDigestCache) accountStoragePrefix(addr []byte) []byte {
	addr32 := pbinAddr32(addr)
	return append([]byte{pbinStorageZone}, c.stemDigest(&addr32)[:]...)
}

func (c *pbinDigestCache) codeChunkKey(addr []byte, chunkID int) []byte {
	if chunkID < 0 || chunkID >= pbinHeaderCodeChunks {
		panic(fmt.Sprintf("pbin: code chunk %d lives outside the account header", chunkID))
	}
	return c.accountKey(addr, byte(pbinCodeOffset+chunkID))
}

// codeOverflowKey derives the code-zone key of an overflow chunk. The digest is
// not memoized: one contract spans at most a handful of tree indexes, and the
// cache's entries are bound to an address these keys do not have.
func (c *pbinDigestCache) codeOverflowKey(codeHash common.Hash, chunkID int) []byte {
	if chunkID < pbinHeaderCodeChunks {
		panic(fmt.Sprintf("pbin: code chunk %d is a header chunk, not a code-zone one", chunkID))
	}
	overflow := chunkID - pbinHeaderCodeChunks
	var preimage [2 * length.Hash]byte
	copy(preimage[:], codeHash[:])
	binary.BigEndian.PutUint64(preimage[2*length.Hash-8:], uint64(overflow/pbinStemSubtreeWidth))
	position := c.hash(preimage[:])
	return pbinTreeKey(pbinCodeZone, position[:], byte(overflow%pbinStemSubtreeWidth))
}

func (c *pbinDigestCache) storageKey(addr, slot []byte) []byte {
	addr32 := pbinAddr32(addr)
	slot32 := pbinSlot32(slot)
	if pbinSlotInHeader(&slot32) {
		return pbinTreeKey(pbinAccountZone, c.stemDigest(&addr32)[:], pbinHeaderStorageOffset+slot32[31])
	}
	var position [64]byte
	copy(position[:32], c.stemDigest(&addr32)[:])
	copy(position[32:], c.groupDigest(&addr32, &slot32)[:])
	return pbinTreeKey(pbinStorageZone, position[:], slot32[31])
}

func (c *pbinDigestCache) treeKey(plainKey []byte) []byte {
	switch len(plainKey) {
	case length.Addr:
		return c.accountKey(plainKey, pbinBasicDataLeafKey)
	case length.Addr + length.Hash:
		return c.storageKey(plainKey[:length.Addr], plainKey[length.Addr:])
	default:
		panic(fmt.Sprintf("pbin: plain key of %d bytes is neither an account nor a storage key", len(plainKey)))
	}
}

func pbinSlot32(slot []byte) [32]byte {
	if len(slot) > 32 {
		panic(fmt.Sprintf("pbin: storage slot of %d bytes exceeds 32", len(slot)))
	}
	var s32 [32]byte
	copy(s32[32-len(slot):], slot)
	return s32
}

func pbinSlotInHeader(slot *[32]byte) bool {
	for _, b := range slot[:31] {
		if b != 0 {
			return false
		}
	}
	return slot[31] < pbinCodeOffset-pbinHeaderStorageOffset
}
