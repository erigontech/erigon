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
	"fmt"
	"sync"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/length"
)

// EIP-8297 embedding constants (eip:261-278).
const (
	pbinBasicDataLeafKey    = 0
	pbinCodeHashLeafKey     = 1
	pbinHeaderStorageOffset = 64
	pbinCodeOffset          = 128

	pbinAccountZone = 0x00
	pbinStorageZone = 0xFF

	pbinAccountKeyLength = 34
	pbinStorageKeyLength = 66
)

// pbinAddr32 widens a legacy address to the spec's Address32 by left-padding
// with zero bytes (eip:291-296).
func pbinAddr32(addr []byte) [32]byte {
	if len(addr) > 32 {
		panic(fmt.Sprintf("pbin: address of %d bytes exceeds 32", len(addr)))
	}
	var a32 [32]byte
	copy(a32[32-len(addr):], addr)
	return a32
}

// pbinTreeKey assembles zone || treePosition || subIndex and asserts the length
// fixed for that zone. The assert is load-bearing: one length per zone is what
// keeps keys prefix-free within a zone (eip:283-288).
func pbinTreeKey(zone byte, treePosition []byte, subIndex byte) []byte {
	key := make([]byte, 0, len(treePosition)+2)
	key = append(key, zone)
	key = append(key, treePosition...)
	key = append(key, subIndex)

	want := pbinAccountKeyLength
	if zone == pbinStorageZone {
		want = pbinStorageKeyLength
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

// pbinTreeKeyStorage returns the key for a storage slot, routing slots below 64
// into the account header and the rest into the storage zone (eip:415-437).
// slot is big-endian and at most 32 bytes.
func pbinTreeKeyStorage(addr, slot []byte) []byte {
	var c pbinDigestCache
	return c.storageKey(addr, slot)
}

// pbinKeyHasher returns a keyHasher deriving the primary leaf's tree key:
// BASIC_DATA for an account, the slot's own leaf for storage. The CODE_HASH
// sibling shares the stem and is written by the engine during the same visit,
// so it needs no key of its own here.
//
// The digest cache is borrowed per call rather than captured: Updates.NewEmpty
// copies the hasher value, so a captured cache would be written by two buffers
// hashing concurrently. Every hit is validated against the address it was built
// from, so borrowing another goroutine's cache stays correct.
func pbinKeyHasher() keyHasher {
	var pool sync.Pool
	return func(plainKey []byte) []byte {
		c, _ := pool.Get().(*pbinDigestCache)
		if c == nil {
			c = new(pbinDigestCache)
		}
		key := c.treeKey(plainKey)
		pool.Put(c)
		return key
	}
}

// pbinDigestCache memoizes the two hash-derived key components across a run of
// keys: key_hash(addr32) per address and key_hash(addr32||tree_index) per
// 256-slot storage group. Both digests are immutable, so a hit is always
// correct; changing address invalidates the group entry, which is bound to the
// address as well as the index (eip:411-414).
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
