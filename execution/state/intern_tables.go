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

package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// storageKeyCacheSize must comfortably exceed a contract's live slot count,
// or conflict misses dominate.
const storageKeyCacheSize = 1024

// storageKeyCacheMinOps delays the table until interning has cost more than
// zeroing 40KB: a hit saves ~10ns over the global intern table, so a state resolving
// fewer keys than this over its lifetime cannot win the allocation back however well
// the keys repeat.
const storageKeyCacheMinOps = 128

// slotIndex masks rather than divides, so the size has to be a power of two.
var _ [0]struct{} = [storageKeyCacheSize & (storageKeyCacheSize - 1)]struct{}{}

// storageKeyCache memoizes InternKey by the source stack word. Interning is a
// pure function and a live handle keeps its entry alive, so the cache never
// goes stale and is never cleared. Words are stored beside the handles because
// that is cheaper than recovering them via handle.Value(); the pointer-free
// words go last so the GC scan stops at the handles.
type storageKeyCache struct {
	handles [storageKeyCacheSize]accounts.StorageKey
	words   [storageKeyCacheSize]uint256.Int
}

// slotIndex mixes all four limbs: keccak-derived slots differ across the whole
// word, array and scalar slots only in the lowest.
func slotIndex(word *uint256.Int) uint64 {
	return (word[0] ^ word[1] ^ word[2] ^ word[3]) & (storageKeyCacheSize - 1)
}

// A nil handle marks an unused entry; the zero word is a legitimate key.
func (c *storageKeyCache) fill(i uint64, word *uint256.Int) accounts.StorageKey {
	h := accounts.InternKey(word.Bytes32())
	c.words[i], c.handles[i] = *word, h
	return h
}

// InternStorageKey returns word interned as a StorageKey, skipping the global
// intern table for words seen before. Short-lived states intern uncached: the table only earns
// back its allocation over a few hundred storage ops.
func (ibs *IntraBlockState) InternStorageKey(word *uint256.Int) accounts.StorageKey {
	c := ibs.internCache
	if c == nil {
		if ibs.internOps < storageKeyCacheMinOps {
			ibs.internOps++
			return accounts.InternKey(word.Bytes32())
		}
		c = new(storageKeyCache)
		ibs.internCache = c
		return c.fill(slotIndex(word), word)
	}
	i := slotIndex(word)
	if h := c.handles[i]; h != accounts.NilKey && c.words[i] == *word {
		return h
	}
	return c.fill(i, word)
}

// Address streams are far narrower than storage-key streams — a handful of
// routers and tokens dominate — so the address table is a quarter of the size
// and pays its zeroing back sooner.
const (
	addressCacheSize   = 256
	addressCacheMinOps = 32
)

// AddressCacheSize lets benchmarks outside this package pin the bucket count
// their hit/miss tiers were chosen against.
const AddressCacheSize = addressCacheSize

var _ [0]struct{} = [addressCacheSize & (addressCacheSize - 1)]struct{}{}

// addressCache is storageKeyCache for InternAddress; see that type for why the
// entries never go stale and why the words sit beside the handles. It differs in
// its key: a stack word may hold anything above the low 20 bytes, so index,
// compare and stored word all drop those bits and one entry then serves an
// address whatever the rest of its word holds.
type addressCache struct {
	handles [addressCacheSize]accounts.Address
	words   [addressCacheSize]uint256.Int
}

// addrIndex mixes only the limbs Bytes20 reads: 0, 1 and the low half of 2.
func addrIndex(word *uint256.Int) uint64 {
	return (word[0] ^ word[1] ^ uint64(uint32(word[2]))) & (addressCacheSize - 1)
}

// fill stores the word with everything above the address cleared, which is also
// what lets the probe compare three limbs instead of four.
func (c *addressCache) fill(i uint64, word *uint256.Int) accounts.Address {
	h := accounts.InternAddress(word.Bytes20())
	c.words[i] = uint256.Int{word[0], word[1], uint64(uint32(word[2])), 0}
	c.handles[i] = h
	return h
}

// InternAddress returns the low 20 bytes of word interned as an Address,
// skipping the global intern table for addresses seen before. Short-lived states intern
// uncached: the table only earns back its allocation over a few dozen ops.
func (ibs *IntraBlockState) InternAddress(word *uint256.Int) accounts.Address {
	c := ibs.addrCache
	if c == nil {
		if ibs.addrOps < addressCacheMinOps {
			ibs.addrOps++
			return accounts.InternAddress(word.Bytes20())
		}
		c = new(addressCache)
		ibs.addrCache = c
		return c.fill(addrIndex(word), word)
	}
	i := addrIndex(word)
	if h := c.handles[i]; h != accounts.NilAddress && c.words[i][0] == word[0] &&
		c.words[i][1] == word[1] && c.words[i][2] == uint64(uint32(word[2])) {
		return h
	}
	return c.fill(i, word)
}
