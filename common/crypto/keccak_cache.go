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

package crypto

import (
	"bytes"
	"hash/maphash"
	"sync/atomic"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

// keccakCacheMaxInput keeps a bucket at 128 bytes: tag 8 + length 1 + input 87 + hash 32.
const keccakCacheMaxInput = 87

const (
	keccakBucketLocked = 1 << 0
	keccakBucketAlive  = 1 << 1
)

// keccakBucket holds its entry in place, so the table allocates nothing and the GC never scans it.
// tag is both a pre-filter and a lock: readers and writers own the fields only between a
// CompareAndSwap that sets keccakBucketLocked and the Store that clears it.
type keccakBucket struct {
	tag  atomic.Uint64
	n    uint8
	in   [keccakCacheMaxInput]byte
	hash common.Hash
}

var (
	keccakCacheSeed    = maphash.MakeSeed()
	keccakCacheBuckets = new([1 << 17]keccakBucket)
)

// Keccak256Hash calc Keccak256. Short inputs are memoized in a direct-mapped table; a bucket
// another goroutine holds is treated as a miss, so a lookup never waits.
func Keccak256Hash(data []byte) common.Hash {
	if len(data) == 0 {
		return empty.CodeHash
	}
	if len(data) > keccakCacheMaxInput {
		return keccak.Sum256(data)
	}
	key := maphash.Bytes(keccakCacheSeed, data)
	b := &keccakCacheBuckets[key&(uint64(len(keccakCacheBuckets))-1)]
	tag := (key | keccakBucketAlive) &^ keccakBucketLocked
	if st := b.tag.Load(); st == tag && b.tag.CompareAndSwap(st, st|keccakBucketLocked) {
		hit := bytes.Equal(b.in[:b.n], data)
		h := b.hash
		b.tag.Store(st)
		if hit {
			return h
		}
	}
	h := keccak.Sum256(data)
	if st := b.tag.Load(); st&keccakBucketLocked == 0 && b.tag.CompareAndSwap(st, st|keccakBucketLocked) {
		b.n = uint8(len(data))
		copy(b.in[:], data)
		b.hash = h
		b.tag.Store(tag)
	}
	return h
}
