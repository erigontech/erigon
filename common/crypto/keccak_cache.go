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
	"encoding/binary"
	"hash/maphash"
	"sync/atomic"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

const (
	keccakCacheMaxInput = 87
	keccakCacheWords    = (keccakCacheMaxInput + 7) / 8
)

// keccakBucket is a seqlock: a hit only loads, so workers that hash the same input
// do not bounce the bucket's cache line between cores. seq is odd while a writer
// fills the bucket; a reader that sees seq change discards what it read.
type keccakBucket struct {
	seq  atomic.Uint64
	tag  atomic.Uint64 // input hash with the low byte replaced by the input length
	hash [4]atomic.Uint64
	in   [keccakCacheWords]atomic.Uint64
}

var (
	keccakCacheSeed    = maphash.MakeSeed()
	keccakCacheBuckets = new([1 << 17]keccakBucket)
)

// tailWord returns the last, partial 8 bytes of an input, zero-padded.
func tailWord(rest []byte) uint64 {
	var w [8]byte
	copy(w[:], rest)
	return binary.LittleEndian.Uint64(w[:])
}

func Keccak256Hash(data []byte) common.Hash {
	if len(data) == 0 {
		return empty.CodeHash
	}
	if len(data) > keccakCacheMaxInput {
		return keccak.Sum256(data)
	}
	key := maphash.Bytes(keccakCacheSeed, data)
	b := &keccakCacheBuckets[key&(uint64(len(keccakCacheBuckets))-1)]
	tag := key&^0xff | uint64(len(data))
	words := (len(data) + 7) / 8
	full := len(data) / 8
	if seq := b.seq.Load(); seq&1 == 0 && b.tag.Load() == tag {
		hit := true
		for i := 0; i < full && hit; i++ {
			hit = b.in[i].Load() == binary.LittleEndian.Uint64(data[i*8:])
		}
		if hit && full < words {
			hit = b.in[full].Load() == tailWord(data[full*8:])
		}
		if hit {
			var h common.Hash
			binary.LittleEndian.PutUint64(h[0:], b.hash[0].Load())
			binary.LittleEndian.PutUint64(h[8:], b.hash[1].Load())
			binary.LittleEndian.PutUint64(h[16:], b.hash[2].Load())
			binary.LittleEndian.PutUint64(h[24:], b.hash[3].Load())
			if b.seq.Load() == seq {
				return h
			}
		}
	}
	h := keccak.Sum256(data)
	if seq := b.seq.Load(); seq&1 == 0 && b.seq.CompareAndSwap(seq, seq+1) {
		b.tag.Store(tag)
		for i := range full {
			b.in[i].Store(binary.LittleEndian.Uint64(data[i*8:]))
		}
		if full < words {
			b.in[full].Store(tailWord(data[full*8:]))
		}
		for i := range b.hash {
			b.hash[i].Store(binary.LittleEndian.Uint64(h[i*8:]))
		}
		b.seq.Store(seq + 2)
	}
	return h
}
