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

// inputWord returns the i-th 8 bytes of data, zero-padded.
func inputWord(data []byte, i int) uint64 {
	rest := data[i*8:]
	if len(rest) >= 8 {
		return binary.LittleEndian.Uint64(rest)
	}
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
	if seq := b.seq.Load(); seq&1 == 0 && b.tag.Load() == tag {
		hit := true
		for i := 0; i < words && hit; i++ {
			hit = b.in[i].Load() == inputWord(data, i)
		}
		var h common.Hash
		for i := range b.hash {
			binary.LittleEndian.PutUint64(h[i*8:], b.hash[i].Load())
		}
		if hit && b.seq.Load() == seq {
			return h
		}
	}
	h := keccak.Sum256(data)
	if seq := b.seq.Load(); seq&1 == 0 && b.seq.CompareAndSwap(seq, seq+1) {
		b.tag.Store(tag)
		for i := range words {
			b.in[i].Store(inputWord(data, i))
		}
		for i := range b.hash {
			b.hash[i].Store(binary.LittleEndian.Uint64(h[i*8:]))
		}
		b.seq.Store(seq + 2)
	}
	return h
}
