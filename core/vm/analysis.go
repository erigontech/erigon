// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"fmt"
	"sort"

	"github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
)

func NewJumpDestsDefault() *JumpDests {
	return NewJumpDests(50000, 10, 5)
}

func NewJumpDestsTest() *JumpDests {
	return NewJumpDests(20, 10, 5)
}

type JumpDests struct {
	maps       map[common.Hash]*item
	lru        []map[common.Hash]struct{}
	chunks     []int
	minToClear int // 1..100
	maxSize    int

	GcCount int
	GcTotal int
}

type item struct {
	m    *pool.ByteBuffer
	used int
}

type Cache interface {
	Len() int
	Set(hash common.Hash, v *pool.ByteBuffer)
	Get(hash common.Hash) (*pool.ByteBuffer, bool)
	Clear(codeHash common.Hash, local *pool.ByteBuffer)
}

type DestsCache struct {
	*lru.Cache
}

func NewDestsCache(maxSize int) *DestsCache {
	c, _ := lru.New(maxSize)
	return &DestsCache{c}
}

func (d *DestsCache) Set(hash common.Hash, v *pool.ByteBuffer) {
	d.Add(hash, v)
}

func (d DestsCache) Get(hash common.Hash) (*pool.ByteBuffer, bool) {
	v, ok := d.Cache.Get(hash)
	if !ok {
		return nil, false
	}
	return v.(*pool.ByteBuffer), ok
}

func (d *DestsCache) Clear(codeHash common.Hash, local *pool.ByteBuffer) {
	if codeHash == (common.Hash{}) {
		return
	}
	_, ok := d.Get(codeHash)
	if ok {
		return
	}
	// analysis is a local one
	pool.PutBuffer(local)
}

func (d *DestsCache) Len() int {
	return d.Cache.Len()
}

func NewJumpDests(maxSize, nChunks, percentToClear int) *JumpDests {
	lru := make([]map[common.Hash]struct{}, nChunks)
	chunks := make([]int, nChunks)
	chunkSize := maxSize / nChunks
	for i := 0; i < nChunks; i++ {
		lru[i] = make(map[common.Hash]struct{}, chunkSize)
		chunks[i] = 1 << (1 + i*2)
	}

	return &JumpDests{
		make(map[common.Hash]*item, maxSize),
		lru,
		chunks,
		maxSize * percentToClear / 100,
		maxSize,
		0,
		0,
	}
}

func (j *JumpDests) Len() int {
	return len(j.maps)
}

func (j *JumpDests) Set(hash common.Hash, v *pool.ByteBuffer) {
	_, ok := j.maps[hash]
	if ok {
		return
	}

	if len(j.maps) >= j.maxSize {
		j.GcCount++
		j.gc()
	}

	j.maps[hash] = &item{v, 1}
	j.lru[0][hash] = struct{}{}
}

func (j *JumpDests) Get(hash common.Hash) (*pool.ByteBuffer, bool) {
	jumps, ok := j.maps[hash]
	if !ok {
		return nil, false
	}

	jumps.used++
	idx := sort.SearchInts(j.chunks, jumps.used)

	// everything greater than j.chunks[len(chunks)-1] should be stored in the last chunk
	if idx >= 0 && idx < len(j.chunks)-1 {
		max := j.chunks[idx]
		if jumps.used >= max {
			// moving to the next chunk
			j.lru[idx+1][hash] = struct{}{}
			delete(j.lru[idx], hash)
		}
	}

	return jumps.m, true
}

func (j *JumpDests) gc() {
	n := 0
	for _, chunk := range j.lru {
		for hash := range chunk {
			delete(chunk, hash)
			delete(j.maps, hash)

			n++
			if n >= j.minToClear {
				j.GcTotal += n
				return
			}
		}
	}
	j.GcTotal += n
}

func (j *JumpDests) Clear(codeHash common.Hash, local *pool.ByteBuffer) {
	if codeHash == (common.Hash{}) {
		return
	}
	_, ok := j.maps[codeHash]
	if ok {
		return
	}
	// analysis is a local one
	pool.PutBuffer(local)
}

func (j *JumpDests) String() string {
	res := ""
	for i, size := range j.chunks {
		res += fmt.Sprintf("chunk %d:%d; ", size, len(j.lru[i]))
	}

	return fmt.Sprintf("cached %d, cacheGC %d, cacheGCCleaned %d, buckets: %s",
		j.Len(), j.GcCount, j.GcTotal, res)
}

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) *pool.ByteBuffer {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := pool.GetBufferZeroed(uint(len(code)/8 + 1 + 4))

	for pc := uint64(0); pc < uint64(len(code)); {
		op := OpCode(code[pc])

		if op >= PUSH1 && op <= PUSH32 {
			numbits := op - PUSH1 + 1
			pc++
			for ; numbits >= 8; numbits -= 8 {
				bits.SetBit8Pos(pc) // 8
				pc += 8
			}
			for ; numbits > 0; numbits-- {
				bits.SetBitPos(pc)
				pc++
			}
		} else {
			pc++
		}
	}
	return bits
}
