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
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/valyala/bytebufferpool"
)

type Cache interface {
	Len() int
	Set(hash common.Hash, v *pool.ByteBuffer)
	Get(hash common.Hash) (*pool.ByteBuffer, bool)
	Clear(codeHash common.Hash, local *pool.ByteBuffer)
}

type DestsCache struct {
	*lru.Cache
}

var buffPool = sync.Pool{
	New: func() interface{} {
		return &pool.ByteBuffer{ByteBuffer: &bytebufferpool.ByteBuffer{B: make([]byte, 0, 1000)}}
	},
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
	buffPool.Put(local)
}

func (d *DestsCache) Len() int {
	return d.Cache.Len()
}

func getBuffer() *pool.ByteBuffer {
	return buffPool.Get().(*pool.ByteBuffer)
}

func getBufferZeroed() *pool.ByteBuffer {
	pp := buffPool.Get().(*pool.ByteBuffer)
	for i := range pp.B {
		pp.B[i] = 0
	}
	return pp
}

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) *pool.ByteBuffer {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := getBufferZeroed()

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
