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
	"github.com/valyala/bytebufferpool"
)

type Cache interface {
	Len() int
	Set(hash common.Hash, v *ByteBuffer)
	Get(hash common.Hash) (*ByteBuffer, bool)
	Clear(codeHash common.Hash, local *ByteBuffer)
}

type DestsCache struct {
	*lru.Cache
}

type ByteBuffer struct {
	*bytebufferpool.ByteBuffer
}

func (b ByteBuffer) Get(pos int) byte {
	return b.B[pos]
}

func (b ByteBuffer) SetBitPos(pos uint64) {
	b.B[pos/8] |= 0x80 >> (pos % 8)
}

func (b ByteBuffer) SetBit8Pos(pos uint64) {
	b.B[pos/8] |= 0xFF >> (pos % 8)
	b.B[pos/8+1] |= ^(0xFF >> (pos % 8))
}

func (b ByteBuffer) CodeSegment(pos uint64) bool {
	return b.B[pos/8]&(0x80>>(pos%8)) == 0
}

type buffPoolT struct {
	p        sync.Pool
	capacity int // drop all buffers more than this threshold to keep constant mem usage by pool
}

func (p *buffPoolT) Put(b *ByteBuffer) {
	if b == nil {
		return
	}
	if b.Len() > p.capacity {
		return
	}
}

func (p *buffPoolT) Get(size uint) *ByteBuffer {
	pp := p.p.Get().(*ByteBuffer)
	if uint(cap(pp.B)) < size {
		pp.B = append(pp.B[:cap(pp.B)], make([]byte, size-uint(cap(pp.B)))...)
	}
	pp.B = pp.B[:size]

	for i := range pp.B {
		pp.B[i] = 0
	}
	return pp
}

var buffPool = buffPoolT{
	capacity: 2048,
	p: sync.Pool{
		New: func() interface{} {
			return &ByteBuffer{ByteBuffer: &bytebufferpool.ByteBuffer{B: make([]byte, 0, 2048)}}
		},
	},
}

func NewDestsCache(maxSize int) *DestsCache {
	c, _ := lru.New(maxSize)
	return &DestsCache{c}
}

func (d *DestsCache) Set(hash common.Hash, v *ByteBuffer) {
	d.Add(hash, v)
}

func (d DestsCache) Get(hash common.Hash) (*ByteBuffer, bool) {
	v, ok := d.Cache.Get(hash)
	if !ok {
		return nil, false
	}
	return v.(*ByteBuffer), ok
}

func (d *DestsCache) Clear(codeHash common.Hash, local *ByteBuffer) {
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

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) *ByteBuffer {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := buffPool.Get(uint(len(code)/8 + 1 + 4))

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
