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
	"github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
)

type Cache interface {
	Len() int
	Set(hash common.Hash, v []uint64)
	Get(hash common.Hash) ([]uint64, bool)
}

type DestsCache struct {
	*lru.Cache
}

func NewDestsCache(maxSize int) *DestsCache {
	c, _ := lru.New(maxSize)
	return &DestsCache{c}
}

func (d *DestsCache) Set(hash common.Hash, v []uint64) {
	d.Add(hash, v)
}

func (d DestsCache) Get(hash common.Hash) ([]uint64, bool) {
	if v, ok := d.Cache.Get(hash); ok {
		return v.([]uint64), true
	}
	return nil, false
}

func (d *DestsCache) Len() int {
	return d.Cache.Len()
}

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) []uint64 {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := make([]uint64, (len(code)+32+63)/64)

	for pc := 0; pc < len(code); {
		op := OpCode(code[pc])
		pc++
		if op >= PUSH1 && op <= PUSH32 {
			numbits := int(op - PUSH1 + 1)
			x := uint64(1) << (op - PUSH1)
			x = x | (x - 1) // Smear the bit to the right
			idx := pc / 64
			shift := pc & 63
			bits[idx] |= x << shift
			if shift+shift > 64 {
				bits[idx+1] |= x >> (64 - shift)
			}
			pc += numbits
		}
	}
	return bits
}
