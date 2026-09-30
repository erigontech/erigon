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

//go:build go1.27 && goexperiment.simd && amd64

package vm

import (
	"encoding/binary"
	"simd/archsimd"
)

var hasSIMD = archsimd.X86.AVX()

var (
	jdIota = [16]uint8{0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21}
	jdK8   = [16]uint8{8, 8, 8, 8, 8, 8, 8, 8}
	jdLane = [16]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	jdBit  = [16]uint8{1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128}
)

type jdConsts struct {
	c10, c5f, c5b, bit, lane, k8, iota archsimd.Uint8x16
}

// jdHalf is the HALF macro of analysis_amd64.s: for the 16 code bytes in c and every entry
// lane it returns xm (0x80 + entry into the next group), n (the exit of the lane's half) and
// v (the instruction starts visited).
func jdHalf(k *jdConsts, c archsimd.Uint8x16) (xm, n, v archsimd.Uint8x16) {
	x := c.AsInt8x16().Max(k.c5f.AsInt8x16()).Add(k.iota.AsInt8x16())
	n = x.Sub(k.k8.AsInt8x16()).AsUint8x16()
	x = x.Xor(k.k8.AsInt8x16()).Max(k.lane.AsInt8x16())
	u := x.AsUint8x16()
	v = k.bit
	for range 3 {
		v = v.Or(v.PermuteOrZero(x))
		u = u.PermuteOrZero(x)
		x = u.AsInt8x16()
	}
	n = n.PermuteOrZero(x)
	xm = n.PermuteOrZero(n.AsInt8x16()).Max(n)
	return xm, n, v
}

// jumpdestBitmapSIMD is jumpdestBitmapSSE4 written with simd/archsimd.
func jumpdestBitmapSIMD(code []byte, blocks int, bits bitvec) (entry int) {
	k := jdConsts{
		c10:  archsimd.BroadcastUint8x16(0x10),
		c5f:  archsimd.BroadcastUint8x16(0x5f),
		c5b:  archsimd.BroadcastUint8x16(0x5b),
		bit:  archsimd.LoadUint8x16Array(&jdBit),
		lane: archsimd.LoadUint8x16Array(&jdLane),
		k8:   archsimd.LoadUint8x16Array(&jdK8),
		iota: archsimd.LoadUint8x16Array(&jdIota),
	}
	tab := sse4TabInit
	e := 0x80
	for b := range blocks {
		lo := archsimd.LoadUint8x16Array((*[16]byte)(code[32*b:]))
		hi := archsimd.LoadUint8x16Array((*[16]byte)(code[32*b+16:]))
		jd := uint32(lo.Equal(k.c5b).ToBits()) | uint32(hi.Equal(k.c5b).ToBits())<<16
		x1, x2, x3 := jdHalf(&k, lo)
		x6, x7, x8 := jdHalf(&k, hi)

		x1.StoreArray((*[16]byte)(tab[0:]))
		x6.StoreArray((*[16]byte)(tab[64:]))
		idx := x1.Sub(k.c10)
		x6.PermuteOrZero(idx.AsInt8x16()).Max(idx).StoreArray((*[16]byte)(tab[48:]))
		binary.LittleEndian.PutUint64(tab[96:], x2.AsUint64x2().GetElem(0))
		binary.LittleEndian.PutUint64(tab[144:], x7.AsUint64x2().GetElem(0))
		binary.LittleEndian.PutUint64(tab[192:], x3.AsUint64x2().GetElem(0))
		binary.LittleEndian.PutUint64(tab[240:], x8.AsUint64x2().GetElem(0))
		x3.StoreArray((*[16]byte)(tab[288:]))
		x8.StoreArray((*[16]byte)(tab[336:]))

		e1 := int(tab[e-32])
		e2 := int(tab[e-128])
		e3 := int(tab[e2+16])
		starts := uint32(tab[e+64]) | uint32(tab[e1+176])<<8 | uint32(tab[e2+112])<<16 | uint32(tab[e3+224])<<24
		bits[b/2] |= uint64(starts&jd) << (32 * (b % 2))
		e = int(tab[e-80])
	}
	return e - 0x80
}

func codeBitmapSIMD(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	pc := 0
	if blocks := len(code) / 32; blocks > 0 {
		pc = blocks*32 + jumpdestBitmapSIMD(code, blocks, bits)
	}
	for ; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			bits[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return bits
}
