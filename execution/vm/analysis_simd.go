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

// codeBitmap collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
func codeBitmap(code []byte) bitvec {
	if !hasAVX2 {
		return codeBitmapGeneric(code)
	}
	bits := make(bitvec, (len(code)+63)/64)
	pc := len(code) / 32 * 32
	pc += jumpdestBitmapSIMD(code[:pc], bits)
	for ; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			bits[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return bits
}

var hasAVX2 = archsimd.X86.AVX2()

var (
	jdIota = [32]uint8{0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21,
		0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21}
	jdK8   = [32]uint8{8, 8, 8, 8, 8, 8, 8, 8, 16: 8, 8, 8, 8, 8, 8, 8, 8}
	jdLane = [32]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	jdBit  = [32]uint8{1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128}
)

// jdTabInit holds the constant tails of the jumpdestBitmapSIMD tables: the values for entry
// offsets past the data written per block. Entries are stored as 0x80 + offset. The tables start
// at jdTabBase, so every lookup is a byte plus a non-negative constant below len(tab), which needs
// no bounds check. Layout relative to jdTabBase:
// TA 0 and PT 48 map a block entry to the entry into bytes 16..31 and into the next block;
// T0 96 and T2 144 map an entry into bytes 0..7 / 16..23 to one into 8..15 / 24..31;
// V0 192, V1 296, V2 240, V3 344 hold the instruction-start bits of each 8-byte half by entry.
var jdTabInit = func() (t [640]byte) {
	const ta, pt, t0, t2 = jdTabBase, jdTabBase + 48, jdTabBase + 96, jdTabBase + 144
	for i := 8; i < 40; i++ {
		t[t0+i] = byte(0x78 + i - 8)
		t[t2+i] = byte(0x78 + i - 8)
	}
	for i := 16; i < 40; i++ {
		t[ta+i] = byte(0x80 + i - 16)
	}
	for i := 32; i < 40; i++ {
		t[pt+i] = byte(0x80 + i - 32)
	}
	return t
}()

const jdTabBase = 128

// jumpdestBitmapSIMD analyzes whole 32-byte blocks branch-free and returns the offset past the
// last block at which the next instruction starts. For each 8-byte half and every entry offset it
// resolves next = entry + 1 + pushlen by pointer jumping with byte shuffles, collecting the
// instruction starts visited; small per-block tables indexed by entry then let a scalar chain
// follow the real entry with one dependent load per block. Both 16-byte groups of a block share
// one 256-bit pass, one group per 128-bit lane.
func jumpdestBitmapSIMD(code []byte, bits bitvec) (entry int) {
	c10 := archsimd.BroadcastUint8x16(0x10)
	c5b := archsimd.BroadcastUint8x32(0x5b)
	c5f := archsimd.BroadcastInt8x32(0x5f)
	iota := archsimd.LoadUint8x32Array(&jdIota).AsInt8x32()
	k8 := archsimd.LoadUint8x32Array(&jdK8).AsInt8x32()
	lane := archsimd.LoadUint8x32Array(&jdLane).AsInt8x32()
	bit := archsimd.LoadUint8x32Array(&jdBit)

	tab := jdTabInit
	e := uint8(0x80)
	var acc uint64
	b := uint(0)
	for ; len(code) >= 32; b, code = b+1, code[32:] {
		c := archsimd.LoadUint8x32Array((*[32]byte)(code))
		jd := c.Equal(c5b).ToBits()

		x := c.AsInt8x32().Max(c5f).Add(iota)
		n := x.Sub(k8).AsUint8x32()
		x = x.Xor(k8).Max(lane)
		u := x.AsUint8x32()
		v := bit.Or(bit.PermuteOrZeroGrouped(x))
		x = u.PermuteOrZeroGrouped(x).AsInt8x32()
		v = v.Or(v.PermuteOrZeroGrouped(x))
		x = x.AsUint8x32().PermuteOrZeroGrouped(x).AsInt8x32()
		v = v.Or(v.PermuteOrZeroGrouped(x))
		x = x.AsUint8x32().PermuteOrZeroGrouped(x).AsInt8x32()
		n = n.PermuteOrZeroGrouped(x)
		xm := n.PermuteOrZeroGrouped(n.AsInt8x32()).Max(n)

		x1 := xm.GetLo()
		x6 := xm.GetHi()
		x6.StoreArray((*[16]byte)(tab[jdTabBase+64:]))
		idx := x1.Sub(c10)
		x6.PermuteOrZero(idx.AsInt8x16()).Max(idx).StoreArray((*[16]byte)(tab[jdTabBase+48:]))
		var starts uint32
		if jd != 0 { // without a JUMPDEST only the entry into the next block is needed
			x1.StoreArray((*[16]byte)(tab[jdTabBase:]))
			nq, vq := n.AsUint64x4(), v.AsUint64x4()
			binary.LittleEndian.PutUint64(tab[jdTabBase+96:], nq.GetLo().GetElem(0))
			binary.LittleEndian.PutUint64(tab[jdTabBase+144:], nq.GetHi().GetElem(0))
			binary.LittleEndian.PutUint64(tab[jdTabBase+192:], vq.GetLo().GetElem(0))
			binary.LittleEndian.PutUint64(tab[jdTabBase+240:], vq.GetHi().GetElem(0))
			v.GetLo().StoreArray((*[16]byte)(tab[jdTabBase+288:]))
			v.GetHi().StoreArray((*[16]byte)(tab[jdTabBase+336:]))

			e1 := tab[int(e)+jdTabBase-32]
			e2 := tab[int(e)+jdTabBase-128]
			e3 := tab[int(e2)+jdTabBase+16]
			starts = uint32(tab[int(e)+jdTabBase+64]) | uint32(tab[int(e1)+jdTabBase+176])<<8 |
				uint32(tab[int(e2)+jdTabBase+112])<<16 | uint32(tab[int(e3)+jdTabBase+224])<<24
		}
		if b&1 == 0 {
			acc = uint64(starts & jd)
		} else {
			bits[b>>1] = acc | uint64(starts&jd)<<32
		}
		e = tab[int(e)+jdTabBase-80]
	}
	if b&1 == 1 {
		bits[b>>1] = acc
	}
	return int(e) - 0x80
}
