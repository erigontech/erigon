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

import "simd/archsimd"

var (
	scanLane  = [32]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
	scanNot0  = [32]uint8{0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	scanByte  = [32]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3}
	scanBit   = [32]uint8{1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128}
	scanShift = func() (t [4][32]int8) {
		for s, k := range []int{1, 2, 4, 8} {
			for i := range 32 {
				t[s][i] = int8(i%16 - k)
			}
		}
		return t
	}()
	scan15 = [32]int8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}
)

type scanConsts struct {
	lane, not0, byteIdx, bit archsimd.Uint8x32
	shift                    [4]archsimd.Int8x32
	b15                      archsimd.Int8x32
}

// cover returns, for the PUSH opcodes in set, the bytes of the block their data covers (bytes
// before the entry e count as covered) and the per-byte end of that data. ends[y] is where the
// data of the byte before y ends, if that byte is a PUSH, else 0; a running maximum of it from
// the entry says for every byte how far the data of the PUSHes before it reaches.
func (k *scanConsts) cover(ends archsimd.Uint8x32, set uint32, e uint8) (uint32, archsimd.Uint8x32) {
	m := archsimd.BroadcastUint32x8(set << 1).AsUint8x32().PermuteOrZeroGrouped(k.byteIdx.AsInt8x32()).And(k.bit)
	s := ends.And(m.Equal(k.bit).ToInt8x32().AsUint8x32())
	s = s.Max(s.PermuteOrZeroGrouped(k.shift[0]))
	s = s.Max(s.PermuteOrZeroGrouped(k.shift[1]))
	s = s.Max(s.PermuteOrZeroGrouped(k.shift[2]))
	s = s.Max(s.PermuteOrZeroGrouped(k.shift[3]))
	var zero archsimd.Uint8x32
	s = s.Max(zero.SetHi(s.PermuteOrZeroGrouped(k.b15).GetLo())).Max(archsimd.BroadcastUint8x32(e))
	return s.AsInt8x32().Greater(k.lane.AsInt8x32()).ToBits(), s
}

// codeBitmapScan resolves which PUSH opcodes of each 32-byte block are real from how far their
// data reaches. A PUSH that no PUSH before it reaches over is real; a PUSH that a real one
// reaches over is data. Dropping those and repeating resolves the leftmost open PUSH every
// round; real code needs one or two rounds, and a scalar walk finishes any block that needs more.
func codeBitmapScan(code []byte) bitvec {
	if !hasAVX2 {
		return codeBitmapGeneric(code)
	}
	out := make(bitvec, (len(code)+63)/64)
	k := scanConsts{
		lane: archsimd.LoadUint8x32Array(&scanLane), not0: archsimd.LoadUint8x32Array(&scanNot0),
		byteIdx: archsimd.LoadUint8x32Array(&scanByte), bit: archsimd.LoadUint8x32Array(&scanBit),
		b15: archsimd.LoadInt8x32Array(&scan15),
	}
	for s := range k.shift {
		k.shift[s] = archsimd.LoadInt8x32Array(&scanShift[s])
	}
	c5f := archsimd.BroadcastInt8x32(0x5f)
	c5b := archsimd.BroadcastUint8x32(0x5b)
	var first [32]byte
	copy(first[1:], code)
	e := 0
	for i := 0; i+32 <= len(code); i += 32 {
		prev := &first
		if i > 0 {
			prev = (*[32]byte)(code[i-1:])
		}
		cur := archsimd.LoadUint8x32Array((*[32]byte)(code[i:]))
		pl := archsimd.LoadUint8x32Array(prev).AsInt8x32().Max(c5f).AsUint8x32().Sub(c5f.AsUint8x32())
		ends := k.lane.Add(pl).And(k.not0)
		pushes := cur.AsInt8x32().Greater(c5f).ToBits()
		jd := cur.Equal(c5b).ToBits()

		open := pushes &^ uint32(uint64(1)<<e-1)
		var data, sure uint32
		var reach archsimd.Uint8x32
		resolved := false
		if open == 0 { // no PUSH: every byte from the entry on is an instruction start
			data, next := uint32(uint64(1)<<e-1), 0
			out[i/64] |= uint64(jd&^data) << (uint(i) % 64)
			e = max(next-32, 0)
			continue
		}
		for range 3 {
			coverOpen, reachOpen := k.cover(ends, open, uint8(e))
			if coverOpen&open == 0 { // no PUSH reaches over another: all of them are real
				sure, data, reach, resolved = open, coverOpen, reachOpen, true
				break
			}
			sure = open &^ coverOpen
			var coverReal uint32
			coverReal, reach = k.cover(ends, sure, uint8(e))
			open &^= open & coverReal
			if open == sure {
				data, resolved = coverReal, true
				break
			}
		}
		var next int
		if resolved {
			next = int(reach.GetHi().GetElem(15))
			if sure>>31&1 == 1 {
				next = max(next, 32+int(pushLenOf(code[i+31])))
			}
		} else {
			data, next = scanWalk(code[i:i+32], e)
		}
		out[i/64] |= uint64(jd&^data) << (uint(i) % 64)
		e = max(next-32, 0)
	}
	for pc := len(code)/32*32 + e; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return out
}

// scanWalk returns the non-start bytes of a 32-byte block entered at e, and where the last
// instruction's data ends.
func scanWalk(block []byte, e int) (data uint32, next int) {
	data = uint32(uint64(1)<<e - 1)
	pc := e
	for pc < 32 {
		n := int(pushLenOf(block[pc]))
		data |= uint32((uint64(1)<<n - 1) << (pc + 1))
		pc += 1 + n
	}
	return data, pc
}
func pushLenOf(op byte) uint8 {
	if int8(op) >= int8(PUSH1) {
		return op - byte(PUSH1) + 1
	}
	return 0
}
