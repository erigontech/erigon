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

//go:build go1.27 && goexperiment.simd

package vm

import (
	"encoding/binary"
	"math/bits"
	"simd"
	"unsafe"
)

var portableIota, portableIota96 = func() (t [64]int8, t96 [96]int8) {
	for i := range t96 {
		t96[i] = int8(i)
	}
	copy(t[:], t96[:])
	return t, t96
}()

// anyByte reports whether any of the 64 bytes is non-zero.
func anyByte(b *[64]int8) bool {
	raw := (*[64]byte)(unsafe.Pointer(b))
	var v uint64
	for i := 0; i < 64; i += 8 {
		v |= binary.LittleEndian.Uint64(raw[i:])
	}
	return v != 0
}

// packBytes turns 64 bytes of 0x00/0xff into 64 bits.
func packBytes(b *[64]int8) uint64 {
	raw := (*[64]byte)(unsafe.Pointer(b))
	var v uint64
	for i := 0; i < 64; i += 8 {
		v |= (binary.LittleEndian.Uint64(raw[i:]) & 0x8040201008040201 * 0x0101010101010101 >> 56) << i
	}
	return v
}

// codeBitmapPortable uses only the portable simd package: lane-wise arithmetic on loads at shifted
// offsets, no shuffles. A byte y is inside the data of a PUSH d bytes before it when
// int8(code[y-d]) >= 0x5f+d, so y is covered when the maximum over d of code[y-d]-d, saturated,
// reaches 0x5f. Taking every PUSH at or past the entry as real gives the covered bytes; when no
// such PUSH is itself covered the guess is exact, otherwise a scalar walk takes the chunk. The
// first chunk is walked too, so the 32-byte lookback stays in bounds.
func codeBitmapPortable(code []byte) bitvec {
	out := make(bitvec, (len(code)+63)/64)
	var z simd.Int8s
	n := z.Len()
	one := simd.BroadcastInt8s(1)
	c5f := simd.BroadcastInt8s(0x5f)
	c5b := simd.BroadcastInt8s(0x5b)
	var win [96]int8 // lookback and chunk, with the bytes before the entry cleared
	var res, conflict, cand [64]int8
	e := walkChunk(code, 0, 0, out)
	w := 1
	for ; (w+1)*64 <= len(code); w++ {
		i := w * 64
		src := code[i-32 : i+64]
		for j := 0; j < 96; j += n {
			j = min(j, 96-n) // the last load may overlap the one before it
			c := simd.LoadUint8s(src[j:]).BitsToInt8()
			c.IfElse(simd.LoadInt8s(portableIota96[j:]).GreaterEqual(simd.BroadcastInt8s(int8(32+e))), z).Store(win[j:])
		}
		entry := simd.BroadcastInt8s(int8(e))
		for j := 0; j < 64; j += n {
			d, reach := one, simd.BroadcastInt8s(-128)
			for back := 1; back <= 32; back++ {
				reach = reach.Max(simd.LoadInt8s(win[32+j-back:]).SubSaturated(d))
				d = d.Add(one)
			}
			covered := reach.GreaterEqual(c5f).Or(simd.LoadInt8s(portableIota[j:]).Less(entry))
			isPush := simd.LoadInt8s(win[32+j:]).Greater(c5f)
			isPush.And(covered).ToInt8s().Store(conflict[j:])
			isPush.ToInt8s().Store(cand[j:])
			free := reach.Less(c5f).And(simd.LoadInt8s(portableIota[j:]).GreaterEqual(entry))
			simd.LoadUint8s(code[i+j:]).BitsToInt8().Equal(c5b).And(free).ToInt8s().Store(res[j:])
		}
		if anyByte(&conflict) {
			e = walkChunk(code, i, e, out)
			continue
		}
		out[w] = packBytes(&res)
		next := e
		if candBits := packBytes(&cand); candBits != 0 {
			p := 63 - bits.LeadingZeros64(candBits)
			next = max(next, p+1+int(pushLenOf(code[i+p])))
		}
		e = max(next-64, 0)
	}
	for pc := w*64 + e; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return out
}

// walkChunk marks the JUMPDESTs of the 64-byte chunk at i entered at e, or of the code left if
// shorter, and returns the entry of the next chunk.
func walkChunk(code []byte, i, e int, out bitvec) int {
	end := min(i+64, len(code))
	pc := i + e
	for ; pc < end; pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return max(pc-(i+64), 0)
}

func pushLenOf(op byte) uint8 {
	if int8(op) >= int8(PUSH1) {
		return op - byte(PUSH1) + 1
	}
	return 0
}
