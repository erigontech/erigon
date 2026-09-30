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

var portableIota = func() (t [64]int8) {
	for i := range t {
		t[i] = int8(i)
	}
	return t
}()

// packBytes turns 64 bytes of 0x00/0xff into 64 bits.
func packBytes(b *[64]int8) uint64 {
	raw := (*[64]byte)(unsafe.Pointer(b))
	var v uint64
	for i := 0; i < 64; i += 8 {
		v |= (binary.LittleEndian.Uint64(raw[i:]) & 0x8040201008040201 * 0x0101010101010101 >> 56) << i
	}
	return v
}

// codeBitmapPortable uses only the portable simd package: lane-wise compares on loads of the
// code at shifted offsets, no shuffles. A byte y is inside the data of a PUSH d bytes before it
// when int8(code[y-d]) >= 0x5f+d. Taking every PUSH at or past the entry as real gives the covered
// bytes in 32 compares; when no such PUSH is itself covered the guess is exact, otherwise a scalar
// walk takes the chunk. The first chunk is walked too, so the 32-byte lookback stays in bounds.
func codeBitmapPortable(code []byte) bitvec {
	out := make(bitvec, (len(code)+63)/64)
	var z simd.Int8s
	n := z.Len()
	c5f := simd.BroadcastInt8s(0x5f)
	c5b := simd.BroadcastInt8s(0x5b)
	var cov, cand, jdb [64]int8
	e := walkChunk(code, 0, 0, out)
	w := 1
	for ; (w+1)*64 <= len(code); w++ {
		i := w * 64
		for j := 0; j < 64; j += n {
			pos := simd.LoadInt8s(portableIota[j:])
			c := simd.LoadUint8s(code[i+j:]).BitsToInt8()
			covered := c.Less(c) // all false
			for d := 1; d <= 32; d++ {
				pushed := simd.LoadUint8s(code[i+j-d:]).BitsToInt8().GreaterEqual(simd.BroadcastInt8s(int8(0x5f + d)))
				covered = covered.Or(pushed.And(pos.GreaterEqual(simd.BroadcastInt8s(int8(e + d)))))
			}
			covered.ToInt8s().Store(cov[j:])
			c.Greater(c5f).ToInt8s().Store(cand[j:])
			c.Equal(c5b).ToInt8s().Store(jdb[j:])
		}
		covBits, jdBits := packBytes(&cov), packBytes(&jdb)
		candBits := packBytes(&cand) &^ (uint64(1)<<e - 1)
		if covBits&candBits != 0 {
			e = walkChunk(code, i, e, out)
			continue
		}
		out[w] = jdBits &^ (covBits | uint64(1)<<e - 1)
		next := e
		if candBits != 0 {
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
