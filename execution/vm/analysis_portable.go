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

// codeBitmapPortable uses only the portable simd package: lane-wise arithmetic, compares and
// loads at shifted offsets, no shuffles. For every byte of a 64-byte chunk it computes where the
// data of a PUSH there would end, and a running maximum over the 32 bytes before each byte says
// how far PUSH data reaches, assuming every PUSH is real. When no PUSH of the chunk is inside such
// data the assumption holds and the coverage is exact; otherwise a scalar walk takes the chunk.
func codeBitmapPortable(code []byte) bitvec {
	out := make(bitvec, (len(code)+63)/64)
	var z simd.Int8s
	n := z.Len()
	zero := simd.BroadcastInt8s(0)
	one := simd.BroadcastInt8s(1)
	c5f := simd.BroadcastInt8s(0x5f)
	c5b := simd.BroadcastInt8s(0x5b)
	var ends, tmp [96]int8
	var cov, cand, jdb [64]int8
	e, w := 0, 0
	for ; len(code) >= 64; w, code = w+1, code[64:] {
		entry := simd.BroadcastInt8s(int8(e))
		for j := 0; j < 64; j += n {
			c := simd.LoadUint8s(code[j:]).BitsToInt8()
			pos := simd.LoadInt8s(portableIota[j:])
			ln := c.Max(c5f).Sub(c5f)
			valid := ln.Greater(zero).And(pos.GreaterEqual(entry))
			pos.Add(ln).Add(one).IfElse(valid, zero).Store(ends[32+j:])
			valid.ToInt8s().Store(cand[j:])
			c.Equal(c5b).ToInt8s().Store(jdb[j:])
		}
		src, dst := &ends, &tmp
		for k := 1; k < 32; k *= 2 {
			for j := 32; j < 96; j += n {
				simd.LoadInt8s(src[j:]).Max(simd.LoadInt8s(src[j-k:])).Store(dst[j:])
			}
			src, dst = dst, src
		}
		for j := 0; j < 64; j += n {
			simd.LoadInt8s(src[31+j:]).Max(entry).Greater(simd.LoadInt8s(portableIota[j:])).ToInt8s().Store(cov[j:])
		}
		covBits, candBits, jdBits := packBytes(&cov), packBytes(&cand), packBytes(&jdb)
		var next int
		if covBits&candBits == 0 {
			out[w] = jdBits &^ covBits
			next = max(e, int(src[95]))
		} else {
			pc := e
			for ; pc < 64; pc++ {
				if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
					pc += int(op - PUSH1 + 1)
				} else if op == JUMPDEST {
					out[w] |= 1 << uint(pc)
				}
			}
			next = pc
		}
		e = max(next-64, 0)
	}
	for pc := e; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[w] |= 1 << uint(pc)
		}
	}
	return out
}
