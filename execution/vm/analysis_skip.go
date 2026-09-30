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
	"math/bits"
	"simd/archsimd"
)

// codeBitmapSkip visits only the real PUSH opcodes. Per 64-byte chunk AVX2 builds a JUMPDEST mask
// and a PUSH-opcode mask; the lowest PUSH bit at or past the current instruction start is a real
// PUSH, since every byte between two instruction starts that is not push data is a 1-byte opcode.
// Its data bits are set and every PUSH bit inside them is cleared, so the next lowest bit is real
// again. The cost follows the number of real PUSHes, not the number of bytes.
func codeBitmapSkip(code []byte) bitvec {
	out := make(bitvec, (len(code)+63)/64)
	c5b := archsimd.BroadcastUint8x32(0x5b)
	c5f := archsimd.BroadcastInt8x32(0x5f)
	start := 0 // offset of the first instruction start in the current chunk, past carried push data
	w := 0
	for ; len(code) >= 64; w, code = w+1, code[64:] {
		lo := archsimd.LoadUint8x32Array((*[32]byte)(code))
		hi := archsimd.LoadUint8x32Array((*[32]byte)(code[32:]))
		jd := uint64(lo.Equal(c5b).ToBits()) | uint64(hi.Equal(c5b).ToBits())<<32
		push := uint64(lo.AsInt8x32().Greater(c5f).ToBits()) | uint64(hi.AsInt8x32().Greater(c5f).ToBits())<<32
		data := uint64(1)<<start - 1
		end := start
		for m := push &^ data; m != 0; m &^= uint64(1)<<end - 1 {
			p := bits.TrailingZeros64(m)
			end = p + 1 + int(code[p]-0x5f)
			data |= (uint64(1)<<end - 1) &^ (uint64(1)<<(p+1) - 1)
		}
		out[w] = jd &^ data
		start = max(end-64, 0)
	}
	for pc := start; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[w] |= 1 << uint(pc)
		}
	}
	return out
}
