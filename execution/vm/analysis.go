// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm

import "unsafe"

// codeBitmap collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
// It is the fastest implementation supported by the CPU, selected at startup.
var codeBitmap = func() func([]byte) bitvec {
	if hasSSE4 {
		return codeBitmapSSE4
	}
	return codeBitmapGeneric
}()

func codeBitmapGeneric(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	for pc := 0; pc < len(code); {
		// Collect the bits of a 64-byte chunk in a register: updating the bitmap in memory
		// for every opcode makes each iteration wait for the previous store.
		i := pc / 64
		end := min(i*64+64, len(code))
		var w uint64
		for pc < end {
			op := OpCode(code[pc])
			if int8(op) < int8(PUSH1) { // not PUSH1..PUSH32, as int8(op) > int8(PUSH32) is always false
				// Avoid a data-dependent branch: it mispredicts on code mixing JUMPDEST and other opcodes.
				var j uint64
				if op == JUMPDEST {
					j = 1
				}
				w |= j << (uint(pc) % 64)
				pc++
				continue
			}
			pc += 1 + int(op-PUSH1+1)
		}
		bits[i] = w
	}
	return bits
}

func codeBitmapSSE4(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	pc := 0
	if blocks := len(code) / 32; blocks > 0 {
		tab := sse4TabInit
		out := (*byte)(unsafe.Pointer(&bits[0]))
		pc = blocks*32 + jumpdestBitmapSSE4(&code[0], blocks, &tab[0], out)
	}
	for ; pc < len(code); pc++ { // the tail of less than 32 bytes
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			bits[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return bits
}

// sse4TabInit holds the constant tails of the jumpdestBitmapSSE4 tables (see analysis_amd64.s): the
// values for entry offsets past the data written per block.
var sse4TabInit = func() (t [384]byte) {
	const ta, pt, t0, t2 = 0, 48, 96, 144
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

// bitvec is a bit vector which maps bytes in a program.
// A set bit means the byte is a valid jump destination.
type bitvec []uint64

// isJumpdest checks if the position is a valid jump destination.
func (bits bitvec) isJumpdest(pos uint64) bool {
	return ((bits[pos/64] >> (pos % 64)) & 1) != 0
}
