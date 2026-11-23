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

// codeBitmapGeneric collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
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

// markJumpdestsTail marks the JUMPDESTs of the code tail of less than 32 bytes starting at pc.
func markJumpdestsTail(code []byte, bits bitvec, pc int) bitvec {
	for ; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			bits[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return bits
}

// bitvec is a bit vector which maps bytes in a program.
// A set bit means the byte is a valid jump destination.
type bitvec []uint64

// isJumpdest checks if the position is a valid jump destination.
func (bits bitvec) isJumpdest(pos uint64) bool {
	return ((bits[pos/64] >> (pos % 64)) & 1) != 0
}
