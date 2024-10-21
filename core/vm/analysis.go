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

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) bitvec {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := make(bitvec, (len(code)+32+63)/64)
	for pc := uint64(0); pc < uint64(len(code)); {
		op := OpCode(code[pc])
		pc++
		if int8(op) < int8(PUSH1) { // If not PUSH (the int8(op) > int(PUSH32) is always false).
			continue
		}
		if op == PUSH1 {
			bits.set1(pc)
			pc += 1
			continue
		}

		numbits := uint64(op - PUSH1 + 1)
		bits.setN(uint64(1)<<numbits-1, pc)
		pc += numbits
	}
	return bits
}

// bitvec is a bit vector which maps bytes in a program.
// An unset bit means the byte is an opcode, a set bit means
// it's data (i.e. argument of PUSHxx).
type bitvec []uint64

func (bits bitvec) set1(pos uint64) {
	bits[pos/64] |= 1 << (pos % 64)
}

func (bits bitvec) setN(flag uint64, pc uint64) {
	shift := pc % 64
	bits[pc/64] |= flag << shift
	if shift > 32 {
		bits[pc/64+1] = flag >> (64 - shift)
	}
}

// codeSegment checks if the position is in a code segment.
func (bits bitvec) codeSegment(pos uint64) bool {
	return ((bits[pos/64] >> (pos % 64)) & 1) == 0
}
