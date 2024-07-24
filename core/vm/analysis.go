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
func codeBitmap(code []byte) []uint64 {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := make([]uint64, (len(code)+32+63)/64)

	for pc := 0; pc < len(code); {
		op := OpCode(code[pc])
		pc++
		if op >= PUSH1 && op <= PUSH32 {
			numbits := int(op - PUSH1 + 1)
			x := uint64(1) << (op - PUSH1)
			x = x | (x - 1) // Smear the bit to the right
			idx := pc / 64
			shift := pc & 63
			bits[idx] |= x << shift
			if shift+shift > 64 {
				bits[idx+1] |= x >> (64 - shift)
			}
			pc += numbits
		}
	}
	return bits
}
