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

package vm

import "golang.org/x/sys/cpu"

var hasNEON = cpu.ARM64.HasASIMD

// codeBitmap collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
// It is an accelerated implementation if the CPU supports one, selected at startup.
var codeBitmap = func() func([]byte) bitvec {
	if hasNEON {
		return codeBitmapNEON
	}
	return codeBitmapGeneric
}()

func codeBitmapNEON(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	pc := 0
	if steps := len(code) / 32; steps > 0 {
		var tab [224]byte
		pc = steps*32 + jumpdestBitmapNEON(&code[0], steps, &tab[0], &bits[0])
	}
	return markJumpdestsTail(code, bits, pc)
}

// jumpdestBitmapNEON writes the JUMPDEST bits of the given number (at least 1) of 32-byte steps
// of code to out (4 bytes per step) and returns the offset past the last step at which the next
// instruction starts (0..32). tab is a zeroed 224-byte scratch table.
//
//go:noescape
func jumpdestBitmapNEON(code *byte, steps int, tab *byte, out *uint64) (entry int)
