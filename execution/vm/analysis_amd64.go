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

var hasSSE4 = cpu.X86.HasSSSE3 && cpu.X86.HasSSE41

// codeBitmap collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
// It is an accelerated implementation if the CPU supports one, selected at startup.
var codeBitmap = func() func([]byte) bitvec {
	if hasSSE4 {
		return codeBitmapSSE4
	}
	return codeBitmapGeneric
}()

func codeBitmapSSE4(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	pc := 0
	if steps := len(code) / 32; steps > 0 {
		tab := sse4TabInit
		pc = steps*32 + jumpdestBitmapSSE4(&code[0], steps, &tab[0], &bits[0])
	}
	return markJumpdestsTail(code, bits, pc)
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

// jumpdestBitmapSSE4 writes the JUMPDEST bits of the given number (at least 1) of 32-byte steps
// of code to out (4 bytes per step) and returns the offset past the last step at which the next
// instruction starts (0..32). tab is a scratch table initialized from sse4TabInit.
//
//go:noescape
func jumpdestBitmapSSE4(code *byte, steps int, tab *byte, out *uint64) (entry int)
