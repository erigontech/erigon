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

import (
	"math/rand"
	"testing"
)

// codeBitmapRef is the straightforward byte-at-a-time reference that the SWAR
// codeBitmap must match exactly.
func codeBitmapRef(code []byte) bitvec {
	bits := make(bitvec, (len(code)+32+63)/64)
	for pc := uint64(0); pc < uint64(len(code)); {
		op := OpCode(code[pc])
		pc++
		if int8(op) < int8(PUSH1) {
			continue
		}
		if op == PUSH1 {
			bits.set1(pc)
			pc++
			continue
		}
		numbits := uint64(op - PUSH1 + 1)
		bits.setN(uint64(1)<<numbits-1, pc)
		pc += numbits
	}
	return bits
}

func equalBitvec(a, b bitvec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCodeBitmapSWAREquivalence fuzzes codeBitmap against the reference across
// jumpdest-heavy, push-dense and fully-random code, plus boundary edge cases.
func TestCodeBitmapSWAREquivalence(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	gen := func(n, mode int) []byte {
		c := make([]byte, n)
		for i := range c {
			switch mode {
			case 0: // jumpdest-heavy (the unique-code adversarial shape)
				if r.Intn(20) == 0 {
					c[i] = byte(0x60 + r.Intn(32))
				} else {
					c[i] = 0x5b
				}
			case 1: // push-dense
				if r.Intn(2) == 0 {
					c[i] = byte(0x60 + r.Intn(32))
				} else {
					c[i] = byte(r.Intn(256))
				}
			default: // uniform random
				c[i] = byte(r.Intn(256))
			}
		}
		return c
	}
	for iter := 0; iter < 20000; iter++ {
		code := gen(r.Intn(260), iter%3)
		if !equalBitvec(codeBitmap(code), codeBitmapRef(code)) {
			t.Fatalf("mismatch (len=%d) code=%x", len(code), code)
		}
	}
	edges := [][]byte{{}, {0x5b}, {0x60}, {0x7f}, {0x00}}
	for n := 0; n < 48; n++ {
		edges = append(edges,
			append([]byte{0x7f}, make([]byte, n)...),       // PUSH32 + n bytes
			append([]byte{0x5b, 0x7f}, make([]byte, n)...), // JUMPDEST, PUSH32, ...
			append(make([]byte, n), 0x7f),                  // trailing PUSH32
		)
	}
	for _, code := range edges {
		if !equalBitvec(codeBitmap(code), codeBitmapRef(code)) {
			t.Fatalf("edge mismatch code=%x", code)
		}
	}
}

// BenchmarkJumpdestAnalysisJumpdest24k mirrors the EIP-2780
// unique_code_jumpdest receiver: 24KiB of JUMPDEST with no PUSH data.
func BenchmarkJumpdestAnalysisJumpdest24k(b *testing.B) {
	code := make([]byte, 24576)
	for i := range code {
		code[i] = 0x5b
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codeBitmap(code)
	}
}
