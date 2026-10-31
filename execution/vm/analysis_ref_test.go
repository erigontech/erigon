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
	"bytes"
	"maps"
	"math/rand"
	"testing"
)

// codeBitmapRef is the straightforward byte-at-a-time reference that all
// codeBitmap implementations must match exactly.
func codeBitmapRef(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	for pc := uint64(0); pc < uint64(len(code)); {
		op := OpCode(code[pc])
		if op == JUMPDEST {
			bits[pc/64] |= 1 << (pc % 64)
		}
		pc++
		if int8(op) < int8(PUSH1) {
			continue
		}
		pc += uint64(op - PUSH1 + 1)
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

func codeBitmapImpls() map[string]func([]byte) bitvec {
	impls := map[string]func([]byte) bitvec{"generic": codeBitmapGeneric}
	maps.Copy(impls, simdImpls())
	return impls
}

// TestCodeBitmapEquivalence fuzzes the codeBitmap implementations against the reference across
// jumpdest-heavy, push-dense and fully-random code, plus boundary edge cases.
func TestCodeBitmapEquivalence(t *testing.T) {
	impls := codeBitmapImpls()
	impls["dispatched"] = codeBitmap
	r := rand.New(rand.NewSource(1))
	gen := func(n, mode int) []byte {
		c := make([]byte, n)
		for i := range c {
			switch mode {
			case 0: // jumpdest-heavy
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
	for iter := range 20000 {
		code := gen(r.Intn(260), iter%3)
		for name, impl := range impls {
			if !equalBitvec(impl(code), codeBitmapRef(code)) {
				t.Fatalf("%s: mismatch (len=%d) code=%x", name, len(code), code)
			}
		}
	}
	edges := [][]byte{{}, {0x5b}, {0x60}, {0x7f}, {0x00}}
	for n := range 48 {
		edges = append(
			edges,
			append([]byte{0x7f}, bytes.Repeat([]byte{0x5b}, n)...),       // PUSH32 + n bytes
			append([]byte{0x5b, 0x7f}, bytes.Repeat([]byte{0x5b}, n)...), // JUMPDEST, PUSH32, ...
			append(bytes.Repeat([]byte{0x5b}, n), 0x7f),                  // trailing PUSH32
		)
	}
	for _, code := range edges {
		for name, impl := range impls {
			if !equalBitvec(impl(code), codeBitmapRef(code)) {
				t.Fatalf("%s: edge mismatch code=%x", name, code)
			}
		}
	}
}
