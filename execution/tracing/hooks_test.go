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

package tracing

import "testing"

// TestOpcodeMask pins the two things the interpreter relies on: a nil mask delivers
// every opcode, and a set mask delivers exactly the named ones. The boundaries matter
// because the mask indexes a word per 64 opcodes.
func TestOpcodeMask(t *testing.T) {
	var nilMask *OpcodeMask
	for op := range 256 {
		if !nilMask.wants(byte(op)) {
			t.Fatalf("nil mask must want opcode %#x", op)
		}
	}

	want := map[byte]bool{0x00: true, 0x3f: true, 0x40: true, 0x54: true, 0x55: true, 0xf1: true, 0xff: true}
	keys := make([]byte, 0, len(want))
	for op := range want {
		keys = append(keys, op)
	}
	m := NewOpcodeMask(keys...)
	for op := range 256 {
		if got := m.wants(byte(op)); got != want[byte(op)] {
			t.Fatalf("opcode %#x: wants=%v, want %v", op, got, want[byte(op)])
		}
	}
}
