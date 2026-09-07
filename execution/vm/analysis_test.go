// Copyright 2017 The go-ethereum Authors
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

import (
	"testing"
)

func TestJumpDestAnalysis(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code  []byte
		exp   uint64
		which int
	}{
		{[]byte{byte(PUSH1), 0x01, 0x01, 0x01}, 0x02, 0},
		{[]byte{byte(PUSH1), byte(PUSH1), byte(PUSH1), byte(PUSH1)}, 0x0a, 0},
		{[]byte{byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), 0x01, 0x01, 0x01}, 0x01fe, 0},
		{[]byte{byte(PUSH8), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x01fe, 0},
		{[]byte{0x01, 0x01, 0x01, 0x01, 0x01, byte(PUSH2), byte(PUSH2), byte(PUSH2), 0x01, 0x01, 0x01}, 0xc0, 0},
		{[]byte{0x01, 0x01, 0x01, 0x01, 0x01, byte(PUSH2), 0x01, 0x01, 0x01, 0x01, 0x01}, 0xc0, 0},
		{[]byte{byte(PUSH3), 0x01, 0x01, 0x01, byte(PUSH1), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x2e, 0},
		{[]byte{0x01, byte(PUSH8), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x03fc, 0},
		{[]byte{byte(PUSH16), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x01fffe, 0},
		{[]byte{byte(PUSH8), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, byte(PUSH1), 0x01}, 0x05fe, 0},
		{[]byte{byte(PUSH32)}, 0x01fffffffe, 0},
		{[]byte{byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5)}, 0b1110111110111110111110111110111110111110111110111110111110111110, 0},
		{[]byte{byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5), byte(PUSH5)}, 0b11111011111011111011111011, 1},
	}
	for _, test := range tests {
		ret := codeBitmap(test.code)
		if ret[test.which] != test.exp {
			t.Fatalf("expected %x, got %02x", test.exp, ret[test.which])
		}
	}
}
