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
	"bytes"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestJumpDestAnalysis(t *testing.T) {
	t.Parallel()
	const J, P1, P2, P32 = byte(JUMPDEST), byte(PUSH1), byte(PUSH2), byte(PUSH32)
	tests := []struct {
		code  []byte
		exp   uint64
		which int
	}{
		{[]byte{J, J, J}, 0b111, 0},
		{[]byte{P1, J, J}, 0b100, 0},
		{[]byte{P2, J, J, J}, 0b1000, 0},
		{[]byte{P1, P1, J, P1, J}, 0b00100, 0},
		{[]byte{P1, P1, P1, J}, 0b0000, 0},
		{append([]byte{P32}, append(make([]byte, 31), J, J)...), 0b1 << 33, 0},
		{append(make([]byte, 62), P2, J, J, J), 0b10, 1},
		{append(make([]byte, 64), J), 0b1, 1},
		{append(bytes.Repeat([]byte{J}, 61), P2, J, J), 1<<61 - 1, 0},
		{append(append(bytes.Repeat([]byte{J}, 20), P32), bytes.Repeat([]byte{J}, 44)...), 0xffe0_0000_000f_ffff, 0},
	}
	for _, test := range tests {
		ret := codeBitmap(test.code)
		if ret[test.which] != test.exp {
			t.Fatalf("code %x: expected %x, got %x", test.code, test.exp, ret[test.which])
		}
	}
}

func TestValidJumpdest(t *testing.T) {
	t.Parallel()
	contract := NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH1), byte(JUMPDEST), byte(JUMPDEST), byte(ADD)}
	for dest, want := range []bool{false, false, true, false, false} {
		if got := contract.validJumpdest(uint256.NewInt(uint64(dest))); got != want {
			t.Errorf("dest %d: expected %v, got %v", dest, want, got)
		}
	}
	overflow := new(uint256.Int).Lsh(uint256.NewInt(1), 64)
	if contract.validJumpdest(overflow.AddUint64(overflow, 2)) {
		t.Error("dest 2^64+2: expected false")
	}
}
