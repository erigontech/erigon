// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
)

func TestExpNegativeOneParity(t *testing.T) {
	modulus := new(big.Int).Lsh(big.NewInt(1), 256)
	base := new(uint256.Int).SetAllOne()
	for _, exponent := range []uint256.Int{{}, {1}, {2}, {3}, {0, 1}, {1, 1}, {0, 0, 0, 1}, {1, 0, 0, 1}, *new(uint256.Int).SetAllOne()} {
		scope := &CallContext{}
		scope.Stack.push(exponent)
		scope.Stack.push(*base)
		_, _, err := opExp(0, nil, scope)
		want := new(big.Int).Exp(base.ToBig(), exponent.ToBig(), modulus)
		if err != nil || scope.Stack.peek().ToBig().Cmp(want) != 0 {
			t.Fatalf("exp %x: got %x, %v; want %x", exponent, scope.Stack.peek(), err, want)
		}
	}
}
