// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkExpNegativeOne(b *testing.B) {
	for _, tc := range []struct {
		name           string
		base, exponent uint256.Int
	}{
		{"negative_one/odd", *new(uint256.Int).SetAllOne(), *new(uint256.Int).SetAllOne()},
		{"negative_one/even", *new(uint256.Int).SetAllOne(), uint256.Int{0, 0, 0, 1}},
		{"small_base", uint256.Int{3}, uint256.Int{13}},
		{"other_large_base", uint256.Int{3, 4, 5, 6}, uint256.Int{13}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			scope := &CallContext{}
			b.ReportAllocs()
			for b.Loop() {
				scope.Stack.Reset()
				scope.Stack.push(tc.exponent)
				scope.Stack.push(tc.base)
				if _, _, err := opExp(0, nil, scope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
