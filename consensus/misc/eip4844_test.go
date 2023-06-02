// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package misc

import (
	"testing"

	"github.com/holiman/uint256"
)

func TestFakeExponential(t *testing.T) {
	var tests = []struct {
		factor, num, denom uint64
		want               uint64
	}{
		// When num==0 the return value should always equal the value of factor
		{1, 0, 1, 1},
		{38493, 0, 1000, 38493},
		{0, 1234, 2345, 0}, // should be 0
		{1, 2, 1, 6},       // approximate 7.389
		{1, 4, 2, 6},
		{1, 3, 1, 16}, // approximate 20.09
		{1, 6, 2, 18},
		{1, 4, 1, 49}, // approximate 54.60
		{1, 8, 2, 50},
		{10, 8, 2, 542}, // approximate 540.598
		{11, 8, 2, 596}, // approximate 600.58
		{1, 5, 1, 136},  // approximate 148.4
		{1, 5, 2, 11},   // approximate 12.18
		{2, 5, 2, 23},   // approximate 24.36
	}

	for _, tt := range tests {
		factor := uint256.NewInt(tt.factor)
		denom := uint256.NewInt(tt.denom)
		result, err := FakeExponential(factor, denom, tt.num)
		if err != nil {
			t.Error(err)
		}
		//t.Logf("%v*e^(%v/%v): %v", factor, num, denom, result)
		if tt.want != result.ToBig().Uint64() {
			t.Errorf("got %v want %v", result, tt.want)
		}
	}
}
