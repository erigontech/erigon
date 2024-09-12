// Copyright 2024 The Erigon Authors
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

package utils_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/utils"
)

func TestIsPowerOf2(t *testing.T) {
	testCases := []struct {
		n          uint64
		isPowerOf2 bool
	}{
		{0, false},
		{1, true},
		{2, true},
		{3, false},
		{4, true},
		{16, true},
		{17, false},
		{64, true},
		{100, false},
		{256, true},
	}

	for _, tc := range testCases {
		isPowerOf2 := utils.IsPowerOf2(tc.n)
		if isPowerOf2 != tc.isPowerOf2 {
			t.Errorf("IsPowerOf2 returned incorrect result for %d. Expected: %t, Got: %t", tc.n, tc.isPowerOf2, isPowerOf2)
		}
	}
}

func TestPowerOf2(t *testing.T) {
	testCases := []struct {
		n            uint64
		expectedPow2 uint64
		shouldPanic  bool
	}{
		{0, 1, false},
		{1, 2, false},
		{2, 4, false},
		{3, 8, false},
		{4, 16, false},
		{63, 9223372036854775808, false},
		{64, 0, true},
	}

	for _, tc := range testCases {
		var pow2 uint64
		func() {
			defer func() {
				if r := recover(); r != nil {
					pow2 = 0
				}
			}()

			pow2 = utils.PowerOf2(tc.n)
		}()

		if pow2 != tc.expectedPow2 {
			t.Errorf("PowerOf2 returned incorrect result for %d. Expected: %d, Got: %d", tc.n, tc.expectedPow2, pow2)
		}

		if (pow2 == 0) != tc.shouldPanic {
			t.Errorf("PowerOf2 panicked unexpectedly for %d. Expected panic: %t", tc.n, tc.shouldPanic)
		}
	}
}

func TestIntegerSquareRoot(t *testing.T) {
	testCases := []struct {
		n            uint64
		expectedSqrt uint64
	}{
		{0, 0},
		{1, 1},
		{3, 1},
		{4, 2},
		{63, 7},
		{64, 8},
		{100, 10},
		{255, 15},
		{256, 16},
		{1000, 31},
		{1000000, 1000},
		{18446744073709551615, 4294967295}, // Maximum uint64
	}

	for _, tc := range testCases {
		sqrt := utils.IntegerSquareRoot(tc.n)
		if sqrt != tc.expectedSqrt {
			t.Errorf("IntegerSquareRoot returned incorrect result for %d. Expected: %d, Got: %d", tc.n, tc.expectedSqrt, sqrt)
		}
	}
}
