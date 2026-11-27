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

package txpool

import (
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameReplacementFee(t *testing.T) {
	maximum := *new(uint256.Int).SetAllOne()
	halfRange := uint256.Int{0, 0, 0, 1 << 63}
	// A 20% bump on this fee requires MaxUint256 + 0.6.
	roundingOverflow := *uint256.MustFromHex("0xd555555555555555555555555555555555555555555555555555555555555555")

	for _, tc := range []struct {
		name        string
		previous    uint256.Int
		replacement uint256.Int
		bump        uint64
		want        bool
	}{
		{"zero unchanged", uint256.Int{}, uint256.Int{}, 10, false},
		{"zero increased", uint256.Int{}, uint256.Int{1}, math.MaxUint64, true},
		{"fee decreased", uint256.Int{100}, uint256.Int{99}, 0, false},
		{"unchanged with zero bump", uint256.Int{100}, uint256.Int{100}, 0, false},
		{"increased with zero bump", uint256.Int{100}, uint256.Int{101}, 0, true},
		{"small fee unchanged", uint256.Int{1}, uint256.Int{1}, 10, false},
		{"small fee increased", uint256.Int{1}, uint256.Int{2}, 10, true},
		{"below exact threshold", uint256.Int{100}, uint256.Int{109}, 10, false},
		{"exact threshold", uint256.Int{100}, uint256.Int{110}, 10, true},
		{"rounded down", uint256.Int{101}, uint256.Int{111}, 10, false},
		{"rounded up", uint256.Int{101}, uint256.Int{112}, 10, true},
		{"wide product underpriced", halfRange, *new(uint256.Int).AddUint64(&halfRange, 1), 50, false},
		{"wide product exact threshold", halfRange, uint256.Int{0, 0, 0, 3 << 62}, 50, true},
		{"quotient overflow", *new(uint256.Int).SubUint64(&maximum, 1), maximum, 10, false},
		{"rounding overflow", roundingOverflow, maximum, 20, false},
		{"maximum bump underpriced", uint256.Int{100}, uint256.Int{98, 1}, math.MaxUint64, false},
		{"maximum bump exact threshold", uint256.Int{100}, uint256.Int{99, 1}, math.MaxUint64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous, replacement := tc.previous, tc.replacement
			require.Equal(t, tc.want, frameReplacementFee(&previous, &replacement, tc.bump))
			require.Equal(t, tc.previous, previous)
			require.Equal(t, tc.replacement, replacement)
		})
	}
}
