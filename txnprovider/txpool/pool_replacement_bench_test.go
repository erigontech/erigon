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
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkFrameReplacementFee(b *testing.B) {
	maximum := *new(uint256.Int).SetAllOne()
	for _, tc := range []struct {
		name        string
		previous    uint256.Int
		replacement uint256.Int
	}{
		{"normal", uint256.Int{1_000_000_000}, uint256.Int{1_100_000_000}},
		{"uint256", uint256.Int{0, 0, 0, 1 << 63}, maximum},
		{"overflow", *new(uint256.Int).SubUint64(&maximum, 1), maximum},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				frameReplacementFee(&tc.previous, &tc.replacement, 10)
			}
		})
	}
}
