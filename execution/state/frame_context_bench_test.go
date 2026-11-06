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

package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkNewFrameContext(b *testing.B) {
	for _, dataSize := range []int{0, 128 * 1024} {
		b.Run(fmt.Sprint(dataSize), func(b *testing.B) {
			var ibs IntraBlockState
			tx := &types.FrameTransaction{
				NonceKeys:  []uint256.Int{{1}, {2}},
				Frames:     []types.Frame{{Data: make([]byte, dataSize)}},
				Signatures: []types.FrameSignature{{Signature: make([]byte, 65)}},
			}
			b.ReportAllocs()
			for b.Loop() {
				ibs.NewFrameContext(tx, uint256.Int{})
			}
		})
	}
}
