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

package peerdasutils

import (
	"fmt"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
)

func BenchmarkRecoverMatrix(b *testing.B) {
	for _, blobCount := range []int{1, 6, 9, 16} {
		full, half := testMatrix(b, blobCount)
		for _, tc := range []struct {
			name  string
			input []cltypes.MatrixEntry
		}{{"half", half}, {"complete", slices.Concat(full...)}} {
			b.Run(fmt.Sprintf("blobs=%d/%s", blobCount, tc.name), func(b *testing.B) {
				for b.Loop() {
					if _, err := RecoverMatrix(tc.input, uint64(blobCount)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
