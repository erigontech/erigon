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

package solid_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// Vectors whose length is not a multiple of 4 end in a partial chunk, which must hash as zero-padded.
func TestUint64VectorHashSSZPartialLastChunk(t *testing.T) {
	expected := map[int]string{
		5:  "0xbf033e82435fc6915833d0f0325b9a752b2bef67493b9d27939e9b2fef56a5a8",
		6:  "0xac136edda3bdd2e949a19a945b1ac554e4b607d339a43c540c336098fff97f2b",
		7:  "0x7e33e08fca38a507e49c5e936185292363c2a143d5ff3bb04e90e1a0d056f686",
		9:  "0x7b0a7b9b36024174ef6cbf7de0228f6b1075c0897d01e518dc0b846992f95ba1",
		13: "0x73d8ce9daf208be1dce23bd44b80fffd8462893d67ccf5c8c0b6bc205b8458a8",
	}
	for size, root := range expected {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			vec := solid.NewUint64VectorSSZ(size)
			for i := range size {
				vec.Set(i, uint64(i+1))
			}
			got, err := vec.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(root), common.Hash(got))
		})
	}
}
