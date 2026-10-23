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

package solid

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// specListRoot follows the SSZ spec literally: pad the element roots to
// next_pow_of_two(limit) chunks, hash pairwise, then mix in the length.
func specListRoot(roots [][32]byte, limit int) [32]byte {
	width := 1
	for width < limit {
		width *= 2
	}
	layer := make([][32]byte, width)
	copy(layer, roots)
	for len(layer) > 1 {
		next := make([][32]byte, len(layer)/2)
		for j := range next {
			next[j] = sha256.Sum256(append(layer[2*j][:], layer[2*j+1][:]...))
		}
		layer = next
	}
	var length [32]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(roots)))
	return sha256.Sum256(append(layer[0][:], length[:]...))
}

// rootFromBranch climbs from leaf at index using branch, where branch[0] is
// the sibling closest to the leaf.
func rootFromBranch(leaf [32]byte, branch [][32]byte, index int) [32]byte {
	node := leaf
	for _, sibling := range branch {
		if index&1 == 1 {
			node = sha256.Sum256(append(sibling[:], node[:]...))
		} else {
			node = sha256.Sum256(append(node[:], sibling[:]...))
		}
		index >>= 1
	}
	return node
}

func TestListSSZElementProof(t *testing.T) {
	for _, tc := range []struct{ limit, length int }{
		{limit: 3, length: 2},
		{limit: 4, length: 3},
		{limit: 5, length: 5},
		{limit: 6, length: 4},
	} {
		t.Run(fmt.Sprintf("limit=%d/length=%d", tc.limit, tc.length), func(t *testing.T) {
			list := NewStaticListSSZ[*Checkpoint](tc.limit, CheckpointSizeSSZ)
			roots := make([][32]byte, tc.length)
			for i := range tc.length {
				c := &Checkpoint{Epoch: uint64(i + 1)}
				c.Root[0] = byte(0xa0 + i)
				list.Append(c)
				var err error
				roots[i], err = c.HashSSZ()
				require.NoError(t, err)
			}
			want := specListRoot(roots, tc.limit)

			for i := range tc.length {
				branch := list.ElementProof(i)
				require.Equal(t, want, rootFromBranch(roots[i], branch, i), "element %d", i)
			}
		})
	}
}
