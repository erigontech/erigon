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

package merkle_tree_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
)

var nonPowerOfTwoLimits = []uint64{3, 5, 6, 12}

// referenceMerkleize follows the SSZ spec literally: pad the chunks with zero
// chunks up to next_pow_of_two(limit), then hash pairwise up to one root.
func referenceMerkleize(chunks [][32]byte, limit uint64) [32]byte {
	width := uint64(1)
	for width < limit {
		width *= 2
	}
	layer := make([][32]byte, width)
	copy(layer, chunks)
	var pair [64]byte
	for len(layer) > 1 {
		next := make([][32]byte, len(layer)/2)
		for i := range next {
			copy(pair[:32], layer[2*i][:])
			copy(pair[32:], layer[2*i+1][:])
			next[i] = sha256.Sum256(pair[:])
		}
		layer = next
	}
	return layer[0]
}

func TestMerkleizeVectorNonPowerOfTwoLimit(t *testing.T) {
	for _, limit := range nonPowerOfTwoLimits {
		for count := uint64(0); count <= limit; count++ {
			t.Run(fmt.Sprintf("limit%d_chunks%d", limit, count), func(t *testing.T) {
				chunks := progressiveTestChunks(int(count))
				root, err := merkle_tree.MerkleizeVector(progressiveTestChunks(int(count)), limit)
				require.NoError(t, err)
				require.Equal(t, referenceMerkleize(chunks, limit), root)
			})
		}
	}
}

func TestMerkleRootFromFlatLeavesWithLimitNonPowerOfTwo(t *testing.T) {
	for _, limit := range nonPowerOfTwoLimits {
		for count := uint64(1); count <= limit; count++ {
			t.Run(fmt.Sprintf("limit%d_chunks%d", limit, count), func(t *testing.T) {
				chunks := progressiveTestChunks(int(count))
				var root [32]byte
				require.NoError(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(flatChunks(chunks), root[:], limit))
				require.Equal(t, referenceMerkleize(chunks, limit), root)
			})
		}
	}
}

func TestMerkleTreeNonPowerOfTwoLimit(t *testing.T) {
	for _, limit := range nonPowerOfTwoLimits {
		for count := uint64(0); count <= limit; count++ {
			t.Run(fmt.Sprintf("limit%d_leaves%d", limit, count), func(t *testing.T) {
				chunks := progressiveTestChunks(int(count))
				tree := &merkle_tree.MerkleTree{}
				tree.Initialize(int(count), merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
					copy(out, chunks[idx][:])
				}, &limit)
				require.Equal(t, common.Hash(referenceMerkleize(chunks, limit)), tree.ComputeRoot())
			})
		}
	}
}

func TestMerkleProofRejectsEmptySchema(t *testing.T) {
	_, err := merkle_tree.MerkleProof(0, 0)
	require.Error(t, err)
}

func flatChunks(chunks [][32]byte) []byte {
	leaves := make([]byte, 0, len(chunks)*32)
	for _, chunk := range chunks {
		leaves = append(leaves, chunk[:]...)
	}
	return leaves
}

func TestMerkleizeVectorRejectsOverLimit(t *testing.T) {
	for _, limit := range []uint64{0, 3, 4} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			_, err := merkle_tree.MerkleizeVector(progressiveTestChunks(int(limit)+1), limit)
			require.Error(t, err)
		})
	}
}

func TestMerkleRootFromFlatLeavesWithLimitRejectsOverLimit(t *testing.T) {
	for _, limit := range []uint64{0, 3, 4} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			var root [32]byte
			leaves := flatChunks(progressiveTestChunks(int(limit) + 1))
			require.Error(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(leaves, root[:], limit))
		})
	}
}

func TestMerkleRootFromFlatLeavesWithLimitEmpty(t *testing.T) {
	for _, limit := range []uint64{0, 1, 3, 4, 16} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			var root [32]byte
			require.NoError(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(nil, root[:], limit))
			want, err := merkle_tree.MerkleizeVector(nil, limit)
			require.NoError(t, err)
			require.Equal(t, want, root)
		})
	}
}

func TestMerkleRootFromFlatLeavesWithLimitUnbalanced(t *testing.T) {
	var root [32]byte
	require.NoError(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit([]byte{1, 2, 3, 4, 5}, root[:], 4))

	padded := make([]byte, 32)
	copy(padded, []byte{1, 2, 3, 4, 5})
	var want [32]byte
	require.NoError(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(padded, want[:], 4))
	require.Equal(t, want, root)

	require.Error(t, merkle_tree.MerkleRootFromFlatLeavesWithLimit(make([]byte, 4*32+1), root[:], 4))
}

func TestBitlistRootWithLimitRejectsOverLimit(t *testing.T) {
	_, err := merkle_tree.BitlistRootWithLimit([]byte{0x04}, 1)
	require.Error(t, err, "2 bits, limit 1")

	bits := make([]byte, 38)
	bits[0] = 0xff
	bits[37] = 0x01
	_, err = merkle_tree.BitlistRootWithLimit(bits, 256)
	require.Error(t, err, "296 bits, limit 256")

	_, err = merkle_tree.BitlistRootWithLimit(bits, 296)
	require.NoError(t, err, "296 bits, limit 296")
}
