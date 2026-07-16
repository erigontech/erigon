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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
)

func TestMerkleizeProgressiveReferenceVectors(t *testing.T) {
	// EIP-7916 is pinned here to ethereum/EIPs revision
	// 88da569e65d32ad6efad017f1e40d107f1de8394 (merkleize to the right).
	// The expected roots were generated with ethereum/remerkleable's
	// subtree_fill_progressive at 97d970e107214b59d146dffa7d837e7144b457e6
	// (v0.1.31). These cases cover each transition among the 1, 4, 16, 64,
	// and 256-leaf subtrees.
	tests := []struct {
		name       string
		chunkCount int
		expected   string
	}{
		{name: "empty", chunkCount: 0, expected: "0x0000000000000000000000000000000000000000000000000000000000000000"},
		{name: "end first subtree", chunkCount: 1, expected: "0x037d6dfb3a369a41e01100fdd53c35ee3fb69ddec5830d61e1138d066a4c2285"},
		{name: "start four-leaf subtree", chunkCount: 2, expected: "0x2dfe47da19ad9ff11afe44dd8de4db8517cefd5a9bddffe6652b26a1b91ea5ac"},
		{name: "end four-leaf subtree", chunkCount: 5, expected: "0x3fd53b812118ddea60b9deab5c72d32b0c4dcfd2c94deda753e6e1d548fbc274"},
		{name: "start sixteen-leaf subtree", chunkCount: 6, expected: "0x2e2a2abd4d0e28498ec0cdd817c715b246aa15e7b34767061b7632337188429e"},
		{name: "end sixteen-leaf subtree", chunkCount: 21, expected: "0xf148f679afbfebfe5616080a45461aee3d1f4ce2cc752ce824c3f067d2707623"},
		{name: "start sixty-four-leaf subtree", chunkCount: 22, expected: "0x040be60071c540aafc1d44f366239ab6a41bf8740a38f9d52ab0bbd9cd974c45"},
		{name: "end sixty-four-leaf subtree", chunkCount: 85, expected: "0x24ea21562226364be74fd2696d0824a4347cfac7dd4b2ae28cd0e9cc22bc341d"},
		{name: "start two-hundred-fifty-six-leaf subtree", chunkCount: 86, expected: "0xb73c4c427974f47c74c2812d353c966f5dadae70c44f6fe9a15e179b86914977"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunks := progressiveTestChunks(test.chunkCount)
			original := make([][32]byte, len(chunks))
			copy(original, chunks)

			root, err := merkle_tree.MerkleizeProgressive(chunks)
			require.NoError(t, err)
			require.Equal(t, [32]byte(common.HexToHash(test.expected)), root)
			require.Equal(t, original, chunks, "input chunks must not be modified")
		})
	}
}

func TestMixInActiveFieldsReferenceVectors(t *testing.T) {
	// EIP-7495 is pinned here to ethereum/EIPs revision
	// c81d843b3f8aa839fe42911c5b6e501c7d2940a3. The expected roots were
	// generated with ethereum/remerkleable's ProgressiveContainer backing at
	// 2f0baeef0082d4278acaef7d822deb7009d7db7e. EIP-7807 revision
	// 75d7bc2c20a91ec017d147f400d6bbf767843e2c defines ExecutionPayload with
	// active_fields=[1] * 18.
	var root [32]byte
	for i := range root {
		root[i] = byte(i)
	}

	minimumPacked := [32]byte{0x01}

	executionPayloadFields := make([]bool, 18)
	for i := range executionPayloadFields {
		executionPayloadFields[i] = true
	}
	executionPayloadPacked := [32]byte{0xff, 0xff, 0x03}

	sparseFields := make([]bool, 18)
	for _, i := range []int{0, 7, 8, 17} {
		sparseFields[i] = true
	}
	sparsePacked := [32]byte{0x81, 0x01, 0x02}

	boundaryFields := make([]bool, 256)
	boundaryFields[255] = true
	var boundaryPacked [32]byte
	boundaryPacked[31] = 0x80

	tests := []struct {
		name         string
		activeFields []bool
		packed       [32]byte
		expected     string
	}{
		{
			name:         "minimum legal active fields",
			activeFields: []bool{true},
			packed:       minimumPacked,
			expected:     "0xe987b42bd50123fe7764ebae4f4155beebd99b9ede2613a632484aa090e270df",
		},
		{
			name:         "EIP-7807 ExecutionPayload fields",
			activeFields: executionPayloadFields,
			packed:       executionPayloadPacked,
			expected:     "0xe8db024fb74db97de963cb6aa6e34ae26ed3c45b245fbff7c7ef9109e24eaccc",
		},
		{
			name:         "sparse fields use little-endian bit order",
			activeFields: sparseFields,
			packed:       sparsePacked,
			expected:     "0x68ef7766df9e31c60ea4cab1200dc2789a965957f087b321217c1fa3e0846d75",
		},
		{
			name:         "256-bit boundary",
			activeFields: boundaryFields,
			packed:       boundaryPacked,
			expected:     "0x21e35a7be0be70b70c352ff4940961f9e2492e4d5af1ed164c83c66259efb9d0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mixed, err := merkle_tree.MixInActiveFields(root, test.activeFields)
			require.NoError(t, err)
			require.Equal(t, [32]byte(common.HexToHash(test.expected)), mixed)

			var input [64]byte
			copy(input[:32], root[:])
			copy(input[32:], test.packed[:])
			require.Equal(t, sha256.Sum256(input[:]), mixed, "packed active fields differ")
		})
	}
}

func TestMixInActiveFieldsRejectsMoreThan256Bits(t *testing.T) {
	mixed, err := merkle_tree.MixInActiveFields([32]byte{}, make([]bool, 257))
	require.EqualError(t, err, "active fields exceed 256 bits")
	require.Zero(t, mixed)
}

func TestMixInActiveFieldsDoesNotModifyRoot(t *testing.T) {
	root := [32]byte{0x42, 0x43, 0x44}
	original := root

	_, err := merkle_tree.MixInActiveFields(root, []bool{true, false, true})
	require.NoError(t, err)
	require.Equal(t, original, root)
}

func progressiveTestChunks(count int) [][32]byte {
	chunks := make([][32]byte, count)
	for i := range chunks {
		for j := range chunks[i] {
			chunks[i][j] = byte(i + 1)
		}
	}
	return chunks
}
