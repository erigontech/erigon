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

func TestProgressiveListRootReferenceVectors(t *testing.T) {
	tests := []struct {
		name          string
		chunkCount    int
		logicalLength uint64
		expected      string
	}{
		{name: "empty", chunkCount: 0, logicalLength: 0, expected: "0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b"},
		{name: "single composite element", chunkCount: 1, logicalLength: 1, expected: "0xa21da97c8a597221c87c9ea5ecdfbd860fcd52fd6fb5b001723f6437856c8df1"},
		{name: "start four-leaf subtree", chunkCount: 2, logicalLength: 2, expected: "0x9a4badc45a45e9dd4b131c2c1aaff8a054527d8db40d2a7cd07e8f0f02a8232b"},
		{name: "end four-leaf subtree", chunkCount: 5, logicalLength: 5, expected: "0x183886e81b2e887d5960b2fa49b3464eabee62ec55ff5e6ee6f7e0495d8a01d1"},
		{name: "start sixteen-leaf subtree", chunkCount: 6, logicalLength: 6, expected: "0x690beb7f075e2dc91699aa3ee9354687772923889ce755458cb405ae95e34055"},
		{name: "end sixteen-leaf subtree", chunkCount: 21, logicalLength: 21, expected: "0x93589633f10a1e8fe51bef0481731c7c19d7a87269127b6c6a19720668ee47da"},
		{name: "start sixty-four-leaf subtree", chunkCount: 22, logicalLength: 22, expected: "0xe79bdcda4e58dd09c4b855964e1f1c01c99e215b6e01602f5302763effaf8637"},
		{name: "sixteen packed uint16 values", chunkCount: 1, logicalLength: 16, expected: "0xe803cfbc4d0caecd70bff297ea98e07b6c3e4d0057e7321e020a4023fe52a9d7"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunks := progressiveTestChunks(test.chunkCount)
			original := make([][32]byte, len(chunks))
			copy(original, chunks)

			root, err := merkle_tree.ProgressiveListRoot(chunks, test.logicalLength)
			require.NoError(t, err)
			require.Equal(t, [32]byte(common.HexToHash(test.expected)), root)
			require.Equal(t, original, chunks, "input chunks must not be modified")
		})
	}
}

func TestProgressiveByteListRootReferenceVectors(t *testing.T) {
	tests := []struct {
		name     string
		byteLen  int
		expected string
	}{
		{name: "empty", byteLen: 0, expected: "0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b"},
		{name: "single byte", byteLen: 1, expected: "0x905efb51c2764c2c7a4efb0548e372569df06db82115c3b1896c186632f3fe5b"},
		{name: "before first chunk boundary", byteLen: 31, expected: "0x3e12b2d2b507ef7ffe70761d0b0b69af7a26449621227a7a3e06438917f4aebd"},
		{name: "at first chunk boundary", byteLen: 32, expected: "0x77a8c5b3ec7b888068f0d2f0237b535b7ac6dc38c9ce75ed40a3bb6250537bc9"},
		{name: "after first chunk boundary", byteLen: 33, expected: "0xbdb0c331db145d1efad9e022c70ab1f1c0896e7fc8bd8a83c6f0cd6ca89e1009"},
		{name: "end four-leaf subtree", byteLen: 160, expected: "0x2927fdb091eebe601a502656169f978a3fb2cf2a641ee97aad0f25458ef0f93a"},
		{name: "start sixteen-leaf subtree", byteLen: 161, expected: "0x14103a66a65d67d16d80e2b914241af6d1a371dc03925e610f2ebf7068d22c05"},
		{name: "end sixteen-leaf subtree", byteLen: 672, expected: "0x03a448071ba4184a8586b78b154880195db0a7a29aef04bd865f77c3469d6d5b"},
		{name: "start sixty-four-leaf subtree", byteLen: 673, expected: "0xd411caaeaf48519cd983b24a40115851e63996cbd47927651f686679a23c6c71"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := progressiveByteListTestData(test.byteLen)
			original := make([]byte, len(data))
			copy(original, data)

			root, err := merkle_tree.ProgressiveByteListRoot(data)
			require.NoError(t, err)
			require.Equal(t, [32]byte(common.HexToHash(test.expected)), root)
			require.Equal(t, original, data, "input bytes must not be modified")
		})
	}
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

func progressiveByteListTestData(length int) []byte {
	data := make([]byte, length)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	return data
}
