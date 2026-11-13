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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

func TestMerkleRootFromFlatFromIntermediateLevelWithLimitRejectsOverLimit(t *testing.T) {
	var root [32]byte
	// 5 chunks at the leaf level with a limit of 4 leaves.
	require.Error(t, merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(make([]byte, 5*32), root[:], 4, 0))
}

func TestMerkleRootFromFlatFromIntermediateLevelWithLimitRejectsOverLimitAboveLeafLevel(t *testing.T) {
	var root [32]byte
	// 2 nodes at level 1 hold up to 4 leaves, which exceeds a limit of 2.
	require.Error(t, merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(make([]byte, 2*32), root[:], 2, 1))
}

func TestMerkleRootFromFlatFromIntermediateLevelWithLimitAcceptsPaddedLevels(t *testing.T) {
	// A legal tree carries padded nodes at levels above the leaves, so a level
	// may hold more nodes than the leaf count, but never more than the limit
	// spread over that level.
	cases := []struct {
		chunks uint64
		limit  uint64
		start  uint64
	}{
		{2, 3, 1}, // 3 leaves, limit 3
		{3, 5, 1}, // 5 or 6 leaves, limit 5
		{1, 3, 2}, // 3 leaves, limit 3
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("chunks%d_limit%d_level%d", c.chunks, c.limit, c.start), func(t *testing.T) {
			var root [32]byte
			require.NoError(t, merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(make([]byte, c.chunks*32), root[:], int(c.limit), int(c.start)))
		})
	}
}
