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

package branch

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBranchData_IsComplete(t *testing.T) {
	t.Parallel()

	// Buffers shorter than the 4-byte touchMap+afterMap header are not complete
	// and must not panic. Empty values are deletion tombstones kept across merges.
	require.False(t, BranchData(nil).IsComplete())
	require.False(t, BranchData{}.IsComplete())
	require.False(t, BranchData{0x00}.IsComplete())
	require.False(t, BranchData{0xff, 0xff, 0x00}.IsComplete())

	complete := make(BranchData, 4)
	binary.BigEndian.PutUint16(complete[0:], 0xffff)
	binary.BigEndian.PutUint16(complete[2:], 0b0000_0000_0000_0111)
	require.True(t, complete.IsComplete())

	incomplete := make(BranchData, 4)
	binary.BigEndian.PutUint16(incomplete[0:], 0b0000_0000_0000_0001)
	binary.BigEndian.PutUint16(incomplete[2:], 0b0000_0000_0000_0011)
	require.False(t, incomplete.IsComplete())
}

func TestBranchData_MergeHexBranchesEmptyBranches(t *testing.T) {
	t.Parallel()

	merger := NewHexBranchMerger(1024)

	branch1 := BranchData{}
	branch2 := BranchData{0x02, 0x02, 0x03, 0x03, 0x0C, 0x02, 0x04, 0x0C}
	mergedBranch, err := merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch2, mergedBranch)

	branch1 = BranchData{}
	branch2 = BranchData{}
	mergedBranch, err = merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch1, mergedBranch)
}
