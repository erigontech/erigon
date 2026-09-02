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
	"testing"

	"github.com/stretchr/testify/require"
)

// A tx opened from a pin (src.Pin() then pin.BeginFilesRo()) must observe the
// SAME visible-file generation as the source tx, even after a newer generation
// is published — this keeps parallel-commitment workers on the generation their
// commitment tx was built against. A plain BeginFilesRo, by contrast, pins
// whatever generation is current when it runs.
func TestFilesPin_PinsSourceGeneration(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)

	gen := func(ranges []testFileRange) {
		t.Helper()
		generateAccountsFile(t, agg.Dirs(), ranges)
		generateCodeFile(t, agg.Dirs(), ranges)
		generateStorageFile(t, agg.Dirs(), ranges)
		generateCommitmentFile(t, agg.Dirs(), ranges)
		require.NoError(t, agg.OpenFolder())
	}

	gen([]testFileRange{{0, 1}})
	src := agg.BeginFilesRo()
	defer src.Close()
	genV1 := src.visible
	require.Same(t, agg.visible.Load(), genV1, "src pins the current generation")

	// Publish a newer generation while src stays open — only add the {1,2} step;
	// rewriting {0,1} would rename over the file src holds open, which Windows denies.
	gen([]testFileRange{{1, 2}})
	genV2 := agg.visible.Load()
	require.NotSame(t, genV1, genV2, "OpenFolder must publish a newer visible generation")

	// The fix: pin the source generation, then txns opened from the pin keep it,
	// not the newer one.
	pin := src.Pin()
	defer pin.Close()
	worker := pin.BeginFilesRo()
	require.Same(t, genV1, worker.visible, "pin.BeginFilesRo must pin the source generation")
	worker.Close()

	// Contrast — a plain BeginFilesRo pins the current (newer) generation. That is
	// the divergence the pin removes for parallel-commitment workers.
	fresh := agg.BeginFilesRo()
	require.Same(t, genV2, fresh.visible, "BeginFilesRo pins the current (newer) generation")
	fresh.Close()

	// Refcnt: a txn from the pin takes a pin on the source generation that Close releases.
	before := genV1.refcnt.Load()
	w2 := pin.BeginFilesRo()
	require.Equal(t, before+1, genV1.refcnt.Load(), "pin.BeginFilesRo increments the source generation refcnt")
	w2.Close()
	require.Equal(t, before, genV1.refcnt.Load(), "Close releases the pin")
}
