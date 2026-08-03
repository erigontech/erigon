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

package state_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/syscheck"
)

// requireAllRandom asserts that no snapshot file is left with readahead enabled.
// Snapshot data is far larger than RAM, so every mapping must stay MADV_RANDOM
// once the phase that touched it is done.
func requireAllRandom(t *testing.T, snapDir, phase string) {
	t.Helper()
	bad, err := syscheck.NonRandomUnder(snapDir)
	require.NoError(t, err)
	if len(bad) == 0 {
		return
	}
	for _, g := range bad {
		t.Errorf("after %s: %s is %s (%d mapping(s))", phase, g.Path, g.Advices(), len(g.Mappings))
	}
	t.Fatalf("after %s: %d snapshot file(s) left without MADV_RANDOM", phase, len(bad))
}

// TestMmapAdviceAcrossFilePhases pins the invariant that build, accessor
// indexing and merge each restore MADV_RANDOM on every snapshot file they map.
// A phase that madvises the shared mapping and does not put it back shows up
// here as readahead left enabled on a file far bigger than RAM.
//
// Referenced (v2.1) commitment files are built on purpose: they are what makes
// the merge take its read-through-the-shared-mmap path instead of opening a
// private sequential view.
func TestMmapAdviceAcrossFilePhases(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	snapDir := agg.Dirs().Snap

	if runtime.GOOS != "linux" {
		bad, err := syscheck.NonRandomUnder(snapDir)
		require.NoError(t, err)
		require.Empty(t, bad, "VmFlags is a Linux-only interface, nothing is observable here")
		return
	}

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	keys := mkAddrs(0x10, 12)

	writeStepsKeys(t, db, agg, keys, 0, 32)
	require.NoError(t, agg.BuildFiles(32*stepSize))
	requireAllRandom(t, snapDir, "build")

	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	requireAllRandom(t, snapDir, "indexing")

	require.NoError(t, agg.MergeLoop(t.Context()))
	requireAllRandom(t, snapDir, "merge")
}
