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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

// commitmentVersionCounts scans the snapshot domain dir and returns how many commitment .kv
// files fall in the referenced regime (version < v2.1 and carry short keys) vs the plain regime
// (version >= v2.1).
func commitmentVersionCounts(t *testing.T, dir string) (referenced, plain int) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range ents {
		n := e.Name()
		if !strings.Contains(n, "commitment") || !strings.HasSuffix(n, ".kv") {
			continue
		}
		ver, _, ok := strings.Cut(n, "-")
		require.True(t, ok)
		fv, err := version.ParseVersion(ver)
		require.NoError(t, err)
		_, short := branchKeyKinds(t, filepath.Join(dir, n))
		if fv.Less(version.V2_1) {
			if short > 0 {
				referenced++
			}
		} else {
			plain++
		}
	}
	return referenced, plain
}

// TestMixedVersionDatadirReadsConsistentAcrossFlag builds a single datadir holding both
// v2.0-referenced and v2.1-plain commitment files, then proves reads are driven by each file's
// own version, not the live write flag: the recomputed state root is identical with the flag on
// and off, and flipping it both directions performs no migration (file set unchanged).
func TestMixedVersionDatadirReadsConsistentAcrossFlag(t *testing.T) {
	const stepSize = uint64(10)
	const frozenSteps = uint64(4) // domain files freeze at 4 steps (>= referencing threshold 2)
	setA := mkAddrs(0x10, 12)     // written only in the frozen v2.0 range; never superseded
	setB := mkAddrs(0xf0, 12)     // written throughout; evolves into the later plain (v2.1) files

	db, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()
	// Cap domain merges at a small frozen size so a referenced (v2.0) file written with the flag
	// on and a plain (v2.1) file written with the flag off both freeze and coexist — neither is
	// consumed (and thus rewritten to the other regime) by a later merge.
	agg.SetErigondbDomainStepsInFrozenFile(frozenSteps)

	// flag ON -> freeze a referenced (v2.0) 0-4 file holding setA, whose branches stay the read
	// winners (setA is never written again, so its deref is exercised on read)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	writeStepsKeys(t, db, agg, setA, 0, frozenSteps)
	writeStepsKeys(t, db, agg, setB, frozenSteps, 2*frozenSteps)
	require.NoError(t, agg.BuildFiles(2*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	// flag OFF -> freeze plain (v2.1) files for the later range; the frozen v2.0 0-4 file is not a
	// merge input, so it stays referenced while later ranges become plain
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	writeStepsKeys(t, db, agg, setB, 2*frozenSteps, 3*frozenSteps)
	require.NoError(t, agg.BuildFiles(3*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	// reopen so each file's version comes from its on-disk name
	db, agg = reopenAggregator(t, db, agg, stepSize)
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))

	referenced, plain := commitmentVersionCounts(t, dirs.SnapDomain)
	require.Positive(t, referenced, "datadir must contain v2.0 referenced commitment files")
	require.Positive(t, plain, "datadir must contain v2.1 plain commitment files")
	assertCommitmentVersionConsistency(t, dirs.SnapDomain)

	// reads are version-driven: same root regardless of the live flag
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	rootOn := recomputeRootFromState(t, db)
	require.NotEmpty(t, rootOn)

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	rootOff := recomputeRootFromState(t, db)
	require.Equal(t, rootOn, rootOff, "flipping the flag off must not change reads on a populated datadir")

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	require.Equal(t, rootOn, recomputeRootFromState(t, db), "flipping the flag back on must stay correct")

	// reads performed no migration: file set and regimes unchanged
	assertCommitmentVersionConsistency(t, dirs.SnapDomain)
	referencedAfter, plainAfter := commitmentVersionCounts(t, dirs.SnapDomain)
	require.Equal(t, referenced, referencedAfter)
	require.Equal(t, plain, plainAfter)
}
