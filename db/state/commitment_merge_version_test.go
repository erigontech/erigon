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
	"bytes"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
)

// TestCommitmentMergeFlagOffExpandsReferencedInputs exercises the corruption vector: v2.0 referenced
// commitment files merged with the write flag off. The transformer must expand the referenced inputs
// to plain keys (never copy short offsets into the plain output). setA is written only in early steps
// with a disjoint nibble prefix, so its referenced branches survive as merge winners and reach the
// flag-off merge.
func TestCommitmentMergeFlagOffExpandsReferencedInputs(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	setA := mkAddrs(0x10, 12)
	setB := mkAddrs(0xf0, 12)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()

	// phase 1: flag on -> referenced (v2.0) files
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	writeStepsKeys(t, db, agg, setA, 0, 6)
	writeStepsKeys(t, db, agg, setB, 6, 32)
	require.NoError(t, agg.BuildFiles(32*stepSize))

	in016 := filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-16.kv")
	_, shortIn := branchKeyKinds(t, in016)
	require.Positive(t, shortIn, "flag on must produce a referenced v2.0 input with short keys")

	// phase 2: flag off -> the merge consuming the v2.0 inputs must produce plain (v2.1) output
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	writeStepsKeys(t, db, agg, setB, 32, 64)
	require.NoError(t, agg.BuildFiles(64*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	merged := filepath.Join(dirs.SnapDomain, "v2.1-commitment.0-32.kv")
	plainOut, shortOut := branchKeyKinds(t, merged)
	require.Positive(t, plainOut, "merged plain file must carry expanded plain keys")
	require.Zero(t, shortOut, "merged v2.1 plain file must not carry stale short offsets")

	// reopen from disk (file versions parsed from names) and confirm the state still reads back
	db, agg = reopenAggregator(t, db, agg, stepSize)
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	assertCommitmentVersionConsistency(t, dirs.SnapDomain)
	require.NotEmpty(t, recomputeRootFromState(t, db))
}

// TestCommitmentRebuildSqueezeReadableAfterReload runs the full rebuild → squeeze cycle, which toggles
// the write flag off (rebuild loop, writing plain v2.1) then on (squeeze, re-referencing). The squeezed
// files must be stamped v2.0 so they read back correctly after a disk reload.
func TestCommitmentRebuildSqueezeReadableAfterReload(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: stepSize, disableCommitmentBranchTransform: false})
	dirs := agg.Dirs()

	refRoot := recomputeRootFromState(t, db)
	require.NotEmpty(t, refRoot)

	db, agg = wipeCommitment(t, db, agg, dirs)

	_, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)

	// reopen fresh so commitment file versions come from the on-disk names
	db, agg = reopenAggregator(t, db, agg, stepSize)
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))

	referenced := assertCommitmentVersionConsistency(t, dirs.SnapDomain)
	require.Positive(t, referenced, "squeeze must produce referenced (v2.0) commitment files")
	require.Equal(t, refRoot, recomputeRootFromState(t, db), "rebuilt+squeezed state must read back to the same root")
}

// TestMergedCommitmentFileVersionStampedInMemory guards that a freshly-merged commitment file carries
// its write version in memory (matching the on-disk name) without waiting for a folder reopen. A zero
// in-memory version makes the merge-scheduling predicates treat a plain v2.1 file as referenced.
func TestMergedCommitmentFileVersionStampedInMemory(t *testing.T) {
	const stepSize = uint64(10)
	keys := mkAddrs(0x20, 16)

	db, agg := testDbAndAggregatorv3(t, stepSize)

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	writeStepsKeys(t, db, agg, keys, 0, 8)
	require.NoError(t, agg.BuildFiles(8*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	ac := state.AggTx(tx)

	var checked int
	for _, f := range ac.Files(kv.CommitmentDomain) {
		if (f.EndRootNum()-f.StartRootNum())/stepSize < 2 {
			continue
		}
		require.Equalf(t, version.V2_1, f.Version(),
			"merged plain commitment file %s must be stamped v2.1 in memory, not the zero version", f.Fullpath())
		checked++
	}
	require.Positive(t, checked, "setup must produce a merged commitment file at >= threshold range")
}

// TestMixedVersionDatadirReadsVersionDriven builds a datadir holding both v2.0-referenced and
// v2.1-plain commitment files and pins the branch's central safety claim: reads are driven by each
// file's own version, not the live write flag.
func TestMixedVersionDatadirReadsVersionDriven(t *testing.T) {
	const stepSize = uint64(10)
	const frozenSteps = uint64(4) // freeze below the referencing threshold so v2.0 and v2.1 files coexist
	db, agg, dirs, _, _ := buildMixedRegimeDatadir(t, stepSize, frozenSteps)

	referenced, plain := commitmentVersionCounts(t, dirs.SnapDomain)
	require.Positive(t, referenced, "datadir must contain v2.0 referenced commitment files")
	require.Positive(t, plain, "datadir must contain v2.1 plain commitment files")
	assertCommitmentVersionConsistency(t, dirs.SnapDomain)

	// A referenced (v2.0) file must have its short keys expanded on read even with the flag off — the
	// deref is driven by the file's own version, not the flag.
	t.Run("referenced branches deref with flag off", func(t *testing.T) {
		var refPrefixes [][]byte
		for _, f := range commitmentKVFiles(t, dirs.SnapDomain) {
			if f.version.Less(version.V2_1) {
				refPrefixes = append(refPrefixes, referencedBranchPrefixes(t, f.path)...)
			}
		}
		require.NotEmpty(t, refPrefixes, "setup must produce v2.0 branches carrying short keys")

		readBranch := func(prefix []byte) (v []byte, fileStart, fileEnd uint64) {
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			ac := state.AggTx(tx)
			val, ok, fs, fe, err := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, prefix, math.MaxUint64)
			require.NoError(t, err)
			require.True(t, ok)
			return bytes.Clone(val), fs, fe
		}

		var expandedFromReferenced int
		for _, prefix := range refPrefixes {
			agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
			vOn, _, _ := readBranch(prefix)
			agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
			vOff, fs, fe := readBranch(prefix)

			require.Equalf(t, vOn, vOff, "deref must be driven by file version, not the live flag (prefix %x)", prefix)
			plainKeys, shortKeys := branchKeyKindsVal(t, vOff)
			require.Zerof(t, shortKeys, "reading a referenced branch with the flag off must expand all short refs (prefix %x)", prefix)
			if commitmentRangeReferenced(t, dirs.SnapDomain, fs, fe, stepSize) {
				require.Positivef(t, plainKeys, "expanded referenced branch must carry plain keys (prefix %x)", prefix)
				expandedFromReferenced++
			}
		}
		require.Positive(t, expandedFromReferenced, "at least one read must source a referenced v2.0 file and exercise deref")
	})

	// Flipping the flag on a populated datadir performs no migration: the recomputed root is identical
	// across flips and the file set/regimes are unchanged.
	t.Run("root stable across flag flips, no migration", func(t *testing.T) {
		referencedBefore, plainBefore := commitmentVersionCounts(t, dirs.SnapDomain)

		agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
		rootOn := recomputeRootFromState(t, db)
		require.NotEmpty(t, rootOn)

		agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
		require.Equal(t, rootOn, recomputeRootFromState(t, db), "flipping the flag off must not change reads on a populated datadir")

		agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
		require.Equal(t, rootOn, recomputeRootFromState(t, db), "flipping the flag back on must stay correct")

		assertCommitmentVersionConsistency(t, dirs.SnapDomain)
		referencedAfter, plainAfter := commitmentVersionCounts(t, dirs.SnapDomain)
		require.Equal(t, referencedBefore, referencedAfter)
		require.Equal(t, plainBefore, plainAfter)
	})
}
