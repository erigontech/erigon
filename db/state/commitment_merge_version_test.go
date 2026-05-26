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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func mkAddrs(firstByte byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range n {
		a := make([]byte, length.Addr)
		a[0] = firstByte
		a[1] = byte(i)
		a[length.Addr-1] = byte(i)
		out[i] = a
	}
	return out
}

func writeStepsKeys(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, keys [][]byte, fromStep, toStep uint64) {
	t.Helper()
	stepSize := agg.StepSize()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	var blockNum uint64
	for i := fromStep * stepSize; i < toStep*stepSize; i++ {
		for j := range keys {
			acc := accounts.Account{Nonce: i + 1, Balance: *uint256.NewInt(i*100_000 + uint64(j)), CodeHash: accounts.EmptyCodeHash}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(t, err)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, i, prev))
		}
		if (i+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, blockNum, i, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
}

// branchKeyKinds opens a commitment .kv file and counts plain vs short (referenced) keys
// across all branch values.
func branchKeyKinds(t *testing.T, path string) (plain, short int) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	g.Reset(0)
	var k, v []byte
	for g.HasNext() {
		k, _ = g.Next(k[:0])
		v, _ = g.Next(v[:0])
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		_, err := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			full := length.Addr
			if isStorage {
				full = length.Addr + length.Hash
			}
			if len(key) == full {
				plain++
			} else {
				short++
			}
			return nil, nil
		})
		require.NoError(t, err)
	}
	return plain, short
}

// assertCommitmentVersionConsistency enforces the core regime invariant: a v2.1 (plain-regime)
// commitment file must contain no short reference keys, while a < v2.1 file may. A v2.1 file
// carrying short keys is the stale-offset corruption this plan prevents.
func assertCommitmentVersionConsistency(t *testing.T, dir string) (referencedFiles int) {
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
		if !fv.Less(version.V2_1) {
			require.Zerof(t, short, "v2.1+ file %s must hold only plain keys, found %d short refs (stale offsets)", n, short)
		} else if short > 0 {
			referencedFiles++
		}
	}
	return referencedFiles
}

func recomputeRootFromState(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	acit, err := rwTx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
	require.NoError(t, err)
	defer acit.Close()
	trieCtx := domains.GetCommitmentContext()
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		trieCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}
	root, err := domains.ComputeCommitment(t.Context(), rwTx, false, 0, 0, "", nil)
	require.NoError(t, err)
	return root
}

// TestCommitmentMergeFlagOffExpandsReferencedInputs exercises the corruption vector: v2.0
// referenced commitment files merged with the write flag off. The transformer must expand the
// referenced inputs to plain keys (never copy short offsets into the plain output). setA is
// written only in early steps with a disjoint nibble prefix, so its referenced branches survive
// as merge winners (not superseded by later plain single-step writes) and actually reach the
// flag-off merge.
func TestCommitmentMergeFlagOffExpandsReferencedInputs(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	setA := mkAddrs(0x10, 12) // early-only, disjoint subtree -> branches stay referenced
	setB := mkAddrs(0xf0, 12) // updated throughout

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

// TestCommitmentRebuildSqueezeReadableAfterReload runs the full rebuild → squeeze cycle, which
// toggles the write flag off (rebuild loop, writing plain v2.1) then on (squeeze, re-referencing).
// The squeezed files must be stamped v2.0 so they read back correctly after a disk reload; the
// previous behavior reused the plain v2.1 name for referenced content (stale-offset corruption).
func TestCommitmentRebuildSqueezeReadableAfterReload(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: stepSize, disableCommitmentBranchTransform: false})
	dirs := agg.Dirs()

	refRoot := recomputeRootFromState(t, db)
	require.NotEmpty(t, refRoot)

	wipeCommitment(t, db, agg, dirs)

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

// branchKeyKindsVal counts plain vs short (referenced) keys in a single branch value.
func branchKeyKindsVal(t *testing.T, v []byte) (plain, short int) {
	t.Helper()
	_, err := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		full := length.Addr
		if isStorage {
			full = length.Addr + length.Hash
		}
		if len(key) == full {
			plain++
		} else {
			short++
		}
		return nil, nil
	})
	require.NoError(t, err)
	return plain, short
}

// referencedBranchPrefixes returns the branch prefixes whose stored value carries short
// (referenced) keys in the given commitment .kv file.
func referencedBranchPrefixes(t *testing.T, path string) [][]byte {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	g.Reset(0)
	var k, v []byte
	var out [][]byte
	for g.HasNext() {
		k, _ = g.Next(k[:0])
		v, _ = g.Next(v[:0])
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		if _, short := branchKeyKindsVal(t, v); short > 0 {
			out = append(out, bytes.Clone(k))
		}
	}
	return out
}

// commitmentRangeReferenced reports whether the on-disk commitment file covering the given
// tx range is in the referenced regime (version < v2.1 and range >= the referencing threshold).
func commitmentRangeReferenced(t *testing.T, dir string, fileStart, fileEnd, stepSize uint64) bool {
	t.Helper()
	fromStep, toStep := fileStart/stepSize, fileEnd/stepSize
	if toStep-fromStep < 2 {
		return false
	}
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	suffix := fmt.Sprintf("commitment.%d-%d.kv", fromStep, toStep)
	for _, e := range ents {
		n := e.Name()
		if !strings.HasSuffix(n, suffix) {
			continue
		}
		ver, _, ok := strings.Cut(n, "-")
		require.True(t, ok)
		fv, err := version.ParseVersion(ver)
		require.NoError(t, err)
		return fv.Less(version.V2_1)
	}
	return false
}

// TestCommitmentReadDerefsReferencedFileWithFlagOff pins the branch's central safety claim at the
// real read boundary: a referenced (v2.0) commitment file must have its short keys expanded back
// to plain on read even when the live write flag is off — the deref is driven by the file's own
// version, not the flag. setA is frozen early as a v2.0 referenced file and never rewritten, so
// its referenced branches stay the read winners; setB advances the tx range into later plain
// (v2.1) files. Disabling deref (or making the predicate flag-dependent) leaves short offsets in
// the read result for the setA branches and fails here.
func TestCommitmentReadDerefsReferencedFileWithFlagOff(t *testing.T) {
	const stepSize = uint64(10)
	const frozenSteps = uint64(4)
	setA := mkAddrs(0x10, 12) // frozen-early, disjoint subtree -> referenced branches stay read winners
	setB := mkAddrs(0xf0, 12) // advances the tx range into later plain files

	db, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()
	agg.SetErigondbDomainStepsInFrozenFile(frozenSteps)

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	writeStepsKeys(t, db, agg, setA, 0, frozenSteps)
	writeStepsKeys(t, db, agg, setB, frozenSteps, 2*frozenSteps)
	require.NoError(t, agg.BuildFiles(2*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	writeStepsKeys(t, db, agg, setB, 2*frozenSteps, 3*frozenSteps)
	require.NoError(t, agg.BuildFiles(3*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	db, agg = reopenAggregator(t, db, agg, stepSize)
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))

	referenced, plain := commitmentVersionCounts(t, dirs.SnapDomain)
	require.Positive(t, referenced, "setup must produce referenced v2.0 commitment files")
	require.Positive(t, plain, "setup must produce plain v2.1 commitment files")

	var refPrefixes [][]byte
	ents, err := os.ReadDir(dirs.SnapDomain)
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
		if fv.Less(version.V2_1) {
			refPrefixes = append(refPrefixes, referencedBranchPrefixes(t, filepath.Join(dirs.SnapDomain, n))...)
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

		// Only count prefixes whose read actually sources a referenced file: those are the ones
		// where deref had to run. The mutation (deref disabled) leaves shortKeys > 0 here.
		if commitmentRangeReferenced(t, dirs.SnapDomain, fs, fe, stepSize) {
			require.Positivef(t, plainKeys, "expanded referenced branch must carry plain keys (prefix %x)", prefix)
			expandedFromReferenced++
		}
	}
	require.Positive(t, expandedFromReferenced, "at least one read must source a referenced v2.0 file and exercise deref")
}

// TestMergedCommitmentFileVersionStampedInMemory guards that a freshly-merged commitment file
// carries its write version in memory (matching the on-disk name) without waiting for a folder
// reopen. A zero in-memory version makes the merge-scheduling predicates treat a plain v2.1 file
// as referenced until restart.
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
