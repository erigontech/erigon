// Copyright 2024 The Erigon Authors
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
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// testDbAndAggregatorSmallFrozen is like testDbAndAggregatorv3 but with a
// configurable stepsInFrozenFile, so "frozen" files can use small step ranges.
// addCommitmentDependencies hides accounts/storage files that no commitment file covers.
// The schema no longer registers this dependency, so commitment can merge on its own.
func addCommitmentDependencies(agg *Aggregator) {
	agg.AddDependencyBtwnDomains(kv.AccountsDomain, kv.CommitmentDomain)
	agg.AddDependencyBtwnDomains(kv.StorageDomain, kv.CommitmentDomain)
}

func testDbAndAggregatorSmallFrozen(t *testing.T, stepSize, stepsInFrozenFile uint64) (kv.RwDB, *Aggregator) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).StepSize(stepSize).StepsInFrozenFile(stepsInFrozenFile).Logger(logger).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(db))
	return db, agg
}

// aggRetire runs Retire on a fresh RoTx (Retire is an AggregatorRoTx method). Tests that must
// assert deferred deletion pin their own RoTx across Retire instead.
func aggRetire(t *testing.T, agg *Aggregator, cutoffs kv.RetireCutoffs) (int, error) {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	return at.Retire(cutoffs)
}

// generateStandaloneIIFile writes mock .ef/.efi files for an inverted index
// with no owning domain (LogAddrIdx, TracesFromIdx, ...).
func generateStandaloneIIFile(t *testing.T, name kv.InvertedIdx, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	ver := version.V1_0_standart
	stepSize := uint64(10)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, name.String(), DataExtensionEf, ver).
		Accessor(dirs.SnapAccessors, ver).
		Build()
	populateFiles2(t, dirs, schema, stepSize, ranges)
}

// TestRetire_RetiresFrozenFileEntirelyBelowCutoff pins that a
// frozen file entirely below cutoff is retired (deferred until readers
// drain), while one still at/after cutoff is kept.
func TestRetire_RetiresFrozenFileEntirelyBelowCutoff(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	// {0,2}: frozen (2 steps == stepsInFrozenFile). {2,3}: recent, kept.
	// Storage/Code need matching files too, or dirtyFilesEndTxNumMinimax
	// clamps the visible ceiling to zero.
	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	oldHistory := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v")
	oldIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-accounts.0-2.ef")
	recentHistory := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.2-3.v")
	recentIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-accounts.2-3.ef")
	mustExist(t, oldHistory, true)
	mustExist(t, oldIdx, true)
	mustExist(t, recentHistory, true)
	mustExist(t, recentIdx, true)

	// pin the current generation to assert deferred (not immediate) deletion
	at := agg.BeginFilesRo()

	n, err := at.Retire(kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	require.Positive(t, n)

	// still pinned -> must not be deleted yet
	mustExist(t, oldHistory, true)
	mustExist(t, oldIdx, true)

	at.Close()
	mustExist(t, oldHistory, false)
	mustExist(t, oldIdx, false)
	mustExist(t, recentHistory, true)
	mustExist(t, recentIdx, true)

	at2 := agg.BeginFilesRo()
	defer at2.Close()
	hf := at2.d[kv.AccountsDomain].ht.files
	require.Len(t, hf, 1)
	require.Equal(t, uint64(2), hf[0].startTxNum/stepSize)
	require.Equal(t, uint64(3), hf[0].endTxNum/stepSize)
}

// A Retire through an older view must report only the files it detached, not
// the ones another view already retired.
func TestRetire_StaleViewReportsOnlyDetachedFiles(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)
	ranges := []testFileRange{{0, 2}, {2, 4}, {4, 5}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	var deleted []string
	agg.OnFilesChange(nil, func(paths []string) { deleted = append(deleted, paths...) })

	stale := agg.BeginFilesRo()
	defer stale.Close()
	_, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)

	deleted = nil
	_, err = stale.Retire(kv.RetireCutoffs{Default: 4 * stepSize})
	require.NoError(t, err)
	require.NotEmpty(t, deleted)
	for _, p := range deleted {
		require.NotContains(t, p, ".0-2.", "already retired by another view: %s", p)
	}
}

// A Retire through an older view whose aged files a merge already replaced
// still retires the dirty files up to that view's boundary.
func TestRetire_StaleViewRetiresMergedReplacement(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)
	generate := func(ranges []testFileRange) {
		generateAccountsFile(t, agg.Dirs(), ranges)
		generateCodeFile(t, agg.Dirs(), ranges)
		generateStorageFile(t, agg.Dirs(), ranges)
		generateCommitmentFile(t, agg.Dirs(), ranges)
	}
	generate([]testFileRange{{0, 2}, {2, 4}, {4, 5}})
	require.NoError(t, agg.OpenFolder(db))

	stale := agg.BeginFilesRo()
	defer stale.Close()
	generate([]testFileRange{{0, 4}})
	require.NoError(t, agg.OpenFolder(db))
	agg.cleanAfterMerge(nil)

	_, err := stale.Retire(kv.RetireCutoffs{Default: 4 * stepSize})
	require.NoError(t, err)

	accountsHist := agg.d[kv.AccountsDomain].History
	for _, dirty := range []*DirtyFiles{accountsHist.dirtyFiles, accountsHist.InvertedIndex.dirtyFiles} {
		var starts []uint64
		dirty.Scan(func(item *FilesItem) bool {
			starts = append(starts, item.startTxNum/stepSize)
			return true
		})
		require.Equal(t, []uint64{4}, starts)
	}
}

// TestEntirelyBeforeStep_BoundaryStraddlingFileKept is a direct unit test of
// entirelyBeforeStep's selection predicate: a file whose range straddles the
// cutoff must never be selected.
func TestEntirelyBeforeStep_BoundaryStraddlingFileKept(t *testing.T) {
	stepSize := uint64(10)
	straddling := newFilesItem(9*stepSize, 11*stepSize) // steps [9,11)
	files := visibleFiles{{src: straddling}}

	outs := entirelyBeforeStep(files, stepSize, kv.Step(10))
	require.Empty(t, outs)

	outsAfterBoundaryMoves := entirelyBeforeStep(files, stepSize, kv.Step(11))
	require.Len(t, outsAfterBoundaryMoves, 1)
}

// enableCommitmentHistory un-skips the CommitmentDomain history guard for one
// test aggregator (production does this via statecfg.EnableHistoricalCommitment).
func enableCommitmentHistory(agg *Aggregator) {
	cd := agg.d[kv.CommitmentDomain]
	cd.SnapshotsDisabled = false
	cd.HistoryDisabled = false
}

// TestRetire_RetiresCommitmentAtOwnCutoff pins commitment retirement
// at its own cutoff, independent of Default (0 here).
func TestRetire_RetiresCommitmentAtOwnCutoff(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	// State domains lead the visible ceiling (dirtyFilesEndTxNumMinimax). Without them
	// commitment history is clamped out of the visible set that Retire reads.
	generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCodeFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateStorageFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCommitmentHistoryAndIndexFiles(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	require.NoError(t, agg.OpenFolder(db))
	enableCommitmentHistory(agg)

	commitmentHist := agg.d[kv.CommitmentDomain].History
	require.Equal(t, 2, commitmentHist.dirtyFiles.Len())

	_, err := aggRetire(t, agg, kv.RetireCutoffs{
		Default:   0,
		PerDomain: map[kv.Domain]uint64{kv.CommitmentDomain: 2 * stepSize},
	})
	require.NoError(t, err)
	require.Equal(t, 1, commitmentHist.dirtyFiles.Len(), "commitment {0,2} below its cutoff must be retired, {2,3} kept")
}

// TestRetire_KeepsDomainWhenCutoffZero pins the 0-override: a
// per-domain cutoff of 0 keeps the domain even when Default would cover it (how
// commitment keep-all and the RCacheDomain skip are expressed).
func TestRetire_KeepsDomainWhenCutoffZero(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	// State domains lead the visible ceiling (dirtyFilesEndTxNumMinimax). Without them
	// commitment history is clamped out of the visible set that Retire reads.
	generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCodeFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateStorageFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	generateCommitmentHistoryAndIndexFiles(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	require.NoError(t, agg.OpenFolder(db))
	enableCommitmentHistory(agg)

	commitmentHist := agg.d[kv.CommitmentDomain].History

	_, err := aggRetire(t, agg, kv.RetireCutoffs{
		Default:   2 * stepSize,
		PerDomain: map[kv.Domain]uint64{kv.CommitmentDomain: 0},
	})
	require.NoError(t, err)
	require.Equal(t, 2, commitmentHist.dirtyFiles.Len(), "0 override must keep commitment even when Default covers it")
}

// TestRetire_SubStepTxNumKeepsFiles pins that a cutoff txNum below
// one full step floors to step 0 and retires nothing (the aggregator owns this
// txNum→step floor).
func TestRetire_SubStepTxNumKeepsFiles(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, 2}})
	generateCodeFile(t, agg.Dirs(), []testFileRange{{0, 2}})
	generateStorageFile(t, agg.Dirs(), []testFileRange{{0, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 2}})
	require.NoError(t, agg.OpenFolder(db))

	n, err := aggRetire(t, agg, kv.RetireCutoffs{Default: stepSize - 1})
	require.NoError(t, err)
	require.Zero(t, n)
	mustExist(t, filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v"), true)
}

// TestRetire_StandaloneII exercises the standalone-II loop
// (LogAddrIdx et al.), separate from the per-domain loop.
func TestRetire_StandaloneII(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	// State domains lead the visible ceiling that Retire reads (see dirtyFilesEndTxNumMinimax).
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFile(t, kv.LogAddrIdx, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	oldIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-logaddrs.0-2.ef")
	recentIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-logaddrs.2-3.ef")
	mustExist(t, oldIdx, true)
	mustExist(t, recentIdx, true)

	at := agg.BeginFilesRo()
	n, err := at.Retire(kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	require.Positive(t, n)
	at.Close()

	mustExist(t, oldIdx, false)
	mustExist(t, recentIdx, true)
}

func TestRetire_IndexAndHistoryPartitionsCanDiffer(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, 2)
	historyRanges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), historyRanges)
	generateCodeFile(t, agg.Dirs(), historyRanges)
	generateStorageFile(t, agg.Dirs(), historyRanges)
	generateCommitmentFile(t, agg.Dirs(), historyRanges)
	indices := []kv.InvertedIdx{kv.TracesFromIdx, kv.TracesToIdx, kv.LogAddrIdx, kv.LogTopicIdx}
	for _, idx := range indices {
		generateStandaloneIIFile(t, idx, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {2, 3}})
	}
	require.NoError(t, agg.OpenFolder(db))
	retired, err := aggRetire(t, agg, kv.RetireCutoffs{Default: stepSize})
	require.NoError(t, err)
	require.Positive(t, retired)
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := agg.BeginFilesRo()
	defer at.Close()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		start, err := at.HistoryStartFrom(domain, tx)
		require.NoError(t, err)
		require.Zero(t, start, "a merged history file spanning the cutoff is retained")
	}
	for _, idx := range indices {
		require.Equal(t, stepSize, at.IIStartFrom(idx, tx), "standalone-index availability cannot be inferred from state history")
	}
}

// TestRetire_ReclaimConcurrent stresses BeginFilesRo/Close
// against concurrent retirement under the race detector.
func TestRetire_ReclaimConcurrent(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx := agg.BeginFilesRo()
				_ = tx.d[kv.AccountsDomain].ht.files.EndTxNum()
				tx.Close()
			}
		})
	}

	_, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	close(stop)
	wg.Wait()

	mustExist(t, filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v"), false)
}

// Retire must detach from dirtyFiles, not merely the visible view: a file left in dirty
// would be rebuilt back into visible by the next recalcVisibleFiles.
func TestRetire_DetachesFromDirtyFiles(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	accountsHist := agg.d[kv.AccountsDomain].History
	require.Equal(t, 2, accountsHist.dirtyFiles.Len())

	n, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	require.Positive(t, n)

	// detached from dirty, not just hidden from the visible view
	require.Equal(t, 1, accountsHist.dirtyFiles.Len())

	at := agg.BeginFilesRo()
	defer at.Close()
	hf := at.d[kv.AccountsDomain].ht.files
	require.Len(t, hf, 1)
	require.Equal(t, uint64(2), hf[0].startTxNum/stepSize)
}

// Retire takes its boundary from aged visible files, so a subsumed (dirty-but-not-visible) file
// below the cutoff stays while no visible file is aged.
func TestRetire_SkipsDirtyButNotVisibleFile(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {0, 4}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	accountsHist := agg.d[kv.AccountsDomain].History
	require.Equal(t, 2, accountsHist.dirtyFiles.Len())

	// [0,4] covers [0,2] -> only the covering file is visible
	at := agg.BeginFilesRo()
	require.Len(t, at.d[kv.AccountsDomain].ht.files, 1)
	require.Equal(t, uint64(4), at.d[kv.AccountsDomain].ht.files[0].endTxNum/stepSize)
	at.Close()

	// cutoff step 2: the only visible accounts file is [0,4] (endStep 4 > 2), so no visible file is
	// aged and nothing is retired, not even the invisible [0,2].
	n, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, 2, accountsHist.dirtyFiles.Len(), "with no aged visible file, nothing is retired")
}

func TestRetire_DetachesCoveredSourcesWithCoveringFile(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	for _, unindexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("unindexed=%v", unindexed), func(t *testing.T) {
			db, agg := testDbAndAggregatorSmallFrozen(t, 10, 2)
			ranges := []testFileRange{{0, 1}, {0, 2}, {2, 3}}
			generateStateFiles(t, agg.Dirs(), ranges)
			generateCommitmentFile(t, agg.Dirs(), ranges)
			generateStandaloneIIFiles(t, agg.Dirs(), ranges)
			if unindexed {
				for _, name := range []string{"v2.0-accounts.0-1.vi", "v1.0-accounts.0-1.efi"} {
					require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapAccessors, name)))
				}
			}
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()
			old := agg.BeginFilesRo()
			defer old.Close()
			require.NotPanics(t, func() {
				n, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 20})
				require.NoError(t, err)
				require.Positive(t, n)
			})
			at := agg.BeginFilesRo()
			defer at.Close()
			requireVisibleEnd(t, agg, 30)
			for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
				require.Equal(t, 1, agg.d[d].History.dirtyFiles.Len())
				require.Equal(t, 1, agg.d[d].InvertedIndex.dirtyFiles.Len())
				require.EqualValues(t, 20, at.d[d].ht.files.StartTxNum())
				require.EqualValues(t, 30, at.d[d].ht.files.EndTxNum())
			}
			path := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v")
			mustExist(t, path, true)
			old.Close()
			mustExist(t, path, false)
		})
	}
}

func TestRetire_DetachesHiddenOutputCoveredByRetiredSources(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorSmallFrozen(t, 10, 2)
	ranges := []testFileRange{{0, 1}, {1, 2}, {0, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	for _, name := range []string{"v2.0-accounts.0-2.vi", "v1.0-accounts.0-2.efi"} {
		require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapAccessors, name)))
	}
	require.NoError(t, agg.OpenFolder(db))
	agg.ForbidVisibilityLowering()
	require.NotPanics(t, func() {
		_, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 20})
		require.NoError(t, err)
	})
	require.Equal(t, 1, agg.d[kv.AccountsDomain].History.dirtyFiles.Len())
	require.Equal(t, 1, agg.d[kv.AccountsDomain].InvertedIndex.dirtyFiles.Len())
	requireVisibleEnd(t, agg, 30)
}

func TestRetire_PreservesTailBelowHiddenCoveringFile(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	for _, tc := range []struct {
		name      string
		tailEnd   uint64
		outputEnd uint64
		unindexed bool
	}{
		{"above ceiling", 3, 4, false},
		{"overlapping unindexed output", 4, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, agg := testDbAndAggregatorSmallFrozen(t, 10, 2)
			ranges := []testFileRange{{0, 2}, {2, tc.tailEnd}}
			generateStateFiles(t, agg.Dirs(), ranges)
			generateCommitmentFile(t, agg.Dirs(), ranges)
			generateStandaloneIIFiles(t, agg.Dirs(), ranges)
			generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, tc.outputEnd}})
			if tc.unindexed {
				for _, name := range []string{"vi", "efi"} {
					ver := "v1.0"
					if name == "vi" {
						ver = "v2.0"
					}
					require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapAccessors, fmt.Sprintf("%s-accounts.0-%d.%s", ver, tc.outputEnd, name))))
				}
			}
			addCommitmentDependencies(agg)
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()
			require.NotPanics(t, func() {
				_, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 20})
				require.NoError(t, err)
			})
			for i := range 2 {
				if i > 0 {
					require.NoError(t, agg.OpenFolder(db))
				}
				at := agg.BeginFilesRo()
				require.EqualValues(t, 20, at.d[kv.AccountsDomain].ht.files.StartTxNum())
				require.EqualValues(t, tc.tailEnd*10, at.d[kv.AccountsDomain].ht.files.EndTxNum())
				at.Close()
				requireVisibleEnd(t, agg, tc.tailEnd*10)
				require.Equal(t, 2, agg.d[kv.AccountsDomain].History.dirtyFiles.Len())
				require.Equal(t, 2, agg.d[kv.AccountsDomain].InvertedIndex.dirtyFiles.Len())
			}
		})
	}
}

// A cutoff reaching the visible tip would retire every file. That happens benignly when the
// unfrozen head outruns the prune distance, so Retire skips the entity (keeping its files) and
// retries once the tip advances — it must not error and poison retirement of the healthy ones.
func TestRetire_SkipsWindowTooSmall(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	db, agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder(db))

	accountsHist := agg.d[kv.AccountsDomain].History
	require.Equal(t, 2, accountsHist.dirtyFiles.Len())

	n, err := aggRetire(t, agg, kv.RetireCutoffs{Default: 3 * stepSize})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, 2, accountsHist.dirtyFiles.Len(), "no files retired when the window is too small")
}
