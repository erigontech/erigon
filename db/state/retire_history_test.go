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
	"path/filepath"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// testDbAndAggregatorSmallFrozen is like testDbAndAggregatorv3 but with a
// configurable stepsInFrozenFile, so "frozen" files can use small step ranges.
func testDbAndAggregatorSmallFrozen(t *testing.T, stepSize, stepsInFrozenFile uint64) *Aggregator {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).StepSize(stepSize).StepsInFrozenFile(stepsInFrozenFile).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())
	return agg
}

// generateStandaloneIIFile writes mock .ef/.efi files for an inverted index
// with no owning domain (LogAddrIdx, TracesFromIdx, ...).
func generateStandaloneIIFile(t *testing.T, name kv.InvertedIdx, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	ver := version.V1_0_standart
	stepSize := uint64(10)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, name.String(), DataExtensionEf, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).
		Build()
	createConfig := SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		MergeStages:    []uint64{20, 40},
		MinimumSize:    10,
		SafetyMargin:   5,
	}
	repo := NewSnapshotRepo(name.String(), FromII(name), &SnapshotConfig{
		SnapshotCreationConfig: &createConfig,
		Schema:                 schema,
	}, log.New())
	t.Cleanup(repo.Close)
	populateFiles2(t, dirs, repo, ranges)
}

// TestRetireOldHistoryFiles_RetiresFrozenFileEntirelyBelowCutoff pins that a
// frozen file entirely below cutoff is retired (deferred until readers
// drain), while one still at/after cutoff is kept.
func TestRetireOldHistoryFiles_RetiresFrozenFileEntirelyBelowCutoff(t *testing.T) {
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	// {0,2}: frozen (2 steps == stepsInFrozenFile). {2,3}: recent, kept.
	// Storage/Code need matching files too, or dirtyFilesEndTxNumMinimax
	// clamps the visible ceiling to zero.
	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())

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

	n, err := agg.RetireOldHistoryFiles(t.Context(), HistoryRetireCutoffs{Default: 2 * stepSize})
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

// TestEntirelyBeforeStep_BoundaryStraddlingFileKept is a direct unit test of
// entirelyBeforeStep's selection predicate: a file whose range straddles the
// cutoff must never be selected.
func TestEntirelyBeforeStep_BoundaryStraddlingFileKept(t *testing.T) {
	stepSize := uint64(10)
	df := newDirtyFiles()
	straddling := newFilesItem(9*stepSize, 11*stepSize) // steps [9,11)
	df.Set(straddling)

	outs := entirelyBeforeStep(df, stepSize, kv.Step(10))
	require.Empty(t, outs)

	outsAfterBoundaryMoves := entirelyBeforeStep(df, stepSize, kv.Step(11))
	require.Len(t, outsAfterBoundaryMoves, 1)
}

// enableCommitmentHistory un-skips the CommitmentDomain history guard for one
// test aggregator (production does this via statecfg.EnableHistoricalCommitment).
func enableCommitmentHistory(agg *Aggregator) {
	cd := agg.d[kv.CommitmentDomain]
	cd.SnapshotsDisabled = false
	cd.HistoryDisabled = false
}

// TestRetireOldHistoryFiles_RetiresCommitmentAtOwnCutoff pins commitment retirement
// at its own cutoff, independent of Default (0 here).
func TestRetireOldHistoryFiles_RetiresCommitmentAtOwnCutoff(t *testing.T) {
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	generateCommitmentHistoryAndIndexFiles(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	require.NoError(t, agg.OpenFolder())
	enableCommitmentHistory(agg)

	commitmentHist := agg.d[kv.CommitmentDomain].History
	require.Equal(t, 2, commitmentHist.dirtyFiles.Len())

	_, err := agg.RetireOldHistoryFiles(t.Context(), HistoryRetireCutoffs{
		Default:   0,
		PerDomain: map[kv.Domain]uint64{kv.CommitmentDomain: 2 * stepSize},
	})
	require.NoError(t, err)
	require.Equal(t, 1, commitmentHist.dirtyFiles.Len(), "commitment {0,2} below its cutoff must be retired, {2,3} kept")
}

// TestRetireOldHistoryFiles_KeepsDomainWhenCutoffZero pins the 0-override: a
// per-domain cutoff of 0 keeps the domain even when Default would cover it (how
// commitment keep-all and the RCacheDomain skip are expressed).
func TestRetireOldHistoryFiles_KeepsDomainWhenCutoffZero(t *testing.T) {
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	generateCommitmentHistoryAndIndexFiles(t, agg.Dirs(), []testFileRange{{0, 2}, {2, 3}})
	require.NoError(t, agg.OpenFolder())
	enableCommitmentHistory(agg)

	commitmentHist := agg.d[kv.CommitmentDomain].History

	_, err := agg.RetireOldHistoryFiles(t.Context(), HistoryRetireCutoffs{
		Default:   2 * stepSize,
		PerDomain: map[kv.Domain]uint64{kv.CommitmentDomain: 0},
	})
	require.NoError(t, err)
	require.Equal(t, 2, commitmentHist.dirtyFiles.Len(), "0 override must keep commitment even when Default covers it")
}

// TestRetireOldHistoryFiles_StandaloneII exercises the standalone-II loop
// (LogAddrIdx et al.), separate from the per-domain loop.
func TestRetireOldHistoryFiles_StandaloneII(t *testing.T) {
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateStandaloneIIFile(t, kv.LogAddrIdx, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())

	oldIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-logaddrs.0-2.ef")
	recentIdx := filepath.Join(agg.Dirs().SnapIdx, "v1.0-logaddrs.2-3.ef")
	mustExist(t, oldIdx, true)
	mustExist(t, recentIdx, true)

	at := agg.BeginFilesRo()
	n, err := agg.RetireOldHistoryFiles(t.Context(), HistoryRetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	require.Positive(t, n)
	at.Close()

	mustExist(t, oldIdx, false)
	mustExist(t, recentIdx, true)
}

// TestRetireOldHistoryFiles_ReclaimConcurrent stresses BeginFilesRo/Close
// against concurrent retirement under the race detector.
func TestRetireOldHistoryFiles_ReclaimConcurrent(t *testing.T) {
	stepSize, stepsInFrozenFile := uint64(10), uint64(2)
	agg := testDbAndAggregatorSmallFrozen(t, stepSize, stepsInFrozenFile)

	ranges := []testFileRange{{0, 2}, {2, 3}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	_, err := agg.RetireOldHistoryFiles(t.Context(), HistoryRetireCutoffs{Default: 2 * stepSize})
	require.NoError(t, err)
	close(stop)
	wg.Wait()

	mustExist(t, filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v"), false)
}
