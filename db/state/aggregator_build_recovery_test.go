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
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// Fixture construction chooses individual steps without recovery planning.
func (a *Aggregator) buildFiles(ctx context.Context, db kv.TemporalRoDB, step kv.Step, finalityCtx kv.FinalityContext, filesDone <-chan struct{}) error {
	return a.buildFilesWithPlan(ctx, db, step, finalityCtx, filesDone, fileBuildPlan{})
}

func snapshotFileIdentities(t *testing.T, dirs datadir.Dirs) map[string]os.FileInfo {
	t.Helper()
	files := make(map[string]os.FileInfo)
	for _, folder := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		entries, err := os.ReadDir(folder)
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			require.NoError(t, err)
			files[filepath.Join(folder, entry.Name())] = info
		}
	}
	return files
}

func requireSnapshotFilesUnchanged(t *testing.T, files map[string]os.FileInfo) {
	t.Helper()
	for path, before := range files {
		after, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, os.SameFile(before, after), "recovery must reuse completed snapshot files: %s", path)
	}
}

func removeSnapshotFiles(t *testing.T, pattern string) []string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no snapshot files match %s", pattern)
	for _, path := range paths {
		require.NoError(t, dir.RemoveFile(path))
	}
	return paths
}

func TestAggregatorBuildRecoversUnindexedFiles(t *testing.T) {
	const stepSize = uint64(10)
	accessorExt := "kvi"
	if statecfg.Schema.CommitmentDomain.Accessors.Has(statecfg.AccessorBTree) {
		accessorExt = "bt"
	}
	type testCase struct {
		name, pattern string
		accessorDir   bool
	}
	tests := []testCase{
		{name: "commitment", pattern: "*-commitment.[23]-[34]." + accessorExt},
		{name: "logaddrs", pattern: "*-logaddrs.[23]-[34].efi", accessorDir: true},
	}
	if statecfg.Schema.CommitmentDomain.Accessors.Has(statecfg.AccessorExistence) {
		tests = append(tests, testCase{name: "commitment_filter", pattern: "*-commitment.[23]-[34].kvei"})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, stepSize)
			ctx := t.Context()
			lastTxNum := 6*stepSize + 1
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				for _, domain := range []kv.Domain{kv.AccountsDomain, kv.CommitmentDomain} {
					writer := at.d[domain].NewWriter(db)
					t.Cleanup(writer.Close)
					for txNum := uint64(1); txNum <= lastTxNum; txNum += stepSize {
						require.NoError(t, writer.PutWithPrev([]byte{byte(txNum)}, []byte{7}, txNum, nil))
					}
					require.NoError(t, writer.Flush(ctx, tx))
					writer.Close()
				}
				writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
				defer writer.close()
				for txNum := uint64(1); txNum <= lastTxNum; txNum += stepSize {
					require.NoError(t, writer.Add([]byte{1}, txNum))
				}
				return writer.Flush(ctx, tx)
			}))
			for step := range kv.Step(4) {
				require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
			}
			dirs := agg.Dirs()
			agg.Close()
			folder := dirs.SnapDomain
			if test.accessorDir {
				folder = dirs.SnapAccessors
			}
			require.Len(t, removeSnapshotFiles(t, filepath.Join(folder, test.pattern)), 2)
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()

			existing := snapshotFileIdentities(t, dirs)
			finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
			require.True(t, started)
			<-finished
			requireVisibleEnd(t, agg, 6*stepSize)
			requireSnapshotFilesUnchanged(t, existing)
		})
	}
}

func TestAggregatorEnableDomainDuringFilePlanning(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var writers sync.WaitGroup
		writers.Go(func() {
			for range 100 {
				agg.EnableDomain(kv.RCacheDomain)
			}
		})
		defer writers.Wait()
		for range 100 {
			plan, err := agg.planFileBuild(tx)
			require.NoError(t, err)
			require.Zero(t, plan.firstStep)
		}
		return nil
	}))
}

func TestAggregatorBuildDefersNewlyEnabledDomain(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	ctx := t.Context()
	key, value := []byte{1}, []byte{7}
	agg.EnableDomain(kv.RCacheDomain)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		for _, domain := range []kv.Domain{kv.AccountsDomain, kv.RCacheDomain} {
			writer := at.d[domain].NewWriter(db)
			t.Cleanup(writer.Close)
			require.NoError(t, writer.PutWithPrev(key, value, 1, nil))
			if domain == kv.AccountsDomain {
				require.NoError(t, writer.PutWithPrev([]byte{2}, value, 3*stepSize, nil))
				require.NoError(t, writer.PutWithPrev([]byte{3}, value, 5*stepSize, nil))
			}
			require.NoError(t, writer.Flush(ctx, tx))
			writer.Close()
		}
		return nil
	}))

	// Disabled entities can still have MDBX data from an earlier run.
	agg.dirtyFilesLock.Lock()
	agg.d[kv.RCacheDomain].Enabled = false
	agg.recalcVisibleFiles(nil)
	agg.dirtyFilesLock.Unlock()
	for step := range kv.Step(3) {
		require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
	}
	plan, err := agg.readFileBuildPlan(ctx, db)
	require.NoError(t, err)
	require.Equal(t, kv.Step(3), plan.firstStep)
	agg.EnableDomain(kv.RCacheDomain)

	assertPendingData := func(t *testing.T) {
		t.Helper()
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			at := agg.BeginFilesRo()
			defer at.Close()
			got, _, found, err := at.d[kv.RCacheDomain].GetLatest(key, tx)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, got)
			return nil
		}))
	}
	t.Run("before_build", assertPendingData)
	for step := plan.firstStep; step < 5; step++ {
		require.NoError(t, agg.buildFilesWithPlan(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil, plan))
	}
	require.Equal(t, 5*stepSize, agg.EndTxNumMinimax(), "healthy state files must keep advancing")
	t.Run("after_build", assertPendingData)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		_, err := at.prune(ctx, tx, 0, true, nil)
		return err
	}))
	t.Run("after_prune", assertPendingData)
}

type buildPlanningTx struct {
	kv.Tx
	table     string
	scans     int
	nextCalls int
}

func (tx *buildPlanningTx) Cursor(table string) (kv.Cursor, error) {
	cursor, err := tx.Tx.Cursor(table) //nolint:gocritic // The caller owns the returned cursor.
	if err != nil || table != tx.table {
		return cursor, err
	}
	tx.scans++
	return &buildPlanningCursor{Cursor: cursor, tx: tx}, nil
}

type buildPlanningCursor struct {
	kv.Cursor
	tx *buildPlanningTx
}

func (c *buildPlanningCursor) Next() ([]byte, []byte, error) {
	c.tx.nextCalls++
	return c.Cursor.Next()
}

func TestAggregatorBuildResumesIncompleteStep(t *testing.T) {
	const stepSize = uint64(10)
	for _, target := range []string{"receipt", "receipt_without_history", "rcache", "logaddrs"} {
		targetDomain := kv.ReceiptDomain
		if target == "rcache" {
			targetDomain = kv.RCacheDomain
		}
		isIndex := target == "logaddrs"
		withoutHistory := target == "receipt_without_history"
		for _, test := range []struct {
			name                   string
			resumeStep, stateSteps kv.Step
			explicit               bool
			fromStep               kv.Step
			unaligned              bool
		}{
			{name: "resume_from_step_0", resumeStep: 0, stateSteps: 1},
			{name: "resume_from_step_1", resumeStep: 1, stateSteps: 2},
			{name: "defer_two_steps_behind", resumeStep: 0, stateSteps: 2},
			{name: "defer_from_step_0", resumeStep: 0, stateSteps: 3},
			{name: "defer_from_step_1", resumeStep: 1, stateSteps: 4},
			{name: "explicit_resume_before_requested_range", resumeStep: 1, stateSteps: 2, explicit: true, fromStep: 2},
			{name: "explicit_resume_after_requested_start", resumeStep: 2, stateSteps: 3, explicit: true},
			{name: "explicit_defer_from_tip", resumeStep: 0, stateSteps: 2, explicit: true, fromStep: 2},
			{name: "explicit_defer_below_tip", resumeStep: 0, stateSteps: 2, explicit: true},
			{name: "explicit_unaligned_backfill", resumeStep: 0, stateSteps: 3, explicit: true, fromStep: 3, unaligned: true},
			{name: "explicit_unaligned_leading_empty_steps", resumeStep: 2, stateSteps: 3, explicit: true, unaligned: true},
		} {
			resumeStep := test.resumeStep
			t.Run(target+"/"+test.name, func(t *testing.T) {
				deferred := test.stateSteps > resumeStep+1 && !test.unaligned
				db, agg := testDbAndAggregatorv3(t, stepSize)
				ctx := t.Context()
				configure := func(agg *Aggregator) {
					agg.EnableDomain(kv.RCacheDomain)
					agg.d[kv.ReceiptDomain].HistoryDisabled = withoutHistory
					agg.KeepRecentTxnsOfHistoriesWithDisabledSnapshots(0)
					agg.ForbidVisibilityLowering()
				}
				configure(agg)
				stateFilesEnd := uint64(test.stateSteps) * stepSize
				lastTxNum := stateFilesEnd + stepSize
				key, value := []byte{1}, []byte{0}
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					for _, domain := range []kv.Domain{kv.AccountsDomain, kv.ReceiptDomain, kv.RCacheDomain} {
						writer := at.d[domain].NewWriter(db)
						t.Cleanup(writer.Close)
						require.NoError(t, writer.PutWithPrev(key, value, uint64(resumeStep)*stepSize+1, nil))
						if test.unaligned {
							// Key order must not let the resume skip an older pending step.
							require.NoError(t, writer.PutWithPrev([]byte{0}, value, uint64(resumeStep+1)*stepSize+1, nil))
						}
						if domain == kv.AccountsDomain || domain == kv.RCacheDomain {
							require.NoError(t, writer.PutWithPrev([]byte{2}, value, stateFilesEnd, nil))
							require.NoError(t, writer.PutWithPrev([]byte{3}, value, lastTxNum, nil))
						}
						require.NoError(t, writer.Flush(ctx, tx))
						writer.Close()
					}
					writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
					defer writer.close()
					require.NoError(t, writer.Add(key, uint64(resumeStep)*stepSize+1))
					require.NoError(t, writer.Flush(ctx, tx))
					return nil
				}))
				for step := kv.Step(0); step < test.stateSteps; step++ {
					require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
				}

				logEvery := time.NewTicker(time.Second)
				defer logEvery.Stop()
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					_, err := at.d[kv.AccountsDomain].Prune(ctx, tx, resumeStep, 0, stateFilesEnd, 0, logEvery)
					require.NoError(t, err)
					require.Equal(t, uint64(test.stateSteps), agg.d[kv.AccountsDomain].minStepInDB(tx))
					_, err = at.d[kv.RCacheDomain].ht.Prune(ctx, tx, 0, stateFilesEnd, 0, false, logEvery)
					require.NoError(t, err)
					return nil
				}))

				dirs := agg.Dirs()
				agg.Close()
				folder, filename := dirs.SnapDomain, targetDomain.String()
				if isIndex {
					folder, filename = dirs.SnapIdx, kv.FileLogAddressIdx
				}
				removeSnapshotFiles(t, filepath.Join(folder, "*-"+filename+".*"))
				agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
				configure(agg)
				t.Cleanup(agg.Close)
				require.NoError(t, agg.OpenFolder(db))
				var realign func()
				if test.unaligned {
					if isIndex {
						realign = agg.UnalignIdx(kv.LogAddrIdx)
					} else {
						realign = agg.Unalign(targetDomain)
					}
				}
				if !isIndex && !withoutHistory && targetDomain == kv.ReceiptDomain {
					require.NotZero(t, agg.d[kv.ReceiptDomain].History.dirtyFiles.Len(), "incomplete history must remain available for recovery")
					require.NotZero(t, agg.d[kv.ReceiptDomain].History.InvertedIndex.dirtyFiles.Len())
				}
				require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.NoError(t, at.CheckFilesDBGap(tx))
					require.Equal(t, stateFilesEnd, at.TxNumsInFiles(kv.StateDomains...))
					if test.explicit {
						return nil
					}
					counted := &buildPlanningTx{Tx: tx}
					if !isIndex {
						counted.table = agg.d[targetDomain].ValuesTable
					}
					plan, err := agg.planFileBuild(counted)
					require.NoError(t, err)
					if deferred {
						require.Equal(t, test.stateSteps, plan.firstStep, "an unsafe backfill must not block healthy entities")
						if withoutHistory || targetDomain == kv.RCacheDomain {
							require.Equal(t, 1, counted.scans)
							require.Zero(t, counted.nextCalls, "the first row already requires deferring this entity")
						}
					} else {
						require.Equal(t, resumeStep, plan.firstStep)
					}
					return nil
				}))
				existing := snapshotFileIdentities(t, dirs)
				if test.explicit {
					require.NoError(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), test.fromStep, kv.Step(lastTxNum/stepSize), unboundedFinalityCtx, false))
					agg.WaitForFiles()
				} else {
					finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
					require.True(t, started)
					<-finished
				}
				requireSnapshotFilesUnchanged(t, existing)
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.StateDomains...), "healthy snapshots must keep advancing")
					if !deferred {
						files := at.d[targetDomain].files
						if isIndex {
							files = at.searchII(kv.LogAddrIdx).files
						}
						require.Equal(t, lastTxNum, files.EndTxNum(), "%s files must catch up", target)
						if test.unaligned {
							require.NotEmpty(t, files)
							require.Zero(t, files[0].startTxNum, "the rebuilt range must include its leading empty steps")
						}
					}
					_, err := at.prune(ctx, tx, 0, true, nil)
					require.NoError(t, err)
					if deferred {
						got, _, found, readErr := at.d[kv.AccountsDomain].GetLatest(key, tx)
						require.NoError(t, readErr)
						require.True(t, found)
						require.Equal(t, value, got, "rejected resume must preserve filed state")
					}
					for _, domain := range []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain} {
						got, _, found, readErr := at.d[domain].GetLatest(key, tx)
						require.NoError(t, readErr)
						require.True(t, found, "pruning must preserve %s values from the incomplete step", domain)
						require.Equal(t, value, got)
					}
					entries, err := at.IndexRange(kv.LogAddrIdx, key, 0, int(lastTxNum), order.Asc, -1, tx)
					require.NoError(t, err)
					defer entries.Close()
					txNums, err := stream.ToArray(entries)
					require.NoError(t, err)
					require.Equal(t, []uint64{uint64(resumeStep)*stepSize + 1}, txNums)
					if deferred {
						return at.CheckFilesDBGap(tx)
					}
					return nil
				}))
				if realign != nil {
					realign()
				}
			})
		}
	}
}

func TestAggregatorBuildKeepsAlignedEntitiesAheadOfBackfill(t *testing.T) {
	const stepSize = uint64(10)
	for _, test := range []struct {
		name    string
		pending bool
	}{
		{name: "pending", pending: true},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, stepSize)
			ctx := t.Context()
			agg.EnableDomain(kv.RCacheDomain)
			generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 3}})
			generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 3}})
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()
			realign := agg.Unalign(kv.ReceiptDomain)
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				for _, domain := range []kv.Domain{kv.AccountsDomain, kv.ReceiptDomain, kv.RCacheDomain} {
					txNums := []uint64{1}
					switch domain {
					case kv.AccountsDomain:
						txNums = []uint64{3 * stepSize, 4 * stepSize}
					case kv.RCacheDomain:
						if !test.pending {
							continue
						}
						txNums = []uint64{2*stepSize + 1}
					}
					writer := at.d[domain].NewWriter(db)
					t.Cleanup(writer.Close)
					for _, txNum := range txNums {
						require.NoError(t, writer.PutWithPrev([]byte{byte(txNum)}, []byte{7}, txNum, nil))
					}
					require.NoError(t, writer.Flush(ctx, tx))
					writer.Close()
				}
				if test.pending {
					writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
					defer writer.close()
					require.NoError(t, writer.Add([]byte{1}, 2*stepSize+1))
					return writer.Flush(ctx, tx)
				}
				return nil
			}))

			finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), 4*stepSize, false, unboundedFinalityCtx)
			require.True(t, started)
			<-finished
			requireVisibleEnd(t, agg, 4*stepSize)
			at := agg.BeginFilesRo()
			defer at.Close()
			for _, files := range []visibleFiles{at.d[kv.RCacheDomain].files, at.searchII(kv.LogAddrIdx).files} {
				require.NotEmpty(t, files)
				require.GreaterOrEqual(t, files[0].startTxNum, 2*stepSize, "aligned entities must not start at the backfill's earlier step")
				require.Equal(t, 4*stepSize, files.EndTxNum())
			}
			require.NotEmpty(t, at.d[kv.ReceiptDomain].files)
			require.Zero(t, at.d[kv.ReceiptDomain].files[0].startTxNum, "unaligned backfill must still start at its pending step")
			realign()
		})
	}
}

func TestAggregatorBuildRespectsSharedCeiling(t *testing.T) {
	const stepSize = uint64(10)
	accessorExt := "kvi"
	if statecfg.Schema.CommitmentDomain.Accessors.Has(statecfg.AccessorBTree) {
		accessorExt = "bt"
	}
	for _, test := range []struct {
		name, accessors   string
		visibleStateSteps uint64
	}{
		{name: "missing_last_accessor", accessors: "*-commitment.2-3." + accessorExt, visibleStateSteps: 2},
		{name: "missing_all_accessors", accessors: "*-commitment.*." + accessorExt, visibleStateSteps: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, stepSize)
			ctx := t.Context()
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				for _, domain := range []kv.Domain{kv.AccountsDomain, kv.CommitmentDomain} {
					writer := at.d[domain].NewWriter(db)
					t.Cleanup(writer.Close)
					for txNum := uint64(1); txNum <= 41; txNum += stepSize {
						require.NoError(t, writer.PutWithPrev([]byte{byte(txNum)}, []byte{7}, txNum, nil))
					}
					require.NoError(t, writer.Flush(ctx, tx))
					writer.Close()
				}
				writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
				defer writer.close()
				require.NoError(t, writer.Add([]byte{1}, stepSize+1))
				return writer.Flush(ctx, tx)
			}))
			for step := range kv.Step(3) {
				require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
			}
			dirs := agg.Dirs()
			agg.Close()
			for _, pattern := range []string{
				filepath.Join(dirs.SnapIdx, "*-"+kv.FileLogAddressIdx+".*"),
				filepath.Join(dirs.SnapAccessors, "*-"+kv.FileLogAddressIdx+".*"),
				filepath.Join(dirs.SnapDomain, test.accessors),
			} {
				removeSnapshotFiles(t, pattern)
			}
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				// Missing accessors cut every readable list, but recovery must still
				// respect the shared ceiling of the files on disk.
				require.Equal(t, test.visibleStateSteps*stepSize, at.TxNumsInFiles(kv.StateDomains...))
				plan, err := agg.planFileBuild(tx)
				require.NoError(t, err)
				require.Equal(t, kv.Step(test.visibleStateSteps), plan.firstStep)
				for id, ii := range at.standaloneIIs() {
					if ii.ii.Name == kv.LogAddrIdx {
						require.True(t, plan.deferredIndices[id], "publishing step 1 would lower the shared ceiling")
					}
				}
				return nil
			}))
		})
	}
}
