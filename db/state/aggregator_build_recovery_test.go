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
	"fmt"
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
	"github.com/erigontech/erigon/db/version"
)

// Fixture construction chooses individual steps without recovery planning.
func (a *Aggregator) buildFiles(ctx context.Context, db kv.TemporalRoDB, step kv.Step, finalityCtx kv.FinalityContext, filesDone <-chan struct{}) error {
	return a.buildFilesWithPlan(ctx, db, step, finalityCtx, filesDone, &fileBuildPlan{})
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
			paths := removeSnapshotFiles(t, filepath.Join(folder, test.pattern))
			require.Len(t, paths, 2)
			// One accessor is missing and the other has an incomplete header.
			var header []byte
			if filepath.Ext(paths[1]) == ".kvei" {
				// A zero-length existence filter is valid for a small data file.
				header = []byte{1}
			}
			require.NoError(t, os.WriteFile(paths[1], header, 0o600))
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()

			existing := snapshotFileIdentities(t, dirs)
			delete(existing, paths[1])
			finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
			require.True(t, started)
			<-finished
			requireVisibleEnd(t, agg, 6*stepSize)
			requireSnapshotFilesUnchanged(t, existing)
		})
	}
}

func buildSnapshotRecoveryStep(t *testing.T, db kv.RwDB, agg *Aggregator, key, firstValue []byte) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		w := at.d[kv.ReceiptDomain].NewWriter(db)
		defer w.Close()
		require.NoError(t, w.PutWithPrev(key, firstValue, 1, nil))
		require.NoError(t, w.PutWithPrev(key, []byte{8}, 2, firstValue))
		require.NoError(t, w.Flush(ctx, tx))
		iw := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
		defer iw.close()
		require.NoError(t, iw.Add(key, 1))
		return iw.Flush(ctx, tx)
	}))
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, nil))
}

func nextMinorSnapshotPath(t *testing.T, path string) string {
	t.Helper()
	base := filepath.Base(path)
	current, err := version.ParseVersion(base)
	require.NoError(t, err)
	return filepath.Join(filepath.Dir(path), version.ReplaceVersion(base, current, current.BumpMinor()))
}

func TestAggregatorRepairsUnreadableHistoryAccessor(t *testing.T) {
	const stepSize = uint64(10)
	for _, build := range []string{"index", "step"} {
		for _, variant := range []string{"current", "newer_minor"} {
			t.Run(build+"/"+variant, func(t *testing.T) {
				ctx := t.Context()
				db, agg := testDbAndAggregatorv3(t, stepSize)
				key, value := []byte{1}, []byte{7}
				buildSnapshotRecoveryStep(t, db, agg, key, value)
				dirs := agg.Dirs()
				accessor := agg.d[kv.ReceiptDomain].History.vAccessorNewFilePath(0, 1)
				agg.Close()
				damaged := accessor
				if variant == "newer_minor" {
					damaged = nextMinorSnapshotPath(t, accessor)
					require.NoError(t, os.Rename(accessor, damaged))
				}
				require.NoError(t, os.Truncate(damaged, 0))
				agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
				t.Cleanup(agg.Close)
				require.NoError(t, agg.OpenFolder(db))
				existing := snapshotFileIdentities(t, dirs)
				delete(existing, damaged)
				if build == "index" {
					require.NoError(t, agg.BuildMissedAccessors(ctx, db, 1))
				} else {
					require.NoError(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), 0, 1, unboundedFinalityCtx, false))
					agg.background.Wait()
				}
				requireSnapshotFilesUnchanged(t, existing)
				agg.CloseFilesNoReopen()
				require.NoError(t, agg.OpenFolder(db))
				at := agg.BeginFilesRo()
				defer at.Close()
				prev, found, err := at.d[kv.ReceiptDomain].ht.historySeekInFiles(key, 2)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, value, prev)
			})
		}
	}
}

func TestAggregatorBuildRepairsUnreadableStepData(t *testing.T) {
	const stepSize = uint64(10)
	for _, ext := range []string{"kv", "v", "ef", "standalone_ef"} {
		for _, pruned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pruned_%t", ext, pruned), func(t *testing.T) {
				ctx := t.Context()
				db, agg := testDbAndAggregatorv3(t, stepSize)
				key, value := []byte{1}, []byte{7}
				buildSnapshotRecoveryStep(t, db, agg, key, value)
				d := agg.d[kv.ReceiptDomain]
				path := map[string]string{
					"kv":            d.kvNewFilePath(0, 1),
					"v":             d.vNewFilePath(0, 1),
					"ef":            d.efNewFilePath(0, 1),
					"standalone_ef": agg.searchII(kv.LogAddrIdx).efNewFilePath(0, 1),
				}[ext]
				original, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				if pruned {
					require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
						at := agg.BeginFilesRo()
						defer at.Close()
						if _, err := at.prune(ctx, tx, 0, true, nil); err != nil {
							return err
						}
						// A prune pass can leave some rows from this step behind.
						w := at.d[kv.ReceiptDomain].NewWriter(db)
						defer w.Close()
						require.NoError(t, w.PutWithPrev([]byte{9}, value, 3, nil))
						require.NoError(t, w.Flush(ctx, tx))
						iw := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
						defer iw.close()
						require.NoError(t, iw.Add([]byte{9}, 3))
						return iw.Flush(ctx, tx)
					}))
				}
				dirs := agg.Dirs()
				agg.Close()
				require.NoError(t, os.Truncate(path, 1))
				agg = NewTest(dirs).StepSize(stepSize).SkipFilesDBGapCheck().MustOpen(ctx)
				t.Cleanup(agg.Close)
				require.NoError(t, agg.OpenFolder(db))
				buildErr := agg.BuildFiles2(ctx, asTemporalRoDB(db), 0, 1, unboundedFinalityCtx, false)
				agg.background.Wait()
				if pruned {
					require.ErrorContains(t, buildErr, "restore")
					require.ErrorContains(t, buildErr, filepath.Base(path))
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Len(t, data, 1)
					require.NoError(t, dir.RemoveFile(path))
					require.ErrorContains(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), 0, 1, unboundedFinalityCtx, false), filepath.Base(path))
					require.NoFileExists(t, path)
					return
				}
				require.NoError(t, buildErr)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, original, data)
				at := agg.BeginFilesRo()
				defer at.Close()
				prev, found, err := at.d[kv.ReceiptDomain].ht.historySeekInFiles(key, 2)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, value, prev)
			})
		}
	}
}

func TestBuildPreservesUnreadableSnapshot(t *testing.T) {
	const stepSize = uint64(10)
	for _, variant := range []string{"published_data", "published_accessor", "newer_minor", "merged_range"} {
		t.Run(variant, func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			buildSnapshotRecoveryStep(t, db, agg, []byte{1}, []byte{7})
			dirs := agg.Dirs()
			currentPath := agg.d[kv.ReceiptDomain].kvNewFilePath(0, 1)
			btPath := agg.d[kv.ReceiptDomain].kvBtAccessorNewFilePath(0, 1)
			mergedPath := agg.d[kv.ReceiptDomain].kvNewFilePath(0, 2)
			agg.Close()
			damaged := currentPath
			switch variant {
			case "newer_minor":
				damaged = nextMinorSnapshotPath(t, currentPath)
				require.NoError(t, os.Rename(currentPath, damaged))
			case "merged_range":
				damaged = mergedPath
				require.NoError(t, os.Rename(currentPath, damaged))
			case "published_data":
				require.NoError(t, os.WriteFile(damaged+".torrent", nil, 0o600))
			case "published_accessor":
				require.NoError(t, os.WriteFile(btPath+".torrent", nil, 0o600))
			}
			require.NoError(t, os.Truncate(damaged, 1))
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			err := agg.BuildFiles2(ctx, asTemporalRoDB(db), 0, 1, unboundedFinalityCtx, false)
			agg.background.Wait()
			require.ErrorContains(t, err, "restore")
			require.ErrorContains(t, err, filepath.Base(damaged))
			info, err := os.Stat(damaged)
			require.NoError(t, err)
			require.EqualValues(t, 1, info.Size())
			if damaged != currentPath {
				require.NoFileExists(t, currentPath)
			}
		})
	}
}

func TestUnreadableFirstFileRespectsBuildFloor(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 3}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 3}})
	path := agg.d[kv.ReceiptDomain].kvNewFilePath(0, 1)
	require.NoError(t, os.WriteFile(path, []byte{1}, 0o600))
	require.NoError(t, agg.OpenFolder(db))

	plan, err := agg.readFileBuildPlan(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, kv.Step(3), plan.firstStep, "a first file ending below the shared ceiling cannot be published")
	require.NoError(t, agg.BuildFiles2(t.Context(), asTemporalRoDB(db), 0, 1, unboundedFinalityCtx, false))
	agg.background.Wait()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, data)
}

func TestStepDataRepairRemovesOnlyDependentAccessors(t *testing.T) {
	folder := t.TempDir()
	touchFiles(t, folder, "v2.0-receipt.0-1.bt", "v2.1-receipt.0-1.bt", "backup-receipt.0-1.bt", "v2.0-receipt.1-2.bt")
	accessorDir := filepath.Join(folder, "v3.0-receipt.0-1.bt")
	require.NoError(t, os.Mkdir(accessorDir, 0o700))

	require.NoError(t, removeStepAccessors(filepath.Join(folder, "v3.0-receipt.0-1.kv"), folder, []string{".bt"}))
	require.NoFileExists(t, filepath.Join(folder, "v2.0-receipt.0-1.bt"))
	require.NoFileExists(t, filepath.Join(folder, "v2.1-receipt.0-1.bt"))
	require.FileExists(t, filepath.Join(folder, "backup-receipt.0-1.bt"))
	require.FileExists(t, filepath.Join(folder, "v2.0-receipt.1-2.bt"))
	require.DirExists(t, accessorDir)
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
		require.NoError(t, agg.buildFilesWithPlan(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil, &plan))
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

func TestAggregatorBuildPreservesRetainedHistory(t *testing.T) {
	const stepSize = uint64(10)
	stateRanges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	overlapping := []testFileRange{{0, 1}, {0, 4}}
	for _, pending := range []bool{true, false} {
		name := "empty_mdbx"
		if pending {
			name = "pending_step_2"
		}
		for _, test := range []struct {
			name           string
			history, index []testFileRange
			deferred       bool
		}{
			{name: "history_only", history: stateRanges[:1], deferred: true},
			{name: "history_gap", history: stateRanges[:1], index: stateRanges, deferred: true},
			{name: "index_gap", history: stateRanges, index: stateRanges[:1], deferred: true},
			{name: "both_gaps", history: stateRanges[:1], index: stateRanges[:1], deferred: true},
			{name: "different_partitions", history: []testFileRange{{0, 2}, {2, 3}}, index: stateRanges, deferred: true},
			{name: "step_2_completes_index", history: stateRanges, index: stateRanges[:2], deferred: !pending},
			{name: "later_step_would_skip_pending_values", history: overlapping, index: overlapping, deferred: pending},
			{name: "continuous", history: stateRanges, index: stateRanges},
		} {
			t.Run(test.name+"/"+name, func(t *testing.T) {
				db, agg := testDbAndAggregatorv3(t, stepSize)
				dirs := agg.Dirs()
				generateStateFiles(t, dirs, stateRanges)
				generateCommitmentFile(t, dirs, stateRanges)
				generateStandaloneIIFiles(t, dirs, stateRanges)
				receipt := agg.d[kv.ReceiptDomain]
				populateFiles2(t, dirs, NewHistorySnapSchema(receipt.HistCfg, stepSize, dirs), stepSize, test.history)
				populateFiles2(t, dirs, NewIISnapSchema(receipt.InvertedIndex.InvIdxCfg, stepSize, dirs), stepSize, test.index)
				require.NoError(t, agg.OpenFolder(db))
				require.Equal(t, len(test.history), receipt.History.dirtyFiles.Len())
				require.Equal(t, len(test.index), receipt.InvertedIndex.dirtyFiles.Len())
				requireVisibleEnd(t, agg, 3*stepSize)
				agg.ForbidVisibilityLowering()
				key, value := []byte{1}, []byte{7}
				if pending {
					require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
						at := agg.BeginFilesRo()
						defer at.Close()
						writer := at.d[kv.ReceiptDomain].NewWriter(db)
						defer writer.Close()
						require.NoError(t, writer.PutWithPrev(key, value, 2*stepSize+1, nil))
						return writer.Flush(t.Context(), tx)
					}))
				}
				existing := snapshotFileIdentities(t, dirs)
				require.NoError(t, agg.BuildFiles2(t.Context(), asTemporalRoDB(db), 3, 4, unboundedFinalityCtx, false))
				agg.background.Wait()
				require.NoError(t, agg.OpenFolder(db))
				requireVisibleEnd(t, agg, 4*stepSize)
				requireSnapshotFilesUnchanged(t, existing)
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					_, err := at.prune(t.Context(), tx, 0, true, nil)
					return err
				}))
				require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					if pending {
						got, _, found, err := at.d[kv.ReceiptDomain].GetLatest(key, tx)
						require.NoError(t, err)
						require.True(t, found, "the pending receipt must survive pruning")
						require.Equal(t, value, got)
					}
					if test.deferred {
						require.Empty(t, at.d[kv.ReceiptDomain].files)
					} else {
						require.Equal(t, 4*stepSize, at.d[kv.ReceiptDomain].files.EndTxNum())
					}
					return nil
				}))
			})
		}
	}
}

func TestAggregatorBuildKeepsReceiptHistoryReadable(t *testing.T) {
	const stepSize = uint64(10)
	for _, test := range []struct {
		name            string
		hasEarlierFiles bool
		unaligned       bool
	}{
		{name: "first_values_file"},
		{name: "later_values_file", hasEarlierFiles: true},
		{name: "unaligned_rebuild", unaligned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			key, previous := []byte{1}, []byte{7}
			const updateTx = 3*stepSize + 2
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				w := at.d[kv.ReceiptDomain].NewWriter(db)
				defer w.Close()
				require.NoError(t, w.PutWithPrev(key, previous, updateTx-1, nil))
				require.NoError(t, w.PutWithPrev(key, []byte{8}, updateTx, previous))
				return w.Flush(ctx, tx)
			}))
			require.NoError(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), 0, 4, unboundedFinalityCtx, true))
			agg.background.Wait()
			agg.CloseFilesNoReopen()
			dirs, d := agg.Dirs(), agg.d[kv.ReceiptDomain]
			removeSnapshotFiles(t, filepath.Join(dirs.SnapDomain, "*-receipt.*.kv"))
			require.NoError(t, dir.RemoveFile(d.efAccessorNewFilePath(0, 4)))
			if test.hasEarlierFiles {
				prefix := []testFileRange{{0, 2}, {2, 3}}
				generateStateFiles(t, dirs, prefix)
				generateCommitmentFile(t, dirs, prefix)
				generateStandaloneIIFiles(t, dirs, prefix)
				generateDomainFiles(t, kv.ReceiptDomain.String(), dirs, prefix)
			}
			require.NoError(t, agg.OpenFolder(db))
			if test.unaligned {
				defer agg.Unalign(kv.ReceiptDomain)()
			}
			before := agg.BeginFilesRo()
			hasEarlierFiles := before.TxNumsInFiles(kv.ReceiptDomain) > 0
			before.Close()
			require.Equal(t, test.hasEarlierFiles, hasEarlierFiles)

			buildAndRead := func() {
				t.Helper()
				require.NoError(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), 3, 4, unboundedFinalityCtx, false))
				agg.background.Wait()
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					_, err := at.prune(ctx, tx, 0, true, nil)
					require.NoError(t, err)
					got, found, err := at.HistorySeek(kv.ReceiptDomain, key, updateTx, tx)
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, previous, got)
					return nil
				}))
			}
			buildAndRead()
			require.NoFileExists(t, d.kvNewFilePath(3, 4))
			require.NoError(t, agg.BuildMissedAccessors(ctx, db, 1))
			buildAndRead()
			require.FileExists(t, d.kvNewFilePath(3, 4))
		})
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
