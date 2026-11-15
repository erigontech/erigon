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
)

// Fixture construction chooses individual steps without recovery planning.
func (a *Aggregator) buildFiles(ctx context.Context, db kv.TemporalRoDB, step kv.Step, finalityCtx kv.FinalityContext, filesDone <-chan struct{}) error {
	return a.buildFilesWithPlan(ctx, db, step, finalityCtx, filesDone, fileBuildPlan{})
}

func TestAggregatorEnableDomainDuringFilePlanning(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		var writers sync.WaitGroup
		writers.Go(func() {
			for range 100 {
				agg.EnableDomain(kv.RCacheDomain)
			}
		})
		defer writers.Wait()
		for range 100 {
			plan, err := at.planFileBuild(tx)
			require.NoError(t, err)
			require.Zero(t, plan.firstStep)
		}
		return nil
	}))
}

func TestAggregatorBuildDefersNewlyEnabledEntities(t *testing.T) {
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
		writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
		defer writer.close()
		require.NoError(t, writer.Add(key, 1))
		return writer.Flush(ctx, tx)
	}))

	// Disabled entities can still have MDBX data from an earlier run.
	agg.d[kv.RCacheDomain].Enabled.Store(false)
	logAddrs := agg.searchII(kv.LogAddrIdx)
	logAddrs.Enabled.Store(false)
	for step := range kv.Step(3) {
		require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
	}
	var plan fileBuildPlan
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		var err error
		plan, err = at.planFileBuild(tx)
		return err
	}))
	require.Equal(t, kv.Step(3), plan.firstStep)
	agg.EnableDomain(kv.RCacheDomain)
	logAddrs.Enabled.Store(true)

	assertPendingData := func(t *testing.T) {
		t.Helper()
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			at := agg.BeginFilesRo()
			defer at.Close()
			t.Run("domain", func(t *testing.T) {
				got, _, found, err := at.d[kv.RCacheDomain].GetLatest(key, tx)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, value, got)
			})
			t.Run("index", func(t *testing.T) {
				entries, err := at.IndexRange(kv.LogAddrIdx, key, 0, int(5*stepSize), order.Asc, -1, tx)
				require.NoError(t, err)
				defer entries.Close()
				txNums, err := stream.ToArray(entries)
				require.NoError(t, err)
				require.Equal(t, []uint64{1}, txNums)
			})
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
		} {
			resumeStep := test.resumeStep
			t.Run(target+"/"+test.name, func(t *testing.T) {
				deferred := test.stateSteps > resumeStep+1 && !test.unaligned
				db, agg := testDbAndAggregatorv3(t, stepSize)
				ctx := t.Context()
				configure := func(agg *Aggregator) {
					agg.EnableDomain(kv.RCacheDomain)
					agg.d[kv.ReceiptDomain].HistoryDisabled = target == "receipt_without_history"
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
							require.NoError(t, writer.PutWithPrev([]byte{0}, value, stepSize+1, nil))
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
				folder, filename := dirs.SnapDomain, kv.ReceiptDomain.String()
				if target == "rcache" {
					filename = kv.RCacheDomain.String()
				}
				if target == "logaddrs" {
					folder, filename = dirs.SnapIdx, kv.FileLogAddressIdx
				}
				paths, err := filepath.Glob(filepath.Join(folder, "*-"+filename+".*"))
				require.NoError(t, err)
				require.NotEmpty(t, paths)
				for _, path := range paths {
					require.NoError(t, dir.RemoveFile(path))
				}
				agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
				configure(agg)
				t.Cleanup(agg.Close)
				require.NoError(t, agg.OpenFolder(db))
				var realign func()
				if test.unaligned {
					switch target {
					case "rcache":
						realign = agg.Unalign(kv.RCacheDomain)
					case "logaddrs":
						realign = agg.UnalignIdx(kv.LogAddrIdx)
					default:
						realign = agg.Unalign(kv.ReceiptDomain)
					}
				}
				if filename == kv.ReceiptDomain.String() {
					require.Zero(t, agg.d[kv.ReceiptDomain].History.dirtyFiles.Len(), "incomplete receipt history must be closed before rebuilding")
					require.Zero(t, agg.d[kv.ReceiptDomain].History.InvertedIndex.dirtyFiles.Len(), "incomplete receipt indices must be closed before rebuilding")
				}
				require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.NoError(t, at.CheckFilesDBGap(tx))
					require.Equal(t, stateFilesEnd, at.TxNumsInFiles(kv.StateDomains...))
					if test.explicit {
						return nil
					}
					counted := &buildPlanningTx{Tx: tx, table: agg.d[kv.ReceiptDomain].ValuesTable}
					if target == "rcache" {
						counted.table = agg.d[kv.RCacheDomain].ValuesTable
					}
					plan, err := at.planFileBuild(counted)
					require.NoError(t, err)
					if deferred {
						require.Equal(t, test.stateSteps, plan.firstStep, "an unsafe backfill must not block healthy entities")
						if target == "receipt_without_history" || target == "rcache" {
							require.Equal(t, 1, counted.scans)
							require.Zero(t, counted.nextCalls, "the first row already requires deferring this entity")
						}
					} else {
						require.Equal(t, resumeStep, plan.firstStep)
					}
					return nil
				}))
				if test.explicit {
					require.NoError(t, agg.BuildFiles2(ctx, asTemporalRoDB(db), test.fromStep, kv.Step(lastTxNum/stepSize), unboundedFinalityCtx, false))
					agg.WaitForFiles()
				} else {
					finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
					require.True(t, started)
					<-finished
				}
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.StateDomains...), "healthy snapshots must keep advancing")
					if !deferred {
						switch target {
						case "rcache":
							require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.RCacheDomain), "rcache files must catch up")
						case "logaddrs":
							require.Equal(t, lastTxNum, at.searchII(kv.LogAddrIdx).files.EndTxNum(), "log address files must catch up")
						default:
							require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.ReceiptDomain), "receipt files must catch up")
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
	for _, test := range []struct {
		name, accessors   string
		visibleStateSteps uint64
	}{
		{name: "missing_last_accessor", accessors: "*-commitment.2-3.kvi", visibleStateSteps: 2},
		{name: "missing_all_accessors", accessors: "*-commitment.*.kvi", visibleStateSteps: 0},
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
				paths, err := filepath.Glob(pattern)
				require.NoError(t, err)
				require.NotEmpty(t, paths)
				for _, path := range paths {
					require.NoError(t, dir.RemoveFile(path))
				}
			}
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			agg.ForbidVisibilityLowering()
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				// An unindexed state file lowers the state minimum, but other entities
				// remain visible up to the shared ceiling.
				require.Equal(t, test.visibleStateSteps*stepSize, at.TxNumsInFiles(kv.StateDomains...))
				require.Equal(t, 3*stepSize, at.TxNumsInFiles(kv.CodeDomain))
				plan, err := at.planFileBuild(tx)
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

func TestAggregatorBuildRetiresReplacedFiles(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	ctx := t.Context()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		for _, domain := range []kv.Domain{kv.AccountsDomain, kv.ReceiptDomain} {
			writer := at.d[domain].NewWriter(db)
			t.Cleanup(writer.Close)
			for _, txNum := range []uint64{1, stepSize + 1} {
				require.NoError(t, writer.PutWithPrev([]byte{byte(txNum)}, []byte{7}, txNum, nil))
			}
			require.NoError(t, writer.Flush(ctx, tx))
			writer.Close()
		}
		writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
		defer writer.close()
		require.NoError(t, writer.Add([]byte{1}, 1))
		return writer.Flush(ctx, tx)
	}))
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, nil))
	reader := agg.BeginFilesRo()
	defer reader.Close()
	domain := agg.d[kv.ReceiptDomain]
	files := []*DirtyFiles{domain.dirtyFiles, domain.History.dirtyFiles, domain.InvertedIndex.dirtyFiles, agg.searchII(kv.LogAddrIdx).dirtyFiles}
	var replaced []*FilesItem
	for _, dirty := range files {
		require.Equal(t, 1, dirty.Len())
		old := dirty.Items()[0]
		t.Cleanup(old.closeFiles)
		require.NotNil(t, old.decompressor)
		replaced = append(replaced, old)
	}

	// Build elsewhere so this checks publication without overwriting mapped files.
	builder := NewTest(datadir.New(t.TempDir())).StepSize(stepSize).MustOpen(ctx)
	defer builder.Close()
	static := &AggV3StaticFiles{}
	defer static.CleanupOnError()
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		domain := builder.d[kv.ReceiptDomain]
		collation, err := domain.collate(ctx, 0, 0, stepSize, tx)
		require.NoError(t, err)
		defer collation.Close()
		static.d[kv.ReceiptDomain], err = domain.buildFiles(ctx, 0, collation, builder.backgroundProgress)
		require.NoError(t, err)
		for id, ii := range builder.standaloneIIs() {
			if ii.Name != kv.LogAddrIdx {
				continue
			}
			indexCollation, err := ii.collate(ctx, 0, tx)
			require.NoError(t, err)
			static.ivfs[id], err = ii.buildFiles(ctx, 0, indexCollation, builder.backgroundProgress)
			indexCollation.Close()
			require.NoError(t, err)
		}
		return nil
	}))
	agg.IntegrateDirtyFiles(static, 0, stepSize)
	for i, old := range replaced {
		require.NotSame(t, old, files[i].Items()[0])
		require.NotNil(t, old.decompressor, "a pinned reader must keep its mappings")
		require.True(t, old.decompressor.IsOpen())
	}
	reader.Close()
	for i, old := range replaced {
		require.Nil(t, old.decompressor, "replaced mappings must close with their last reader")
		require.Nil(t, old.index)
		require.Nil(t, old.vAccessor)
		replacement := files[i].Items()[0]
		require.True(t, replacement.decompressor.IsOpen())
		require.FileExists(t, replacement.decompressor.FilePath())
	}
}
