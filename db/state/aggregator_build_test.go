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
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

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

func TestAggregatorBuildResumesIncompleteStep(t *testing.T) {
	const stepSize = uint64(10)
	for _, target := range []string{"receipt", "receipt_without_history", "rcache", "logaddrs"} {
		for _, test := range []struct{ resumeStep, stateSteps kv.Step }{{0, 1}, {1, 2}, {0, 3}, {1, 4}} {
			resumeStep := test.resumeStep
			t.Run(fmt.Sprintf("%s/step_%d/state_%d", target, resumeStep, test.stateSteps), func(t *testing.T) {
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
				if filename == kv.ReceiptDomain.String() {
					require.Zero(t, agg.d[kv.ReceiptDomain].History.dirtyFiles.Len(), "incomplete receipt history must be closed before rebuilding")
					require.Zero(t, agg.d[kv.ReceiptDomain].History.InvertedIndex.dirtyFiles.Len(), "incomplete receipt indices must be closed before rebuilding")
				}
				require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.NoError(t, at.CheckFilesDBGap(tx))
					require.Equal(t, stateFilesEnd, at.TxNumsInFiles(kv.StateDomains...))
					plan, err := at.planFileBuild(tx)
					require.NoError(t, err)
					if test.stateSteps > resumeStep+1 {
						require.Equal(t, test.stateSteps, plan.firstStep, "an unsafe backfill must not block healthy entities")
					} else {
						require.Equal(t, resumeStep, plan.firstStep)
					}
					return nil
				}))
				finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
				require.True(t, started)
				<-finished
				if test.stateSteps > resumeStep+1 {
					require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
						at := agg.BeginFilesRo()
						defer at.Close()
						require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.StateDomains...), "healthy snapshots must keep advancing")
						_, err := at.prune(ctx, tx, 0, true, nil)
						require.NoError(t, err)
						got, _, found, err := at.d[kv.AccountsDomain].GetLatest(key, tx)
						require.NoError(t, err)
						require.True(t, found)
						require.Equal(t, value, got, "rejected resume must preserve filed state")
						for _, domain := range []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain} {
							got, _, found, readErr := at.d[domain].GetLatest(key, tx)
							require.NoError(t, readErr)
							require.True(t, found)
							require.Equal(t, value, got, "retain pending data for %s", domain)
						}
						entries, err := at.IndexRange(kv.LogAddrIdx, key, 0, int(lastTxNum), order.Asc, -1, tx)
						require.NoError(t, err)
						defer entries.Close()
						txNums, err := stream.ToArray(entries)
						require.NoError(t, err)
						require.Equal(t, []uint64{uint64(resumeStep)*stepSize + 1}, txNums)
						return at.CheckFilesDBGap(tx)
					}))
					return
				}
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.ReceiptDomain), "receipt files must catch up")
					require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.StateDomains...), "state snapshot production must continue")
					_, err := at.prune(ctx, tx, 0, true, nil)
					require.NoError(t, err)
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
					return nil
				}))
			})
		}
	}
}
