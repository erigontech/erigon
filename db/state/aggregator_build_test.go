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
			step, err := at.firstStepToBuild(tx)
			require.NoError(t, err)
			require.Zero(t, step)
		}
		return nil
	}))
}

func TestAggregatorBuildResumesIncompleteStep(t *testing.T) {
	const stepSize = uint64(10)
	for _, resumeStep := range []kv.Step{0, 1} {
		t.Run(fmt.Sprintf("step_%d", resumeStep), func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, stepSize)
			ctx := t.Context()
			stateFilesEnd := uint64(resumeStep+1) * stepSize
			lastTxNum := stateFilesEnd + stepSize
			key, value := []byte{1}, []byte{0}
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				for _, domain := range []kv.Domain{kv.AccountsDomain, kv.ReceiptDomain} {
					writer := at.d[domain].NewWriter(db)
					t.Cleanup(writer.Close)
					require.NoError(t, writer.PutWithPrev(key, value, uint64(resumeStep)*stepSize+1, nil))
					if domain == kv.AccountsDomain {
						require.NoError(t, writer.PutWithPrev([]byte{2}, value, stateFilesEnd, nil))
						require.NoError(t, writer.PutWithPrev([]byte{3}, value, lastTxNum, nil))
					}
					require.NoError(t, writer.Flush(ctx, tx))
					writer.Close()
				}
				return nil
			}))
			for step := kv.Step(0); step <= resumeStep; step++ {
				require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), step, unboundedFinalityCtx, nil))
			}

			logEvery := time.NewTicker(time.Second)
			defer logEvery.Stop()
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				_, err := at.d[kv.AccountsDomain].Prune(ctx, tx, resumeStep, 0, stateFilesEnd, 0, logEvery)
				require.NoError(t, err)
				require.Equal(t, uint64(resumeStep+1), agg.d[kv.AccountsDomain].minStepInDB(tx))
				return nil
			}))

			dirs := agg.Dirs()
			agg.Close()
			// A cancelled build can finish the state domains before receipt values.
			paths, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-receipt.*"))
			require.NoError(t, err)
			require.NotEmpty(t, paths)
			for _, path := range paths {
				require.NoError(t, dir.RemoveFile(path))
			}
			agg = NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
			t.Cleanup(agg.Close)
			require.NoError(t, agg.OpenFolder(db))
			require.Zero(t, agg.d[kv.ReceiptDomain].History.dirtyFiles.Len(), "incomplete receipt history must be closed before rebuilding")
			require.Zero(t, agg.d[kv.ReceiptDomain].History.InvertedIndex.dirtyFiles.Len(), "incomplete receipt indices must be closed before rebuilding")
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				require.NoError(t, at.CheckFilesDBGap(tx))
				require.Equal(t, stateFilesEnd, at.TxNumsInFiles(kv.StateDomains...))
				require.Zero(t, at.TxNumsInFiles(kv.ReceiptDomain))
				step, err := at.firstStepToBuild(tx)
				require.NoError(t, err)
				require.Equal(t, resumeStep, step)
				return nil
			}))
			finished, started := agg.buildFilesInBackground(asTemporalRoDB(db), lastTxNum, false, unboundedFinalityCtx)
			require.True(t, started)
			<-finished
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.ReceiptDomain), "receipt files must catch up")
				require.Equal(t, lastTxNum, at.TxNumsInFiles(kv.StateDomains...), "state snapshot production must continue")
				_, err := at.prune(ctx, tx, 0, true, nil)
				require.NoError(t, err)
				got, _, found, err := at.d[kv.ReceiptDomain].GetLatest(key, tx)
				require.NoError(t, err)
				require.True(t, found, "pruning must preserve values from the incomplete step")
				require.Equal(t, value, got)
				return nil
			}))
		})
	}
}
