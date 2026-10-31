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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
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
)

type finalityContextStub struct {
	finalisedBlockNum uint64
	lastBlockInStep   uint64
	lastBlockInDB     uint64
	lastTxInDB        uint64
	maxReorgDepth     uint64
	ready             bool
}

var unboundedFinalityCtx = finalityContextStub{ready: true}

type temporalRoDBStub struct {
	kv.RoDB
}

func (temporalRoDBStub) OnFilesChange(kv.OnFilesChange, kv.OnFilesChange) {
}

func (temporalRoDBStub) MaxPrunableStepsBacklog() uint64 {
	return 0
}

func (temporalRoDBStub) StepSize() uint64 {
	return 0
}

func (temporalRoDBStub) ViewTemporal(context.Context, func(kv.TemporalTx) error) error {
	panic("not implemented")
}

func (temporalRoDBStub) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	panic("not implemented")
}

func (temporalRoDBStub) Debug() kv.TemporalDebugDB {
	panic("not implemented")
}

func asTemporalRoDB(db kv.RoDB) kv.TemporalRoDB {
	return temporalRoDBStub{RoDB: db}
}

func (c finalityContextStub) PruneToBlockNum() uint64 {
	return 0
}

func (c finalityContextStub) RetireToBlockNum() uint64 {
	return 0
}

func (c finalityContextStub) MaxReorgDepth() uint64 {
	return c.maxReorgDepth
}

func (c finalityContextStub) ReadyForCollation(_ context.Context, _ kv.TemporalRoDB, _ uint64) (finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB uint64, ok bool, err error) {
	return c.finalisedBlockNum, c.lastBlockInStep, c.lastBlockInDB, c.lastTxInDB, c.ready, nil
}

func TestReadyForCollationPreservesFinalityContextResult(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	finalityCtx := finalityContextStub{
		finalisedBlockNum: 12,
		lastBlockInStep:   10,
		lastBlockInDB:     20,
		lastTxInDB:        25,
		ready:             true,
	}
	finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB, ready, err := agg.readyForCollation(t.Context(), asTemporalRoDB(db), kv.Step(0), finalityCtx)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, uint64(12), finalisedBlockNum)
	require.Equal(t, uint64(10), lastBlockInStep)
	require.Equal(t, uint64(20), lastBlockInDB)
	require.Equal(t, uint64(25), lastTxInDB)
}

func TestBuildFilesClosesBatchValuesWhenHistoryCollationFails(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 16)
	d := agg.d[kv.AccountsDomain]
	writeDupSortStepBacklog(t, db, d, 16, 2, 8, 3)
	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	comps, err := d.collateValuesBatch(t.Context(), 0, 1, roTx)
	roTx.Rollback()
	require.NoError(t, err)
	batch := &collationBatch{from: 0, to: 1}
	batch.values[kv.AccountsDomain] = comps
	defer batch.Close()
	obstacle := filepath.Join(agg.dirs.Tmp, filepath.Base(d.History.vNewFilePath(0, 1))+".idt")
	require.NoError(t, os.Mkdir(obstacle, 0o755))

	require.Error(t, agg.buildFiles(t.Context(), asTemporalRoDB(db), 0, unboundedFinalityCtx, nil, batch))
	leftover, err := filepath.Glob(filepath.Join(agg.dirs.Tmp, "*.idt"))
	require.NoError(t, err)
	require.Equal(t, []string{obstacle}, leftover)
}

func TestBuildFilesFallsBackToSingleStepWhenBatchCollationFails(t *testing.T) {
	for name, build := range map[string]func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB){
		"BuildFilesInBackground": func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB) {
			<-agg.BuildFilesInBackground(db, 4*16, unboundedFinalityCtx)
		},
		"BuildFiles2": func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB) {
			require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
			agg.background.Wait()
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, 16)
			d := agg.d[kv.AccountsDomain]
			writeDupSortStepBacklog(t, db, d, 16, 4, 8, 3)
			obstacle := filepath.Join(agg.dirs.Tmp, filepath.Base(d.kvNewFilePath(1, 2))+".idt")
			require.NoError(t, os.Mkdir(obstacle, 0o755))
			agg.SetMaintenanceObserver(func(_ context.Context, event MaintenanceEvent) {
				if event.Point == MaintenanceBuildStarted {
					_ = dir.RemoveAll(obstacle)
				}
			})

			build(t, agg, asTemporalRoDB(db))
			require.GreaterOrEqual(t, agg.EndTxNumMinimax(), uint64(2*16))
		})
	}
}

func TestBuildFilesRetriesStepWithoutBatchWhenBatchedBuildFails(t *testing.T) {
	for name, build := range map[string]func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB){
		"BuildFilesInBackground": func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB) {
			<-agg.BuildFilesInBackground(db, 4*16, unboundedFinalityCtx)
		},
		"BuildFiles2": func(t *testing.T, agg *Aggregator, db kv.TemporalRoDB) {
			require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
			agg.background.Wait()
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, 16)
			d := agg.d[kv.AccountsDomain]
			writeDupSortStepBacklog(t, db, d, 16, 4, 8, 3)
			obstacle := filepath.Join(agg.dirs.Tmp, filepath.Base(d.History.vNewFilePath(0, 1))+".idt")
			require.NoError(t, os.Mkdir(obstacle, 0o755))
			staged := filepath.Join(agg.dirs.Tmp, filepath.Base(d.kvNewFilePath(1, 2))+".idt")
			stagedLater := filepath.Join(agg.dirs.Tmp, filepath.Base(d.kvNewFilePath(2, 3))+".idt")
			var attempts atomic.Int32
			var stagedAtRetry, stagedAfterRetry atomic.Bool
			agg.SetMaintenanceObserver(func(_ context.Context, event MaintenanceEvent) {
				if event.Point != MaintenanceBuildStarted {
					return
				}
				switch attempts.Add(1) {
				case 2:
					_, err := os.Stat(staged)
					stagedAtRetry.Store(err == nil)
					_ = dir.RemoveAll(obstacle)
				case 3:
					_, err := os.Stat(stagedLater)
					stagedAfterRetry.Store(err == nil)
				}
			})

			build(t, agg, asTemporalRoDB(db))
			require.GreaterOrEqual(t, agg.EndTxNumMinimax(), uint64(16))
			require.False(t, stagedAtRetry.Load(), "batch must be closed before the single-step retry")
			require.GreaterOrEqual(t, attempts.Load(), int32(3))
			require.False(t, stagedAfterRetry.Load(), "batching must stay off after a batched build failure")
		})
	}
}

func TestBuildFilesInBackgroundStopsWhenBatchCollationIsCanceled(t *testing.T) {
	logger := log.New()
	var warnings bytes.Buffer
	logger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StreamHandler(&warnings, log.LogfmtFormat())))
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(db))
	writeDupSortStepBacklog(t, db, agg.d[kv.AccountsDomain], 16, 4, 8, 3)

	agg.ctxCancel()
	<-agg.BuildFilesInBackground(asTemporalRoDB(db), 4*16, unboundedFinalityCtx)
	require.Empty(t, warnings.String())
}

func TestCollateBatchCompressorsOutliveTheCollation(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 16)
	writeDupSortStepBacklog(t, db, agg.d[kv.AccountsDomain], 16, 4, 8, 3)

	batch, err := agg.collateBatch(t.Context(), asTemporalRoDB(db), 0, 4, unboundedFinalityCtx)
	require.NoError(t, err)
	defer batch.Close()
	comp := batch.values[kv.AccountsDomain][0]
	for range 1024 {
		require.NoError(t, comp.AddUncompressedWord([]byte("word")))
	}
}

func TestCollateBatchCoversAtMostMaxSteps(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 16)
	writeDupSortStepBacklog(t, db, agg.d[kv.AccountsDomain], 16, 12, 8, 5)

	batch, err := agg.collateBatch(t.Context(), asTemporalRoDB(db), 0, 12, unboundedFinalityCtx)
	require.NoError(t, err)
	defer batch.Close()
	require.Equal(t, kv.Step(0), batch.from)
	require.Equal(t, kv.Step(8), batch.to)
	require.Len(t, batch.values[kv.AccountsDomain], 8)
}
