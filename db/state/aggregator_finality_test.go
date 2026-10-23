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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
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

func TestBuildFilesClosesBatchValuesWhenCollationTxFails(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 16)
	d := agg.d[kv.AccountsDomain]
	writeDupSortStepBacklog(t, db, d, 16, 2, 8, 3)
	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	comps, err := d.collateValuesBatch(t.Context(), 0, 2, roTx)
	roTx.Rollback()
	require.NoError(t, err)
	batch := &collationBatch{from: 0, to: 2}
	batch.values[kv.AccountsDomain] = comps

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, batch))
	batch.Close()
	leftover, err := filepath.Glob(filepath.Join(agg.dirs.Tmp, "*.idt"))
	require.NoError(t, err)
	require.Empty(t, leftover)
}
