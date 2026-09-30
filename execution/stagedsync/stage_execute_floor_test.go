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

package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestUnwindExecutionStageConversionBlockFloor(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles())
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for blockNum := uint64(0); blockNum <= 6; blockNum++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, blockNum, blockNum*10+9))
	}
	require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(4, common.Hash{1}), []byte{1}))
	conversionBlock := uint64(5)
	conversionTx := uint64(53)
	require.NoError(t, state.WriteErigonDBSettings(tx.Debug().Dirs(), &state.ErigonDBSettings{
		ConversionBlockNum: &conversionBlock,
		ConversionTxNum:    &conversionTx,
	}))

	doms, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	cfg := ExecuteBlockCfg{blockReader: br}
	s := &StageState{ID: stages.Execution, BlockNumber: conversionBlock}
	u := &UnwindState{ID: stages.Execution, UnwindPoint: conversionBlock - 1, CurrentBlockNumber: conversionBlock}
	err = UnwindExecutionStage(u, s, doms, tx, context.Background(), cfg, log.New())
	require.ErrorContains(t, err, "conversion point", "an unwind below the conversion block must name the conversion point")
	require.ErrorIs(t, err, ErrTooDeepUnwind)
	require.ErrorIs(t, err, state.ErrConversionFloor)

	u = &UnwindState{ID: stages.Execution, UnwindPoint: conversionBlock, CurrentBlockNumber: conversionBlock}
	require.NoError(t, UnwindExecutionStage(u, s, doms, tx, context.Background(), cfg, log.New()), "the conversion block itself must remain unwindable")
}

func TestUnwindExecutionStageConversionTxFloor(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles())
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for blockNum := uint64(0); blockNum <= 7; blockNum++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, blockNum, blockNum*10+9))
	}
	conversionBlock := uint64(5)
	conversionTx := uint64(60)
	require.NoError(t, state.WriteErigonDBSettings(tx.Debug().Dirs(), &state.ErigonDBSettings{
		ConversionBlockNum: &conversionBlock,
		ConversionTxNum:    &conversionTx,
	}))

	doms, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	_, err = unwindDomsToBlock(context.Background(), tx, br, doms, conversionBlock, nil)
	require.ErrorContains(t, err, "conversion point")
	require.ErrorIs(t, err, state.ErrConversionFloor)
	_, err = unwindDomsToBlock(context.Background(), tx, br, doms, conversionBlock+1, nil)
	require.NoError(t, err, "an unwind above the conversion txNum must be allowed")
}

func TestUnwindExecutionStageConversionPointMayBeMidBlock(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	conversionBlock := uint64(5)
	conversionTx := uint64(53)
	require.NoError(t, state.WriteErigonDBSettings(tx.Debug().Dirs(), &state.ErigonDBSettings{
		ConversionBlockNum: &conversionBlock,
		ConversionTxNum:    &conversionTx,
	}))

	blockStart := uint64(50)
	blockEnd := uint64(59)
	require.Greater(t, conversionTx, blockStart)
	require.Less(t, conversionTx, blockEnd)
}

func TestSyncUnwindToConversionBlockFloorIsTyped(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	conversionBlock, conversionTx := uint64(5), uint64(53)
	require.NoError(t, state.WriteErigonDBSettings(tx.Debug().Dirs(), &state.ErigonDBSettings{
		ConversionBlockNum: &conversionBlock,
		ConversionTxNum:    &conversionTx,
	}))
	sync := New(ethconfig.Defaults.Sync, nil, nil, nil, log.New(), stages.ModeApplyingBlocks)
	err = sync.UnwindTo(conversionBlock-1, UnwindReason{}, tx)
	require.ErrorIs(t, err, ErrTooDeepUnwind)
	require.ErrorIs(t, err, state.ErrConversionFloor)
}
