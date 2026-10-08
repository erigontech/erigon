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

package execmodule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type frozenCycleObservation struct {
	pipelineInitialCycles []bool
	pruneRetentionBlocks  []uint64
}

// runFrozenBlocksObserved runs ProcessFrozenBlocks over a one-stage pipeline
// whose head is at block 1000 and whose stored finalized block (2000) is stale,
// i.e. above the head.
func runFrozenBlocksObserved(t *testing.T, syncCfg ethconfig.Sync) frozenCycleObservation {
	t.Helper()
	const head, staleFinalized = 1000, 2000
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		finalizedHash := common.Hash{0x01}
		if err := rawdb.WriteHeaderNumber(tx, finalizedHash, staleFinalized); err != nil {
			return err
		}
		rawdb.WriteForkchoiceFinalized(tx, finalizedHash)
		return nil
	}))

	var obs frozenCycleObservation
	runsInPipeline := false
	stage := &stagedsync.Stage{
		ID: stages.Snapshots,
		Forward: func(_ bool, s *stagedsync.StageState, _ stagedsync.Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
			if !runsInPipeline {
				runsInPipeline = true
				return nil
			}
			obs.pipelineInitialCycles = append(obs.pipelineInitialCycles, s.CurrentSyncCycle.IsInitialCycle)
			return stages.SaveStageProgress(tx, stages.Execution, head)
		},
		Unwind: func(*stagedsync.UnwindState, *stagedsync.StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
			return nil
		},
		Prune: func(_ context.Context, p *stagedsync.PruneState, _ kv.RwTx, _ time.Duration, _ log.Logger) error {
			obs.pruneRetentionBlocks = append(obs.pruneRetentionBlocks, p.FinalityCtx.PruneToBlockNum())
			return nil
		},
	}
	chainConfig := &chain.Config{ChainName: "test"}
	notifications := shards.NewNotifications(nil)
	sync := stagedsync.New(syncCfg, []*stagedsync.Stage{stage}, nil, stagedsync.PruneOrder{stages.Snapshots}, logger, stages.ModeApplyingBlocks)
	dispatcher := NewDispatcher(chainConfig, notifications.Events, notifications.StateChangesConsumer, logger)
	pe := NewPipelineExecutor(sync, db, pinTestBlockReader{}, chainConfig, nil, nil, nil, dispatcher, logger)
	hook := stageloop.NewHook(t.Context(), notifications, sync, chainConfig, logger, dispatcher, nil, nil, nil, pinTestBlockReader{})

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))
	require.NotEmpty(t, obs.pipelineInitialCycles)
	require.NotEmpty(t, obs.pruneRetentionBlocks)
	return obs
}

func TestProcessFrozenBlocksCycleMode(t *testing.T) {
	const maxReorgDepth = 10
	const retentionBelowHead = 1000 - maxReorgDepth

	t.Run("batch mode runs initial cycles", func(t *testing.T) {
		obs := runFrozenBlocksObserved(t, ethconfig.Sync{MaxReorgDepth: maxReorgDepth})
		for _, initial := range obs.pipelineInitialCycles {
			require.True(t, initial)
		}
		for _, retention := range obs.pruneRetentionBlocks {
			require.EqualValues(t, retentionBelowHead, retention)
		}
	})

	t.Run("chain tip mode runs non-initial cycles and ignores the stored finalized block", func(t *testing.T) {
		obs := runFrozenBlocksObserved(t, ethconfig.Sync{MaxReorgDepth: maxReorgDepth, ChainTipMode: true})
		for _, initial := range obs.pipelineInitialCycles {
			require.False(t, initial)
		}
		for _, retention := range obs.pruneRetentionBlocks {
			require.EqualValues(t, retentionBelowHead, retention)
		}
	})
}

type frozenTo100BlockReader struct{ pinTestBlockReader }

func (frozenTo100BlockReader) FrozenBlocks() uint64 { return 100 }

func TestProcessFrozenBlocksStopsAtExecStopAtBlock(t *testing.T) {
	const stopAt = 5
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for blockNum := uint64(0); blockNum <= 100; blockNum++ {
			if err := rawdbv3.TxNums.Append(tx, blockNum, blockNum); err != nil {
				return err
			}
		}
		return nil
	}))
	runsInPipeline := false
	stage := &stagedsync.Stage{
		ID: stages.Snapshots,
		Forward: func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
			if !runsInPipeline {
				runsInPipeline = true
				return nil
			}
			progress, err := stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return err
			}
			for _, id := range []stages.SyncStage{stages.Execution, stages.Finish} {
				if err := stages.SaveStageProgress(tx, id, progress+1); err != nil {
					return err
				}
			}
			return &stagedsync.ErrLoopExhausted{}
		},
		Unwind: func(*stagedsync.UnwindState, *stagedsync.StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
			return nil
		},
	}
	chainConfig := &chain.Config{ChainName: "test"}
	notifications := shards.NewNotifications(nil)
	sync := stagedsync.New(ethconfig.Sync{ExecStopAtBlock: stopAt}, []*stagedsync.Stage{stage}, nil, nil, logger, stages.ModeApplyingBlocks)
	dispatcher := NewDispatcher(chainConfig, notifications.Events, notifications.StateChangesConsumer, logger)
	pe := NewPipelineExecutor(sync, db, frozenTo100BlockReader{}, chainConfig, nil, nil, nil, dispatcher, logger)
	hook := stageloop.NewHook(t.Context(), notifications, sync, chainConfig, logger, dispatcher, nil, nil, nil, frozenTo100BlockReader{})

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))

	var progress uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) (err error) {
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}))
	require.EqualValues(t, stopAt, progress)
}
