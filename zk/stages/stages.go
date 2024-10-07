package stages

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	stages "github.com/ledgerwatch/erigon/eth/stagedsync"
	stages2 "github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func SequencerZkStages(
	ctx context.Context,
	cumulativeIndex stages.CumulativeIndexCfg,
	l1SyncerCfg L1SyncerCfg,
	l1SequencerSyncCfg L1SequencerSyncCfg,
	l1InfoTreeCfg L1InfoTreeCfg,
	sequencerL1BlockSyncCfg SequencerL1BlockSyncCfg,
	dataStreamCatchupCfg DataStreamCatchupCfg,
	exec SequenceBlockCfg,
	hashState stages.HashStateCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	history stages.HistoryCfg,
	logIndex stages.LogIndexCfg,
	callTraces stages.CallTracesCfg,
	txLookup stages.TxLookupCfg,
	finish stages.FinishCfg,
	test bool,
) []*stages.Stage {
	return []*stages.Stage{
		/*
			TODO: doesn't work since we don't have headers yet at this stage; should be moved until after execution

			{
				ID:          stages.CumulativeIndex,
				Description: "Write Cumulative Index",
				Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
					return stages.SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
				},
				Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
					return stages.UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
				},
				Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
					return stages.PruneCumulativeIndexStage(p, tx, ctx)
				},
			},
		*/
		{
			ID:          stages2.L1Syncer,
			Description: "Download L1 Verifications",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageL1Syncer(s, u, ctx, tx, l1SyncerCfg, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1SyncerStage(u, tx, l1SyncerCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1SyncerStage(p, tx, l1SyncerCfg, ctx)
			},
		},
		{
			ID:          stages2.L1SequencerSyncer,
			Description: "L1 Sequencer Sync Updates",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnL1SequencerSyncStage(s, u, tx, l1SequencerSyncCfg, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1SequencerSyncStage(u, tx, l1SequencerSyncCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1SequencerSyncStage(p, tx, l1SequencerSyncCfg, ctx)
			},
		},
		{
			ID:          stages2.L1InfoTree,
			Description: "L1 Info tree index updates sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnL1InfoTreeStage(s, u, tx, l1InfoTreeCfg, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1InfoTreeStage(u, tx, l1InfoTreeCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1InfoTreeStage(p, tx, l1InfoTreeCfg, ctx)
			},
		},
		{
			ID:          stages2.L1BlockSync,
			Description: "L1 Sequencer L1 Block Sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, unwinder stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnSequencerL1BlockSyncStage(s, unwinder, ctx, tx, sequencerL1BlockSyncCfg, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindSequencerL1BlockSyncStage(u, tx, sequencerL1BlockSyncCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneSequencerL1BlockSyncStage(p, tx, sequencerL1BlockSyncCfg, ctx)
			},
		},
		{
			ID:          stages2.Execution,
			Description: "Sequence transactions",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				sequencerErr := SpawnSequencingStage(s, u, ctx, exec, history, quiet)
				if sequencerErr != nil || u.IsUnwindSet() {
					exec.legacyVerifier.CancelAllRequests()
					// on the begining of next iteration the EXECUTION will be aligned to DS
					shouldCheckForExecutionAndDataStreamAlighment = true
				}
				return sequencerErr
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindSequenceExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneSequenceExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.IntermediateHashes,
			Description: "Sequencer Intermediate Hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnSequencerInterhashesStage(s, u, tx, ctx, zkInterHashesCfg, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindSequencerInterhashsStage(u, s, tx, ctx, zkInterHashesCfg)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneSequencerInterhashesStage(p, tx, zkInterHashesCfg, ctx)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindHashStateStage(u, s, tx, hashState, ctx, false)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:                  stages2.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				// return stages.SpawnAccountHistoryIndex(s, tx, history, ctx)
				// only forward part of this stage is part of execution stage
				return nil
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				// return stages.SpawnStorageHistoryIndex(s, tx, history, ctx)
				// only forward part of this stage is part of execution stage
				return nil
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, _ stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func DefaultZkStages(
	ctx context.Context,
	l1SyncerCfg L1SyncerCfg,
	l1InfoTreeCfg L1InfoTreeCfg,
	batchesCfg BatchesCfg,
	dataStreamCatchupCfg DataStreamCatchupCfg,
	cumulativeIndex stages.CumulativeIndexCfg,
	blockHashCfg stages.BlockHashesCfg,
	senders stages.SendersCfg,
	exec stages.ExecuteBlockCfg,
	hashState stages.HashStateCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	history stages.HistoryCfg,
	logIndex stages.LogIndexCfg,
	callTraces stages.CallTracesCfg,
	txLookup stages.TxLookupCfg,
	finish stages.FinishCfg,
	test bool,
) []*stages.Stage {
	return []*stages.Stage{
		{
			ID:          stages2.L1Syncer,
			Description: "Download L1 Verifications",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageL1Syncer(s, u, ctx, tx, l1SyncerCfg, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1SyncerStage(u, tx, l1SyncerCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1SyncerStage(p, tx, l1SyncerCfg, ctx)
			},
		},
		{
			ID:          stages2.L1InfoTree,
			Description: "L1 Info tree index updates sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnL1InfoTreeStage(s, u, tx, l1InfoTreeCfg, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1InfoTreeStage(u, tx, l1InfoTreeCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1InfoTreeStage(p, tx, l1InfoTreeCfg, ctx)
			},
		},
		{
			ID:          stages2.Batches,
			Description: "Download batches",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageBatches(s, u, ctx, tx, batchesCfg)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindBatchesStage(u, tx, batchesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneBatchesStage(p, tx, batchesCfg, ctx)
			},
		},
		{
			ID:          stages2.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages2.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages2.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnExecuteBlocksStageZk(s, u, tx, 0, ctx, exec, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindExecutionStageZk(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneExecutionStageZk(p, tx, exec, ctx, firstCycle)
			},
		}, {
			ID:          stages2.CumulativeIndex,
			Description: "Write Cumulative Index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneCumulativeIndexStage(p, tx, ctx)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindHashStateStage(u, s, tx, hashState, ctx, false)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages2.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				_, err := SpawnZkIntermediateHashesStage(s, u, tx, zkInterHashesCfg, ctx, quiet)
				return err
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindZkIntermediateHashesStage(u, s, tx, zkInterHashesCfg, ctx, false)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				// TODO: implement this in zk interhashes
				return nil
			},
		},
		{
			ID:                  stages2.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,

			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.DataStream,
			Description: "Update the data stream with missing details",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageDataStreamCatchup(s, ctx, tx, dataStreamCatchupCfg)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return nil
			},
		},
		{
			ID:          stages2.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, _ stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stages.FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stages.UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stages.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

var AllStagesZk = []stages2.SyncStage{
	stages2.L1Syncer,
	stages2.Batches,
	stages2.CumulativeIndex,
	stages2.BlockHashes,
	stages2.Senders,
	stages2.Execution,
	stages2.HashState,
	stages2.IntermediateHashes,
	stages2.LogIndex,
	stages2.CallTraces,
	stages2.TxLookup,
	stages2.Finish,
}

var ZkSequencerUnwindOrder = stages.UnwindOrder{
	stages2.TxLookup,
	stages2.LogIndex,
	stages2.HashState,
	stages2.SequenceExecutorVerify,
	stages2.IntermediateHashes, // need to unwind SMT before we remove history
	stages2.StorageHistoryIndex,
	stages2.AccountHistoryIndex,
	stages2.CallTraces,
	stages2.Execution, // need to happen after history and calltraces
	stages2.L1Syncer,
	stages2.Finish,
}

var ZkUnwindOrder = stages.UnwindOrder{
	stages2.TxLookup,
	stages2.LogIndex,
	stages2.HashState,
	stages2.IntermediateHashes, // need to unwind SMT before we remove history
	stages2.StorageHistoryIndex,
	stages2.AccountHistoryIndex,
	stages2.CallTraces,
	stages2.Execution, // need to happen after history and calltraces
	stages2.CumulativeIndex,
	stages2.Senders,
	stages2.BlockHashes,
	stages2.Batches,
	stages2.L1Syncer,
	stages2.Finish,
}
