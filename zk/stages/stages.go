package stages

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
	stages "github.com/ledgerwatch/erigon/eth/stagedsync"
	stages2 "github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func SequencerZkStages(
	ctx context.Context,
	cumulativeIndex stagedsync.CumulativeIndexCfg,
	dataStreamCatchupCfg DataStreamCatchupCfg,
	sequencerInterhashesCfg SequencerInterhashesCfg,
	exec SequenceBlockCfg,
	hashState stagedsync.HashStateCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
	callTraces stagedsync.CallTracesCfg,
	txLookup stagedsync.TxLookupCfg,
	finish stagedsync.FinishCfg,
	test bool,
) []*stages.Stage {
	return []*stages.Stage{
		/*
			TODO: doesn't work since we don't have headers yet at this stage; should be moved until after execution

			{
				ID:          stages.CumulativeIndex,
				Description: "Write Cumulative Index",
				Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
					return stagedsync.SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
				},
				Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
					return stagedsync.UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
				},
				Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
					return stagedsync.PruneCumulativeIndexStage(p, tx, ctx)
				},
			},
		*/
		/* TODO: here should be some stage of getting GERs from the L1 and writing to the DB for future batches
		 */
		{
			/*
				TODO:
				we need to have another "execution" stage, that takes data from the txpool instead of from headers/bodies.

				this "execution" stage should, in fact, write the following:
				* block headers
				* block bodies
				* batches

				currently it could be hard-coded to 1 batch -contains-> 1 block -contains-> 1 tx
					+------------+
					|  Batch #1  |
					+------------+
					| +--------+ |
					| |Block #1| |
					| |        | |
					| | +Tx #1 | |
					| +--------+ |
					+------------+
				later it should take both the gas limit of the block and the zk counters limit

				it should also generate a retainlist for the future batch witness generation
			*/
			ID:          stages2.Execution,
			Description: "Sequence transactions",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnSequencingStage(s, u, tx, 0, ctx, exec, firstCycle, quiet)
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
				return SpawnSequencerInterhashesStage(s, u, tx, ctx, sequencerInterhashesCfg, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindSequencerInterhashsStage(u, s, tx, ctx, sequencerInterhashesCfg, firstCycle)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneSequencerInterhashesStage(p, tx, sequencerInterhashesCfg, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:                  stages2.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
			},
		},
		/*
		  TODO: verify batches stage -- real executor that verifies batches
		  if it fails, we need to unwind everything up until before the bad batch
		*/
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
				return stagedsync.FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func DefaultZkStages(
	ctx context.Context,
	l1SyncerCfg L1SyncerCfg,
	batchesCfg BatchesCfg,
	dataStreamCatchupCfg DataStreamCatchupCfg,
	cumulativeIndex stagedsync.CumulativeIndexCfg,
	blockHashCfg stagedsync.BlockHashesCfg,
	senders stagedsync.SendersCfg,
	exec stagedsync.ExecuteBlockCfg,
	hashState stagedsync.HashStateCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
	callTraces stagedsync.CallTracesCfg,
	txLookup stagedsync.TxLookupCfg,
	finish stagedsync.FinishCfg,
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
				return SpawnStageL1Syncer(s, u, ctx, tx, l1SyncerCfg, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindL1SyncerStage(u, tx, l1SyncerCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneL1SyncerStage(p, tx, l1SyncerCfg, ctx)
			},
		},
		{
			ID:          stages2.Batches,
			Description: "Download batches",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageBatches(s, u, ctx, tx, batchesCfg, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return UnwindBatchesStage(u, tx, batchesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return PruneBatchesStage(p, tx, batchesCfg, ctx)
			},
		},
		{
			/*
			* FIXME: this stage doesn't work since the "headers" we have in the datastream don't have any gasUsed, it's always 0.
			*
			* to solve this we probably should move it after execution (execution doesn't depend on it) and update the unwinds.
			**/
			ID:          stages2.CumulativeIndex,
			Description: "Write Cumulative Index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneCumulativeIndexStage(p, tx, ctx)
			},
		},
		{
			ID:          stages2.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages2.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages2.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneHashStateStage(p, tx, hashState, ctx)
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
				return UnwindZkIntermediateHashesStage(u, s, tx, zkInterHashesCfg, ctx)
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
				return stagedsync.SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,

			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return stagedsync.SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
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
				return stagedsync.FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, tx kv.RwTx) error {
				return stagedsync.UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx) error {
				return stagedsync.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

var ZkSequencerUnwindOrder = stages.UnwindOrder{
	stages2.IntermediateHashes, // need to unwind SMT before we remove history
	stages2.Execution,
	stages2.HashState,
	stages2.CallTraces,
	stages2.AccountHistoryIndex,
	stages2.StorageHistoryIndex,
	stages2.LogIndex,
	stages2.TxLookup,
	stages2.Finish,
}

var ZkUnwindOrder = stages.UnwindOrder{
	stages2.L1Syncer,
	stages2.Batches,
	stages2.BlockHashes,
	stages2.IntermediateHashes, // need to unwind SMT before we remove history
	stages2.Execution,
	stages2.HashState,
	stages2.Senders,
	stages2.CallTraces,
	stages2.AccountHistoryIndex,
	stages2.StorageHistoryIndex,
	stages2.LogIndex,
	stages2.TxLookup,
	stages2.Finish,
}
