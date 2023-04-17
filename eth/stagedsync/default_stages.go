package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/sync_stages"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
)

func DefaultStages(ctx context.Context, snapshots SnapshotsCfg, headers HeadersCfg, cumulativeIndex CumulativeIndexCfg, blockHashCfg BlockHashesCfg, bodies BodiesCfg, senders SendersCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg, history HistoryCfg, logIndex LogIndexCfg, callTraces CallTracesCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*sync_stages.Stage {
	return []*sync_stages.Stage{
		{
			ID:          sync_stages.Headers,
			Description: "Download headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageHeaders(s, u, ctx, tx, headers, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return HeadersUnwind(u, s, tx, headers, test)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return HeadersPrune(p, tx, headers, ctx)
			},
		},
		{
			ID:          sync_stages.CumulativeIndex,
			Description: "Write Cumulative Index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneCumulativeIndexStage(p, tx, ctx)
			},
		},
		{
			ID:          sync_stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          sync_stages.Bodies,
			Description: "Download block bodies",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return BodiesForward(s, u, ctx, tx, bodies, test, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindBodiesStage(u, tx, bodies, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneBodiesStage(p, tx, bodies, ctx)
			},
		},
		{
			ID:          sync_stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          sync_stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          sync_stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    bodies.historyV3 && ethconfig.EnableHistoryV4InTest,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          sync_stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    bodies.historyV3 && ethconfig.EnableHistoryV4InTest,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if exec.chainConfig.IsPrague(0) {
					_, err := SpawnVerkleTrie(s, u, tx, trieCfg, ctx)
					return err
				}
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx, quiet)
				return err
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				if exec.chainConfig.IsPrague(0) {
					return UnwindVerkleTrie(u, s, tx, trieCfg, ctx)
				}
				return UnwindIntermediateHashesStage(u, s, tx, trieCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  sync_stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            bodies.historyV3,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          sync_stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    bodies.historyV3,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          sync_stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    bodies.historyV3,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          sync_stages.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    bodies.historyV3,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          sync_stages.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
			},
		},
		{
			ID:          sync_stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, _ sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

// StateStages are all stages necessary for basic unwind and stage computation, it is primarily used to process side forks and memory execution.
func StateStages(ctx context.Context, headers HeadersCfg, bodies BodiesCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg) []*sync_stages.Stage {
	return []*sync_stages.Stage{
		{
			ID:          sync_stages.Headers,
			Description: "Download headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return nil
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return HeadersUnwind(u, s, tx, headers, false)
			},
		},
		{
			ID:          sync_stages.Bodies,
			Description: "Download block bodies",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return BodiesForward(s, u, ctx, tx, bodies, false, false, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindBodiesStage(u, tx, bodies, ctx)
			},
		},
		{
			ID:          sync_stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          sync_stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindSendersStage(u, tx, senders, ctx)
			},
		},
		{
			ID:          sync_stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
		},
		{
			ID:          sync_stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
		},
		{
			ID:          sync_stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx, quiet)
				return err
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindIntermediateHashesStage(u, s, tx, trieCfg, ctx)
			},
		},
	}
}

func DefaultZkStages(
	ctx context.Context,
	snapshots SnapshotsCfg,
	l1VerificationsCfg zkStages.L1VerificationsCfg,
	l1SequencesCfg zkStages.L1SequencesCfg,
	batchesCfg zkStages.BatchesCfg,
	cumulativeIndex CumulativeIndexCfg,
	blockHashCfg BlockHashesCfg,
	senders SendersCfg,
	exec ExecuteBlockCfg,
	hashState HashStateCfg,
	zkInterHashesCfg zkStages.ZkInterHashesCfg,
	history HistoryCfg,
	logIndex LogIndexCfg,
	callTraces CallTracesCfg,
	txLookup TxLookupCfg,
	finish FinishCfg,
	test bool,
) []*sync_stages.Stage {
	return []*sync_stages.Stage{
		{
			ID:          sync_stages.L1Verifications,
			Description: "Download L1 Verifications",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return zkStages.SpawnStageL1Verifications(s, u, ctx, tx, l1VerificationsCfg, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return zkStages.UnwindL1VerificationsStage(u, tx, l1VerificationsCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return zkStages.PruneL1VerificationsStage(p, tx, l1VerificationsCfg, ctx)
			},
		},
		{
			ID:          sync_stages.L1Sequences,
			Description: "Download L1 Sequences",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return zkStages.SpawnStageL1Sequences(s, u, ctx, tx, l1SequencesCfg, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return zkStages.UnwindL1SequencesStage(u, tx, l1SequencesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return zkStages.PruneL1SequencesStage(p, tx)
			},
		},
		{
			ID:          sync_stages.Batches,
			Description: "Download batches",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				if badBlockUnwind {
					return nil
				}
				return zkStages.SpawnStageBatches(s, u, ctx, tx, batchesCfg, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return zkStages.UnwindBatchesStage(u, tx, batchesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return zkStages.PruneBatchesStage(p, tx, batchesCfg, ctx)
			},
		},
		{
			ID:          sync_stages.CumulativeIndex,
			Description: "Write Cumulative Index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneCumulativeIndexStage(p, tx, ctx)
			},
		},
		{
			ID:          sync_stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          sync_stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          sync_stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          sync_stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnHashStateStage(s, tx, hashState, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          sync_stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				_, err := zkStages.SpawnZkIntermediateHashesStage(s, u, tx, zkInterHashesCfg, ctx, quiet)
				return err
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return zkStages.UnwindZkIntermediateHashesStage(u, s, tx, zkInterHashesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				// TODO: implement this in zk interhashes
				return nil
			},
		},
		{
			ID:                  sync_stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          sync_stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          sync_stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          sync_stages.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnLogIndex(s, tx, logIndex, ctx, 0)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          sync_stages.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneTxLookup(p, tx, txLookup, ctx, firstCycle)
			},
		},
		{
			ID:          sync_stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, _ sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *sync_stages.PruneState, tx kv.RwTx) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

var DefaultForwardOrder = sync_stages.UnwindOrder{
	sync_stages.Snapshots,
	sync_stages.Headers,
	sync_stages.BlockHashes,
	sync_stages.Bodies,

	// Stages below don't use Internet
	sync_stages.Senders,
	sync_stages.Execution,
	sync_stages.Translation,
	sync_stages.HashState,
	sync_stages.IntermediateHashes,
	sync_stages.CallTraces,
	sync_stages.AccountHistoryIndex,
	sync_stages.StorageHistoryIndex,
	sync_stages.LogIndex,
	sync_stages.TxLookup,
	sync_stages.Finish,
}

var ZkUnwindOrder = sync_stages.UnwindOrder{
	sync_stages.L1Verifications,
	sync_stages.Batches,
	sync_stages.BlockHashes,

	sync_stages.IntermediateHashes, // need to unwind SMT before we remove history
	sync_stages.Execution,
	sync_stages.HashState,
	sync_stages.Senders,
	sync_stages.Translation,
	sync_stages.CallTraces,
	sync_stages.AccountHistoryIndex,
	sync_stages.StorageHistoryIndex,
	sync_stages.LogIndex,
	sync_stages.TxLookup,
	sync_stages.Finish,
}

var DefaultUnwindOrder = sync_stages.UnwindOrder{}

var StateUnwindOrder = sync_stages.UnwindOrder{}

var DefaultPruneOrder = sync_stages.PruneOrder{}

var MiningUnwindOrder = sync_stages.UnwindOrder{} // nothing to unwind in mining - because mining does not commit db changes
var MiningPruneOrder = sync_stages.PruneOrder{}   // nothing to unwind in mining - because mining does not commit db changes
