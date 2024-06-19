package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func DefaultStages(ctx context.Context,
	snapshots SnapshotsCfg,
	headers HeadersCfg,
	borHeimdallCfg BorHeimdallCfg,
	blockHashCfg BlockHashesCfg,
	bodies BodiesCfg,
	senders SendersCfg,
	exec ExecuteBlockCfg,
	hashState HashStateCfg,
	trieCfg TrieCfg,
	history HistoryCfg,
	logIndex LogIndexCfg,
	callTraces CallTracesCfg,
	txLookup TxLookupCfg,
	finish FinishCfg,
	test bool) []*Stage {
	return []*Stage{
		{
			ID:          stages.Snapshots,
			Description: "Download snapshots",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageSnapshots(s, ctx, txc.Tx, snapshots, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return SnapshotsPrune(p, snapshots, ctx, tx, logger)
			},
		},
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageHeaders(s, u, ctx, txc.Tx, headers, test, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return HeadersUnwind(u, s, txc.Tx, headers, test)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.BorHeimdall,
			Description: "Download Bor-specific data from Heimdall",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return BorHeimdallForward(s, u, ctx, txc.Tx, borHeimdallCfg, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return BorHeimdallUnwind(u, ctx, s, txc.Tx, borHeimdallCfg)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return BorHeimdallPrune(p, ctx, tx, borHeimdallCfg)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnBlockHashStage(s, txc.Tx, blockHashCfg, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBlockHashStage(u, txc.Tx, blockHashCfg, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return BodiesForward(s, u, ctx, txc.Tx, bodies, test, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBodiesStage(u, txc.Tx, bodies, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Disabled:    dbg.StagesOnlyBlocks,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnExecuteBlocksStage(s, u, txc, 0, ctx, exec, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindExecutionStage(u, s, txc, ctx, exec, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneExecutionStage(p, tx, exec, ctx)
			},
		},
		//{
		//	ID:          stages.CustomTrace,
		//	Description: "Re-Execute blocks on history state - with custom tracer",
		//	Disabled:    !bodies.historyV3 || dbg.StagesOnlyBlocks,
		//	Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return SpawnCustomTrace(s, txc, cfg, ctx, firstCycle, 0, logger)
		//	},
		//	Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return UnwindCustomTrace(u, s, txc, cfg, ctx, logger)
		//	},
		//	Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return PruneCustomTrace(p, tx, cfg, ctx, firstCycle, logger)
		//	},
		//},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnHashStateStage(s, txc.Tx, hashState, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindHashStateStage(u, s, txc.Tx, hashState, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					_, err := SpawnVerkleTrie(s, u, txc.Tx, trieCfg, ctx, logger)
					return err
				}
				_, err := SpawnIntermediateHashesStage(s, u, txc.Tx, trieCfg, ctx, logger)
				return err
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					return UnwindVerkleTrie(u, s, txc.Tx, trieCfg, ctx, logger)
				}
				return UnwindIntermediateHashesStage(u, s, txc.Tx, trieCfg, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnCallTraces(s, txc.Tx, callTraces, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindCallTraces(u, s, txc.Tx, callTraces, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneCallTraces(p, tx, callTraces, ctx, logger)
			},
		},
		{
			ID:          stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnAccountHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindAccountHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnStorageHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindStorageHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnLogIndex(s, txc.Tx, logIndex, ctx, 0, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindLogIndex(u, s, txc.Tx, logIndex, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneLogIndex(p, tx, logIndex, ctx, logger)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate tx lookup index",
			Disabled:    dbg.StagesOnlyBlocks,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneTxLookup(p, tx, txLookup, ctx, logger)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(badBlockUnwind bool, s *StageState, _ Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return FinishForward(s, txc.Tx, finish)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func PipelineStages(ctx context.Context, snapshots SnapshotsCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg, history HistoryCfg, logIndex LogIndexCfg, callTraces CallTracesCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*Stage {
	return []*Stage{
		{
			ID:          stages.Snapshots,
			Description: "Download snapshots",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageSnapshots(s, ctx, txc.Tx, snapshots, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return SnapshotsPrune(p, snapshots, ctx, tx, logger)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnBlockHashStage(s, txc.Tx, blockHashCfg, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBlockHashStage(u, txc.Tx, blockHashCfg, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnExecuteBlocksStage(s, u, txc, 0, ctx, exec, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindExecutionStage(u, s, txc, ctx, exec, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneExecutionStage(p, tx, exec, ctx)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnHashStateStage(s, txc.Tx, hashState, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindHashStateStage(u, s, txc.Tx, hashState, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					_, err := SpawnVerkleTrie(s, u, txc.Tx, trieCfg, ctx, logger)
					return err
				}
				_, err := SpawnIntermediateHashesStage(s, u, txc.Tx, trieCfg, ctx, logger)
				return err
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					return UnwindVerkleTrie(u, s, txc.Tx, trieCfg, ctx, logger)
				}
				return UnwindIntermediateHashesStage(u, s, txc.Tx, trieCfg, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnCallTraces(s, txc.Tx, callTraces, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindCallTraces(u, s, txc.Tx, callTraces, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneCallTraces(p, tx, callTraces, ctx, logger)
			},
		},
		{
			ID:          stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnAccountHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindAccountHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnStorageHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindStorageHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnLogIndex(s, txc.Tx, logIndex, ctx, 0, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindLogIndex(u, s, txc.Tx, logIndex, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneLogIndex(p, tx, logIndex, ctx, logger)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneTxLookup(p, tx, txLookup, ctx, logger)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(badBlockUnwind bool, s *StageState, _ Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return FinishForward(s, txc.Tx, finish)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

// when uploading - potentially from zero we need to include headers and bodies stages otherwise we won't recover the POW portion of the chain
func UploaderPipelineStages(ctx context.Context, snapshots SnapshotsCfg, headers HeadersCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, bodies BodiesCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg, history HistoryCfg, logIndex LogIndexCfg, callTraces CallTracesCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*Stage {
	return []*Stage{
		{
			ID:          stages.Snapshots,
			Description: "Download snapshots",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageSnapshots(s, ctx, txc.Tx, snapshots, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return SnapshotsPrune(p, snapshots, ctx, tx, logger)
			},
		},
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageHeaders(s, u, ctx, txc.Tx, headers, test, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return HeadersUnwind(u, s, txc.Tx, headers, test)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnBlockHashStage(s, txc.Tx, blockHashCfg, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBlockHashStage(u, txc.Tx, blockHashCfg, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return BodiesForward(s, u, ctx, txc.Tx, bodies, test, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBodiesStage(u, txc.Tx, bodies, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnExecuteBlocksStage(s, u, txc, 0, ctx, exec, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindExecutionStage(u, s, txc, ctx, exec, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneExecutionStage(p, tx, exec, ctx)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnHashStateStage(s, txc.Tx, hashState, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindHashStateStage(u, s, txc.Tx, hashState, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					_, err := SpawnVerkleTrie(s, u, txc.Tx, trieCfg, ctx, logger)
					return err
				}
				_, err := SpawnIntermediateHashesStage(s, u, txc.Tx, trieCfg, ctx, logger)
				return err
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				if exec.chainConfig.IsOsaka(0) {
					return UnwindVerkleTrie(u, s, txc.Tx, trieCfg, ctx, logger)
				}
				return UnwindIntermediateHashesStage(u, s, txc.Tx, trieCfg, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnCallTraces(s, txc.Tx, callTraces, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindCallTraces(u, s, txc.Tx, callTraces, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneCallTraces(p, tx, callTraces, ctx, logger)
			},
		},
		{
			ID:          stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnAccountHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindAccountHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnStorageHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindStorageHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    exec.historyV3,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnLogIndex(s, txc.Tx, logIndex, ctx, 0, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindLogIndex(u, s, txc.Tx, logIndex, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneLogIndex(p, tx, logIndex, ctx, logger)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneTxLookup(p, tx, txLookup, ctx, logger)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(badBlockUnwind bool, s *StageState, _ Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return FinishForward(s, txc.Tx, finish)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

// StateStages are all stages necessary for basic unwind and stage computation, it is primarily used to process side forks and memory execution.
func StateStages(ctx context.Context, headers HeadersCfg, bodies BodiesCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return HeadersUnwind(u, s, txc.Tx, headers, false)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBodiesStage(u, txc.Tx, bodies, ctx)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnBlockHashStage(s, txc.Tx, blockHashCfg, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBlockHashStage(u, txc.Tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnExecuteBlocksStage(s, u, txc, 0, ctx, exec, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindExecutionStage(u, s, txc, ctx, exec, logger)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnHashStateStage(s, txc.Tx, hashState, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindHashStateStage(u, s, txc.Tx, hashState, ctx, logger)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    true,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				_, err := SpawnIntermediateHashesStage(s, u, txc.Tx, trieCfg, ctx, logger)
				return err
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindIntermediateHashesStage(u, s, txc.Tx, trieCfg, ctx, logger)
			},
		},
	}
}

func PolygonSyncStages(
	ctx context.Context,
	snapshots SnapshotsCfg,
	polygonSyncStageCfg PolygonSyncStageCfg,
	senders SendersCfg,
	exec ExecuteBlockCfg,
	txLookup TxLookupCfg,
	finish FinishCfg,
) []*Stage {
	return []*Stage{
		{
			ID:          stages.Snapshots,
			Description: "Download snapshots",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageSnapshots(s, ctx, txc.Tx, snapshots, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return SnapshotsPrune(p, snapshots, ctx, tx, logger)
			},
		},
		{
			ID:          stages.PolygonSync,
			Description: "Use polygon sync component to sync headers, bodies and heimdall data",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnPolygonSyncStage(ctx, txc.Tx, s, u, polygonSyncStageCfg)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindPolygonSyncStage()
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PrunePolygonSyncStage()
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Disabled:    dbg.StagesOnlyBlocks,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnExecuteBlocksStage(s, u, txc, 0, ctx, exec, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindExecutionStage(u, s, txc, ctx, exec, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneExecutionStage(p, tx, exec, ctx)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate tx lookup index",
			Disabled:    dbg.StagesOnlyBlocks,
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneTxLookup(p, tx, txLookup, ctx, logger)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(badBlockUnwind bool, s *StageState, _ Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return FinishForward(s, txc.Tx, finish)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

var DefaultForwardOrder = UnwindOrder{
	stages.Snapshots,
	stages.Headers,
	stages.BorHeimdall,
	stages.BlockHashes,
	stages.Bodies,

	// Stages below don't use Internet
	stages.Senders,
	stages.Execution,
	stages.HashState,
	stages.IntermediateHashes,
	stages.CallTraces,
	stages.AccountHistoryIndex,
	stages.StorageHistoryIndex,
	stages.LogIndex,
	stages.TxLookup,
	stages.Finish,
}

// UnwindOrder represents the order in which the stages needs to be unwound.
// The unwind order is important and not always just stages going backwards.
// Let's say, there is tx pool can be unwound only after execution.
// It's ok to remove some stage from here to disable only unwind of stage
type UnwindOrder []stages.SyncStage
type PruneOrder []stages.SyncStage

var DefaultUnwindOrder = UnwindOrder{
	stages.Finish,
	stages.TxLookup,
	stages.LogIndex,
	stages.StorageHistoryIndex,
	stages.AccountHistoryIndex,
	stages.CallTraces,

	// Unwinding of IHashes needs to happen after unwinding HashState
	stages.HashState,
	stages.IntermediateHashes,

	stages.CustomTrace,
	stages.Execution,
	stages.Senders,

	stages.Bodies,
	stages.BlockHashes,
	stages.BorHeimdall,
	stages.Headers,
}

var PipelineUnwindOrder = UnwindOrder{
	stages.Finish,
	stages.TxLookup,
	stages.LogIndex,
	stages.StorageHistoryIndex,
	stages.AccountHistoryIndex,
	stages.CallTraces,

	// Unwinding of IHashes needs to happen after unwinding HashState
	stages.HashState,
	stages.IntermediateHashes,

	stages.Execution,
	stages.Senders,

	stages.BlockHashes,
}

var StateUnwindOrder = UnwindOrder{
	// Unwinding of IHashes needs to happen after unwinding HashState
	stages.HashState,
	stages.IntermediateHashes,
	stages.Execution,
	stages.Senders,
	stages.Bodies,
	stages.BlockHashes,
	stages.Headers,
}

var PolygonSyncUnwindOrder = UnwindOrder{
	stages.Finish,
	stages.TxLookup,
	stages.Execution,
	stages.Senders,
	stages.PolygonSync,
}

var DefaultPruneOrder = PruneOrder{
	stages.Finish,
	stages.TxLookup,
	stages.LogIndex,
	stages.StorageHistoryIndex,
	stages.AccountHistoryIndex,
	stages.CallTraces,

	// Pruning of IHashes needs to happen after pruning HashState
	stages.HashState,
	stages.IntermediateHashes,

	stages.Execution,
	stages.Senders,

	stages.Bodies,
	stages.BlockHashes,
	stages.BorHeimdall,
	stages.Headers,
	stages.Snapshots,
}

var PipelinePruneOrder = PruneOrder{
	stages.Finish,
	stages.TxLookup,
	stages.LogIndex,
	stages.StorageHistoryIndex,
	stages.AccountHistoryIndex,
	stages.CallTraces,

	// Unwinding of IHashes needs to happen after unwinding HashState
	stages.HashState,
	stages.IntermediateHashes,

	stages.Execution,
	stages.Senders,

	stages.BlockHashes,
	stages.Snapshots,
}

var PolygonSyncPruneOrder = PruneOrder{
	stages.Finish,
	stages.TxLookup,
	stages.Execution,
	stages.Senders,
	stages.PolygonSync,
	stages.Snapshots,
}

var MiningUnwindOrder = UnwindOrder{} // nothing to unwind in mining - because mining does not commit db changes
var MiningPruneOrder = PruneOrder{}   // nothing to unwind in mining - because mining does not commit db changes
