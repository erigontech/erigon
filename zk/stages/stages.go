package stages

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/log/v3"
	"math/big"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/wrap"

	stages "github.com/erigontech/erigon/eth/stagedsync"
	stages2 "github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk/datastream/server"
)

var (
	dataStreamServerFactory = server.NewZkEVMDataStreamServerFactory()
)

func SequencerZkStages(
	ctx context.Context,
	l1SyncerCfg L1SyncerCfg,
	l1SequencerSyncCfg L1SequencerSyncCfg,
	l1InfoTreeCfg L1InfoTreeCfg,
	sequencerL1BlockSyncCfg SequencerL1BlockSyncCfg,
	exec SequenceBlockCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	history stages.HistoryCfg,
	logIndex stages.LogIndexCfg,
	callTraces stages.CallTracesCfg,
	txLookup stages.TxLookupCfg,
	finish stages.FinishCfg,
	hashStateCfg stages.HashStateCfg,
	test bool,
	chainCfg *chain.Config,
) []*stages.Stage {
	return []*stages.Stage{
		{
			ID:          stages2.L1Syncer,
			Description: "Download L1 Verifications",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageL1Syncer(s, u, ctx, txc.Tx, l1SyncerCfg, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindL1SyncerStage(u, txc.Tx, l1SyncerCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneL1SyncerStage(p, tx, l1SyncerCfg, ctx)
			},
		},
		{
			ID:          stages2.L1SequencerSyncer,
			Description: "L1 Sequencer Sync Updates",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnL1SequencerSyncStage(s, u, txc.Tx, l1SequencerSyncCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindL1SequencerSyncStage(u, txc.Tx, l1SequencerSyncCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneL1SequencerSyncStage(p, tx, l1SequencerSyncCfg, ctx)
			},
		},
		{
			ID:          stages2.L1InfoTree,
			Description: "L1 Info tree index updates sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnL1InfoTreeStage(s, u, txc.Tx, l1InfoTreeCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindL1InfoTreeStage(u, txc.Tx, l1InfoTreeCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneL1InfoTreeStage(p, tx, l1InfoTreeCfg, ctx)
			},
		},
		{
			ID:          stages2.L1BlockSync,
			Description: "L1 Sequencer L1 Block Sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, unwinder stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnSequencerL1BlockSyncStage(s, unwinder, ctx, txc.Tx, sequencerL1BlockSyncCfg, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSequencerL1BlockSyncStage(u, txc.Tx, sequencerL1BlockSyncCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSequencerL1BlockSyncStage(p, tx, sequencerL1BlockSyncCfg, ctx, logger)
			},
		},
		{
			ID:          stages2.Execution,
			Description: "Sequence transactions",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				sequencerErr := SpawnSequencingStage(s, u, ctx, exec, history, false)
				if sequencerErr != nil || u.IsUnwindSet() {
					exec.legacyVerifier.CancelAllRequests()
					// on the begining of next iteration the EXECUTION will be aligned to DS
					shouldCheckForExecutionAndDataStreamAlignment = true
				}
				return sequencerErr
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSequenceExecutionStage(u, s, txc.Tx, ctx, exec, firstCycle, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSequenceExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.IntermediateHashes,
			Description: "Sequencer Intermediate Hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				// this is done in execution stage for the sequencer
				return SpawnSequencerInterhashesStage(s, u, txc.Tx, ctx, zkInterHashesCfg, true)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				if chainCfg.PmtEnabledBlock == nil {
					// PMT was never enabled so use SMT for unwind
					return UnwindSequencerInterhashsStage(u, s, txc.Tx, ctx, zkInterHashesCfg)
				} else if chainCfg.PmtEnabledBlock == big.NewInt(0) {
					// PMT was enabled at genesis so use PMT for unwind
					return stages.UnwindIntermediateHashesStage(u, s, txc.Tx, trieConfigSequencer(zkInterHashesCfg), ctx, logger)
				} else {
					// PMT has been enabled, but we must check if we are unwinding past the PMT enabled block
					if big.NewInt(int64(u.UnwindPoint)).Cmp(chainCfg.PmtEnabledBlock) < 0 {
						// Unwind point is lower than PMT enabled block
						// This means we are trying to unwind past the PmtEnabledBlock
						// We do not support this as of now, so we must panic
						panic(fmt.Sprintf("Unwind point %d is lower than PMT enabled block %d. Unwinding past PMT enabled block is not supported.", u.UnwindPoint, chainCfg.PmtEnabledBlock))
					}
					// All is good, we can use PMT for unwind
					return stages.UnwindIntermediateHashesStage(u, s, txc.Tx, trieConfigSequencer(zkInterHashesCfg), ctx, logger)
				}
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneSequencerInterhashesStage(p, tx, zkInterHashesCfg, ctx)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnHashStateStage(s, txc.Tx, hashStateCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindHashStateStage(u, s, txc.Tx, hashStateCfg, ctx, logger, false)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneHashStateStage(p, tx, hashStateCfg, ctx)
			},
		},
		{
			ID:                  stages2.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnCallTraces(s, txc.Tx, callTraces, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindCallTraces(u, s, txc.Tx, callTraces, ctx, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneCallTraces(p, tx, callTraces, ctx, logger)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindAccountHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneAccountHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindStorageHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneStorageHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnLogIndex(s, txc.Tx, logIndex, ctx, 0, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindLogIndex(u, s, txc.Tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneLogIndex(p, tx, logIndex, ctx, logger)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneTxLookup(p, tx, txLookup, ctx, firstCycle, logger)
			},
		},
		{
			ID:          stages2.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, _ stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.FinishForward(s, txc.Tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func trieConfigSequencer(zkInterHashesCfg ZkInterHashesCfg) stages.TrieCfg {
	return stages.StageTrieCfg(
		zkInterHashesCfg.db,
		false, // we can't check the root on sequencer because hash stage is done mid-execution.
		zkInterHashesCfg.saveNewHashesToDB,
		zkInterHashesCfg.badBlockHalt,
		zkInterHashesCfg.tmpDir,
		zkInterHashesCfg.blockReader,
		zkInterHashesCfg.hd,
		zkInterHashesCfg.historyV3,
		zkInterHashesCfg.agg)
}

func trieConfigRPC(zkInterHashesCfg ZkInterHashesCfg) stages.TrieCfg {
	return stages.StageTrieCfg(
		zkInterHashesCfg.db,
		zkInterHashesCfg.checkRoot,
		zkInterHashesCfg.saveNewHashesToDB,
		zkInterHashesCfg.badBlockHalt,
		zkInterHashesCfg.tmpDir,
		zkInterHashesCfg.blockReader,
		zkInterHashesCfg.hd,
		zkInterHashesCfg.historyV3,
		zkInterHashesCfg.agg)
}

func DefaultZkStages(
	ctx context.Context,
	l1SyncerCfg L1SyncerCfg,
	l1InfoTreeCfg L1InfoTreeCfg,
	batchesCfg BatchesCfg,
	dataStreamCatchupCfg DataStreamCatchupCfg,
	blockHashCfg stages.BlockHashesCfg,
	senders stages.SendersCfg,
	exec stages.ExecuteBlockCfg,
	hashState stages.HashStateCfg,
	zkInterHashesCfg ZkInterHashesCfg,
	stageWitnessCfg WitnessCfg,
	history stages.HistoryCfg,
	logIndex stages.LogIndexCfg,
	callTraces stages.CallTracesCfg,
	txLookup stages.TxLookupCfg,
	finish stages.FinishCfg,
	test bool,
	chainCfg *chain.Config,
) []*stages.Stage {
	return []*stages.Stage{
		{
			ID:          stages2.L1Syncer,
			Description: "Download L1 Verifications",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageL1Syncer(s, u, ctx, txc.Tx, l1SyncerCfg, test)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindL1SyncerStage(u, txc.Tx, l1SyncerCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneL1SyncerStage(p, tx, l1SyncerCfg, ctx)
			},
		},
		{
			ID:          stages2.L1InfoTree,
			Description: "L1 Info tree index updates sync",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnL1InfoTreeStage(s, u, txc.Tx, l1InfoTreeCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindL1InfoTreeStage(u, txc.Tx, l1InfoTreeCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneL1InfoTreeStage(p, tx, l1InfoTreeCfg, ctx)
			},
		},
		{
			ID:          stages2.Batches,
			Description: "Download batches",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageBatches(s, u, ctx, txc.Tx, batchesCfg)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindBatchesStage(u, txc.Tx, batchesCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneBatchesStage(p, tx, batchesCfg, ctx)
			},
		},
		{
			ID:          stages2.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnBlockHashStage(s, txc.Tx, blockHashCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindBlockHashStage(u, txc.Tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages2.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages2.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnExecuteBlocksStageZk(s, u, txc.Tx, 0, ctx, exec, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindExecutionStageZk(u, s, txc.Tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneExecutionStageZk(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:          stages2.HashState,
			Description: "Hash the key in the state",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnHashStateStage(s, txc.Tx, hashState, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindHashStateStage(u, s, txc.Tx, hashState, ctx, logger, false)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages2.IntermediateHashesStandalone,
			Description: "Standalone intermediate hashes for the PMT",
			Disabled:    !zkInterHashesCfg.zk.SimultaneousPmtAndSmt,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				// This should be false because we are using the SMT but building the PMT as a standalone.
				zkInterHashesCfg.checkRoot = false
				_, err := stages.SpawnIntermediateHashesStage(s, u, txc.Tx, trieConfigRPC(zkInterHashesCfg), ctx, logger)
				return err
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				zkInterHashesCfg.checkRoot = false
				return stages.UnwindIntermediateHashesStage(u, s, txc.Tx, trieConfigRPC(zkInterHashesCfg), ctx, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages2.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if chainCfg.IsPmtEnabled(s.BlockNumber) {
					_, err := stages.SpawnIntermediateHashesStage(s, u, txc.Tx, trieConfigRPC(zkInterHashesCfg), ctx, logger)
					return err
				}

				_, err := SpawnZkIntermediateHashesStage(s, u, txc.Tx, zkInterHashesCfg, ctx)
				return err
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				if chainCfg.PmtEnabledBlock == nil {
					// PMT was never enabled so use SMT for unwind
					return UnwindZkIntermediateHashesStage(u, s, txc.Tx, zkInterHashesCfg, ctx, false)
				} else if chainCfg.PmtEnabledBlock == big.NewInt(0) {
					// PMT was enabled at genesis so use PMT for unwind
					return stages.UnwindIntermediateHashesStage(u, s, txc.Tx, trieConfigRPC(zkInterHashesCfg), ctx, logger)
				} else {
					// PMT has been enabled, but we must check if we are unwinding past the PMT enabled block
					if big.NewInt(int64(u.UnwindPoint)).Cmp(chainCfg.PmtEnabledBlock) < 0 {
						// Unwind point is lower than PMT enabled block
						// This means we are trying to unwind past the PmtEnabledBlock
						// We do not support this as of now, so we must panic
						panic(fmt.Sprintf("Unwind point %d is lower than PMT enabled block %d. Unwinding past PMT enabled block is not supported.", u.UnwindPoint, chainCfg.PmtEnabledBlock))
					}
					// All is good, we can use PMT for unwind
					return stages.UnwindIntermediateHashesStage(u, s, txc.Tx, trieConfigRPC(zkInterHashesCfg), ctx, logger)
				}
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				// TODO: implement this in zk interhashes
				return nil
			},
		},
		{
			ID:                  stages2.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnCallTraces(s, txc.Tx, callTraces, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindCallTraces(u, s, txc.Tx, callTraces, ctx, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneCallTraces(p, tx, callTraces, ctx, logger)
			},
		},
		{
			ID:          stages2.AccountHistoryIndex,
			Description: "Generate account history index",
			Disabled:    false,

			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnAccountHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindAccountHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneAccountHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages2.StorageHistoryIndex,
			Description: "Generate storage history index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnStorageHistoryIndex(s, txc.Tx, history, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindStorageHistoryIndex(u, s, txc.Tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneStorageHistoryIndex(p, tx, history, ctx, logger)
			},
		},
		{
			ID:          stages2.LogIndex,
			Description: "Generate receipt logs index",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnLogIndex(s, txc.Tx, logIndex, ctx, 0, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindLogIndex(u, s, txc.Tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneLogIndex(p, tx, logIndex, ctx, logger)
			},
		},
		{
			ID:          stages2.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.SpawnTxLookup(s, txc.Tx, 0 /* toBlock */, txLookup, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindTxLookup(u, s, txc.Tx, txLookup, ctx, logger)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneTxLookup(p, tx, txLookup, ctx, firstCycle, logger)
			},
		},
		{
			ID:          stages2.DataStream,
			Description: "Update the data stream with missing details",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnStageDataStreamCatchup(s, ctx, txc.Tx, dataStreamCatchupCfg)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages2.Witness,
			Description: "Generate witness caches for each block",
			Disabled:    false,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, u stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnStageWitness(s, u, ctx, txc.Tx, stageWitnessCfg)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindWitnessStage(u, txc.Tx, stageWitnessCfg, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return PruneWitnessStage(p, tx, stageWitnessCfg, ctx)
			},
		},
		{
			ID:          stages2.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stages.StageState, _ stages.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return stages.FinishForward(s, txc.Tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *stages.UnwindState, s *stages.StageState, txc wrap.TxContainer, logger log.Logger) error {
				return stages.UnwindFinish(u, txc.Tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *stages.PruneState, tx kv.RwTx, logger log.Logger) error {
				return stages.PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

var AllStagesZk = []stages2.SyncStage{
	stages2.L1Syncer,
	stages2.Batches,
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
	stages2.Senders,
	stages2.BlockHashes,
	stages2.Batches,
	stages2.L1Syncer,
	stages2.Finish,
}
