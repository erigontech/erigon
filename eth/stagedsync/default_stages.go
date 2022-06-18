package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
)

func DefaultStages(ctx context.Context, sm prune.Mode, headers HeadersCfg, cumulativeIndex CumulativeIndexCfg, blockHashCfg BlockHashesCfg, bodies BodiesCfg, issuance IssuanceCfg, senders SendersCfg, exec ExecuteBlockCfg, trans TranspileCfg, hashState HashStateCfg, trieCfg TrieCfg, history HistoryCfg, logIndex LogIndexCfg, callTraces CallTracesCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*Stage {
	return []*Stage{
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				if badBlockUnwind {
					return nil
				}
				return SpawnStageHeaders(s, u, ctx, tx, headers, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return HeadersUnwind(u, s, tx, headers, test)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return HeadersPrune(p, tx, headers, ctx)
			},
		},
		{
			ID:          stages.CumulativeIndex,
			Description: "Write Cumulative Index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnStageCumulativeIndex(cumulativeIndex, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindCumulativeIndexStage(u, cumulativeIndex, tx, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneCumulativeIndexStage(p, tx, ctx)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return BodiesForward(s, u, ctx, tx, bodies, test, firstCycle)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindBodiesStage(u, tx, bodies, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneBodiesStage(p, tx, bodies, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:                  stages.Translation,
			Description:         "Transpile marked EVM contracts to TEVM",
			Disabled:            !sm.Experiments.TEVM,
			DisabledDescription: "Enable by adding `tevm` to --experiments",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnTranspileStage(s, tx, 0, trans, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindTranspileStage(u, s, tx, trans, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneTranspileStage(p, tx, trans, firstCycle, ctx)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnHashStateStage(s, tx, hashState, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				return err
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindIntermediateHashesStage(u, s, tx, trieCfg, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:          stages.AccountHistoryIndex,
			Description: "Generate account history index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages.StorageHistoryIndex,
			Description: "Generate storage history index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:          stages.LogIndex,
			Description: "Generate receipt logs index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnLogIndex(s, tx, logIndex, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate tx lookup index",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnTxLookup(s, tx, 0 /* toBlock */, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneTxLookup(p, tx, txLookup, ctx)
			},
		},
		{
			ID:          stages.Issuance,
			Description: "Issuance computation",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnStageIssuance(issuance, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindIssuanceStage(u, issuance, tx, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneIssuanceStage(p, issuance, tx, ctx)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, _ Unwinder, tx kv.RwTx) error {
				return FinishForward(s, tx, finish, firstCycle)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func StateStages(ctx context.Context, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg, hashState HashStateCfg, trieCfg TrieCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnHashStateStage(s, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				return err
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx) error {
				return nil
			},
		},
	}
}

var DefaultForwardOrder = UnwindOrder{
	stages.Headers,
	stages.BlockHashes,
	stages.Bodies,

	// Stages below don't use Internet
	stages.Senders,
	stages.Execution,
	stages.Translation,
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

	stages.Translation,
	stages.Execution,
	stages.Senders,

	stages.Bodies,
	stages.BlockHashes,
	stages.Headers,
}

var DefaultPruneOrder = PruneOrder{
	stages.Finish,
	stages.TxLookup,
	stages.LogIndex,
	stages.StorageHistoryIndex,
	stages.AccountHistoryIndex,
	stages.CallTraces,

	// Unwinding of IHashes needs to happen after unwinding HashState
	stages.HashState,
	stages.IntermediateHashes,

	stages.Translation,
	stages.Execution,
	stages.Senders,

	stages.Bodies,
	stages.BlockHashes,
	stages.Headers,
}

var MiningUnwindOrder = UnwindOrder{} // nothing to unwind in mining - because mining does not commit db changes
var MiningPruneOrder = PruneOrder{}   // nothing to unwind in mining - because mining does not commit db changes
