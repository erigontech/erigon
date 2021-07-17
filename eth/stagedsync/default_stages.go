package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
)

var DefaultStages2 = map[stages.SyncStage]ExecFunc{
	stages.Headers: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error { return nil },
}

func DefaultStages(ctx context.Context,
	sm ethdb.StorageMode,
	headers HeadersCfg,
	blockHashCfg BlockHashesCfg,
	snapshotHeaders SnapshotHeadersCfg,
	bodies BodiesCfg,
	snapshotBodies SnapshotBodiesCfg,
	senders SendersCfg,
	exec ExecuteBlockCfg,
	trans TranspileCfg,
	snapshotState SnapshotStateCfg,
	hashState HashStateCfg,
	trieCfg TrieCfg,
	history HistoryCfg,
	logIndex LogIndexCfg,
	callTraces CallTracesCfg,
	txLookup TxLookupCfg,
	txPool TxPoolCfg,
	finish FinishCfg,
	test bool,
) []*Stage {
	return []*Stage{
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return HeadersForward(s, u, ctx, tx, headers, firstCycle, test)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return HeadersUnwind(u, s, tx, headers)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return HeadersPrune(p, tx, headers, ctx)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBlockHashStage(u, tx, blockHashCfg, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneBlockHashStage(p, tx, blockHashCfg, ctx)
			},
		},
		{
			ID:                  stages.CreateHeadersSnapshot,
			Description:         "Create headers snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnHeadersSnapshotGenerationStage(s, tx, snapshotHeaders, firstCycle, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindHeadersSnapshotGenerationStage(u, tx, snapshotHeaders, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneHeadersSnapshotGenerationStage(p, tx, snapshotHeaders, ctx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return BodiesForward(s, u, ctx, tx, bodies, test)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBodiesStage(u, tx, bodies, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneBodiesStage(p, tx, bodies, ctx)
			},
		},
		{
			ID:                  stages.CreateBodiesSnapshot,
			Description:         "Create bodies snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnBodiesSnapshotGenerationStage(s, tx, snapshotBodies, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBodiesSnapshotGenerationStage(u, tx, snapshotBodies, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneBodiesSnapshotGenerationStage(p, tx, snapshotBodies, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindSendersStage(u, tx, senders, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneSendersStage(p, tx, senders, ctx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneExecutionStage(p, tx, exec, ctx, firstCycle)
			},
		},
		{
			ID:                  stages.Translation,
			Description:         "Transpile marked EVM contracts to TEVM",
			Disabled:            !sm.TEVM,
			DisabledDescription: "Enable by adding `e` to --storage-mode",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnTranspileStage(s, tx, 0, trans, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTranspileStage(u, s, tx, trans, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneTranspileStage(p, tx, trans, firstCycle, ctx)
			},
		},
		{
			ID:                  stages.CreateStateSnapshot,
			Description:         "Create state snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnStateSnapshotGenerationStage(s, tx, snapshotState, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindStateSnapshotGenerationStage(u, tx, snapshotState, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneStateSnapshotGenerationStage(p, tx, snapshotState, ctx)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnHashStateStage(s, tx, hashState, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneHashStateStage(p, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				return err
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindIntermediateHashesStage(u, s, tx, trieCfg, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneIntermediateHashesStage(p, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            !sm.CallTraces,
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnCallTraces(s, tx, callTraces, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindCallTraces(u, s, tx, callTraces, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneCallTraces(p, tx, callTraces, ctx)
			},
		},
		{
			ID:                  stages.AccountHistoryIndex,
			Description:         "Generate account history index",
			Disabled:            !sm.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnAccountHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindAccountHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneAccountHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:                  stages.StorageHistoryIndex,
			Description:         "Generate storage history index",
			Disabled:            !sm.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnStorageHistoryIndex(s, tx, history, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindStorageHistoryIndex(u, s, tx, history, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneStorageHistoryIndex(p, tx, history, ctx)
			},
		},
		{
			ID:                  stages.LogIndex,
			Description:         "Generate receipt logs index",
			Disabled:            !sm.Receipts,
			DisabledDescription: "Enable by adding `r` to --storage-mode",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnLogIndex(s, tx, logIndex, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindLogIndex(u, s, tx, logIndex, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneLogIndex(p, tx, logIndex, ctx)
			},
		},
		{
			ID:                  stages.TxLookup,
			Description:         "Generate tx lookup index",
			Disabled:            !sm.TxIndex,
			DisabledDescription: "Enable by adding `t` to --storage-mode",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnTxLookup(s, tx, txLookup, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTxLookup(u, s, tx, txLookup, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneTxLookup(p, tx, txLookup, ctx)
			},
		},
		{
			ID:          stages.TxPool,
			Description: "Update transaction pool",
			Forward: func(firstCycle bool, s *StageState, _ Unwinder, tx ethdb.RwTx) error {
				return SpawnTxPool(s, tx, txPool, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTxPool(u, s, tx, txPool, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneTxPool(p, tx, txPool, ctx)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			Forward: func(firstCycle bool, s *StageState, _ Unwinder, tx ethdb.RwTx) error {
				return FinishForward(s, tx, finish)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindFinish(u, tx, finish, ctx)
			},
			Prune: func(firstCycle bool, p *PruneState, tx ethdb.RwTx) error {
				return PruneFinish(p, tx, finish, ctx)
			},
		},
	}
}

func DefaultForwardOrder() UnwindOrder {
	return []stages.SyncStage{
		stages.Headers,
		stages.BlockHashes,
		stages.CreateHeadersSnapshot,
		stages.Bodies,
		stages.CreateBodiesSnapshot,
		stages.Senders,
		stages.Execution,
		stages.Translation,
		stages.CreateStateSnapshot,
		stages.HashState,
		stages.IntermediateHashes,
		stages.CallTraces,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.TxLookup,
		stages.TxPool,
		stages.Finish,
	}
}

func DefaultPruningOrder() UnwindOrder {
	return []stages.SyncStage{
		stages.Headers,
		stages.BlockHashes,
		stages.CreateHeadersSnapshot,
		stages.Bodies,
		stages.CreateBodiesSnapshot,
		stages.Senders,
		stages.Execution,
		stages.Translation,
		stages.CreateStateSnapshot,
		stages.HashState,
		stages.IntermediateHashes,
		stages.CallTraces,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.TxLookup,
		stages.TxPool,
		stages.Finish,
	}
}

func DefaultUnwindOrder() UnwindOrder {
	return []stages.SyncStage{
		stages.Headers,
		stages.BlockHashes,
		stages.CreateHeadersSnapshot,
		stages.Bodies,
		stages.CreateBodiesSnapshot,

		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		// also tx pool is before senders because senders unwind is inside cycle transaction
		stages.TxPool,

		stages.Senders,
		stages.Execution,
		stages.Translation,
		stages.CreateStateSnapshot,

		// Unwinding of IHashes needs to happen after unwinding HashState
		stages.IntermediateHashes,
		stages.HashState,

		stages.CallTraces,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.TxLookup,
		stages.Finish,
	}
}
