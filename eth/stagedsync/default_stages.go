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
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return HeadersForward(s, u, ctx, tx, headers, firstCycle, test)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return HeadersUnwind(u, s, tx, headers)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnBlockHashStage(s, tx, blockHashCfg, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBlockHashStage(u, s, tx, blockHashCfg)
			},
		},
		{
			ID:                  stages.CreateHeadersSnapshot,
			Description:         "Create headers snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnHeadersSnapshotGenerationStage(s, tx, snapshotHeaders, firstCycle, ctx)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindHeadersSnapshotGenerationStage(u, s, tx, snapshotHeaders, ctx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return BodiesForward(s, u, ctx, tx, bodies, test)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBodiesStage(u, s, tx, bodies)
			},
		},
		{
			ID:                  stages.CreateBodiesSnapshot,
			Description:         "Create bodies snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnBodiesSnapshotGenerationStage(s, tx, snapshotBodies, ctx)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindBodiesSnapshotGenerationStage(u, s, tx, snapshotBodies, ctx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnRecoverSendersStage(senders, s, u, tx, 0, ctx)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindSendersStage(u, s, tx, senders)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnExecuteBlocksStage(s, u, tx, 0, ctx, exec, firstCycle)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindExecutionStage(u, s, tx, ctx, exec, firstCycle)
			},
		},
		{
			ID:                  stages.Translation,
			Description:         "Transpile marked EVM contracts to TEVM",
			Disabled:            !sm.TEVM,
			DisabledDescription: "Enable by adding `e` to --storage-mode",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnTranspileStage(s, tx, 0, ctx.Done(), trans)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTranspileStage(u, s, tx, ctx.Done(), trans)
			},
		},
		{
			ID:                  stages.CreateStateSnapshot,
			Description:         "Create state snapshot",
			Disabled:            true,
			DisabledDescription: "Enable by --snapshot.layout",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnStateSnapshotGenerationStage(s, tx, snapshotState, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindStateSnapshotGenerationStage(u, s, tx, snapshotState, ctx.Done())
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnHashStateStage(s, tx, hashState, ctx)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindHashStateStage(u, s, tx, hashState, ctx)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				_, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				return err
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindIntermediateHashesStage(u, s, tx, trieCfg, ctx)
			},
		},
		{
			ID:                  stages.CallTraces,
			Description:         "Generate call traces index",
			DisabledDescription: "Work In Progress",
			Disabled:            !sm.CallTraces,
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnCallTraces(s, tx, ctx.Done(), callTraces)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindCallTraces(u, s, tx, ctx.Done(), callTraces)
			},
		},
		{
			ID:                  stages.AccountHistoryIndex,
			Description:         "Generate account history index",
			Disabled:            !sm.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnAccountHistoryIndex(s, tx, history, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindAccountHistoryIndex(u, s, tx, history, ctx.Done())
			},
		},
		{
			ID:                  stages.StorageHistoryIndex,
			Description:         "Generate storage history index",
			Disabled:            !sm.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnStorageHistoryIndex(s, tx, history, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindStorageHistoryIndex(u, s, tx, history, ctx.Done())
			},
		},
		{
			ID:                  stages.LogIndex,
			Description:         "Generate receipt logs index",
			Disabled:            !sm.Receipts,
			DisabledDescription: "Enable by adding `r` to --storage-mode",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnLogIndex(s, tx, logIndex, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindLogIndex(u, s, tx, logIndex, ctx.Done())
			},
		},
		{
			ID:                  stages.TxLookup,
			Description:         "Generate tx lookup index",
			Disabled:            !sm.TxIndex,
			DisabledDescription: "Enable by adding `t` to --storage-mode",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnTxLookup(s, tx, txLookup, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTxLookup(u, s, tx, txLookup, ctx.Done())
			},
		},
		{
			ID:          stages.TxPool,
			Description: "Update transaction pool",
			ExecFunc: func(firstCycle bool, s *StageState, _ Unwinder, tx ethdb.RwTx) error {
				return SpawnTxPool(s, tx, txPool, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindTxPool(u, s, tx, txPool, ctx.Done())
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			ExecFunc: func(firstCycle bool, s *StageState, _ Unwinder, tx ethdb.RwTx) error {
				return FinishForward(s, tx, finish)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				return UnwindFinish(u, s, tx, finish)
			},
		},
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
