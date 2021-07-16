package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type ChainEventNotifier interface {
	OnNewHeader(*types.Header)
	OnNewPendingLogs(types.Logs)
}

type Notifications struct {
	Events      *remotedbserver.Events
	Accumulator *shards.Accumulator
}

func MiningStages(
	ctx context.Context,
	createBlockCfg MiningCreateBlockCfg,
	execCfg MiningExecCfg,
	hashStateCfg HashStateCfg,
	trieCfg TrieCfg,
	finish MiningFinishCfg,
) []*Stage {
	return []*Stage{
		{
			ID:          stages.MiningCreateBlock,
			Description: "Mining: construct new block from tx pool",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnMiningCreateBlockStage(s, tx, createBlockCfg, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
		},
		{
			ID:          stages.MiningExecution,
			Description: "Mining: construct new block from tx pool",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnMiningExecStage(s, tx, execCfg, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnHashStateStage(s, tx, hashStateCfg, ctx)
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				stateRoot, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				if err != nil {
					return err
				}
				createBlockCfg.miner.MiningBlock.Header.Root = stateRoot
				return nil
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
		},
		{
			ID:          stages.MiningFinish,
			Description: "Mining: create and propagate valid block",
			ExecFunc: func(firstCycle bool, s *StageState, u Unwinder, tx ethdb.RwTx) error {
				return SpawnMiningFinishStage(s, tx, finish, ctx.Done())
			},
			UnwindFunc: func(firstCycle bool, u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
		},
	}
}

// UnwindOrder represents the order in which the stages needs to be unwound.
// Currently it is using indexes of stages, 0-based.
// The unwind order is important and not always just stages going backwards.
// Let's say, there is tx pool (state 10) can be unwound only after execution
// is fully unwound (stages 9...3).
type UnwindOrder []stages.SyncStage

func MiningUnwindOrder() UnwindOrder {
	return []stages.SyncStage{}
}
