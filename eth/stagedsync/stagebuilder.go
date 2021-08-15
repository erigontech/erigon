package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type ChainEventNotifier interface {
	OnNewHeader(*types.Header)
	OnNewPendingLogs(types.Logs)
}

type Notifications struct {
	Events               *privateapi.Events
	Accumulator          *shards.Accumulator
	StateChangesConsumer shards.StateChangeConsumer
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
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnMiningCreateBlockStage(s, tx, createBlockCfg, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          stages.MiningExecution,
			Description: "Mining: construct new block from tx pool",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnMiningExecStage(s, tx, execCfg, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnHashStateStage(s, tx, hashStateCfg, ctx)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				stateRoot, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx)
				if err != nil {
					return err
				}
				createBlockCfg.miner.MiningBlock.Header.Root = stateRoot
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          stages.MiningFinish,
			Description: "Mining: create and propagate valid block",
			Forward: func(firstCycle bool, s *StageState, u Unwinder, tx kv.RwTx) error {
				return SpawnMiningFinishStage(s, tx, finish, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx) error { return nil },
		},
	}
}
