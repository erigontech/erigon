package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/sync_stages"
)

type ChainEventNotifier interface {
	OnNewHeader(newHeadersRlp [][]byte)
	OnNewPendingLogs(types.Logs)
	OnLogs([]*remote.SubscribeLogsReply)
	HasLogSubsriptions() bool
}

func MiningStages(
	ctx context.Context,
	createBlockCfg MiningCreateBlockCfg,
	execCfg MiningExecCfg,
	hashStateCfg HashStateCfg,
	trieCfg TrieCfg,
	finish MiningFinishCfg,
) []*sync_stages.Stage {
	return []*sync_stages.Stage{
		{
			ID:          sync_stages.MiningCreateBlock,
			Description: "Mining: construct new block from tx pool",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnMiningCreateBlockStage(s, tx, createBlockCfg, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, u *sync_stages.PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          sync_stages.MiningExecution,
			Description: "Mining: construct new block from tx pool",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnMiningExecStage(s, tx, execCfg, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, u *sync_stages.PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          sync_stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnHashStateStage(s, tx, hashStateCfg, ctx, quiet)
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, u *sync_stages.PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          sync_stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				stateRoot, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx, quiet)
				if err != nil {
					return err
				}
				createBlockCfg.miner.MiningBlock.Header.Root = stateRoot
				return nil
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, u *sync_stages.PruneState, tx kv.RwTx) error { return nil },
		},
		{
			ID:          sync_stages.MiningFinish,
			Description: "Mining: create and propagate valid block",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *sync_stages.StageState, u sync_stages.Unwinder, tx kv.RwTx, quiet bool) error {
				return SpawnMiningFinishStage(s, tx, finish, ctx.Done())
			},
			Unwind: func(firstCycle bool, u *sync_stages.UnwindState, s *sync_stages.StageState, tx kv.RwTx) error {
				return nil
			},
			Prune: func(firstCycle bool, u *sync_stages.PruneState, tx kv.RwTx) error { return nil },
		},
	}
}
