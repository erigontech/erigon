package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
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
	borHeimdallCfg BorHeimdallCfg,
	execCfg MiningExecCfg,
	hashStateCfg HashStateCfg,
	trieCfg TrieCfg,
	finish MiningFinishCfg,
) []*Stage {
	return []*Stage{
		{
			ID:          stages.MiningCreateBlock,
			Description: "Mining: construct new block from tx pool",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnMiningCreateBlockStage(s, tx, createBlockCfg, ctx.Done(), logger)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx, logger log.Logger) error { return nil },
		},
		{
			ID:          stages.BorHeimdall,
			Description: "Download Bor-specific data from Heimdall",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				if badBlockUnwind {
					return nil
				}
				return BorHeimdallForward(s, u, ctx, tx, borHeimdallCfg, true, logger)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				return BorHeimdallUnwind(u, ctx, s, tx, borHeimdallCfg)
			},
			Prune: func(firstCycle bool, p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return BorHeimdallPrune(p, ctx, tx, borHeimdallCfg)
			},
		},
		{
			ID:          stages.MiningExecution,
			Description: "Mining: execute new block from tx pool",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				//fmt.Println("SpawnMiningExecStage")
				//defer fmt.Println("SpawnMiningExecStage", "DONE")
				return SpawnMiningExecStage(s, tx, execCfg, ctx.Done(), logger)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx, logger log.Logger) error { return nil },
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnHashStateStage(s, tx, hashStateCfg, ctx, logger)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx, logger log.Logger) error { return nil },
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				stateRoot, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, ctx, logger)
				if err != nil {
					return err
				}
				createBlockCfg.miner.MiningBlock.Header.Root = stateRoot
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx, logger log.Logger) error { return nil },
		},
		{
			ID:          stages.MiningFinish,
			Description: "Mining: create and propagate valid block",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnMiningFinishStage(s, tx, finish, ctx.Done(), logger)
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error { return nil },
			Prune:  func(firstCycle bool, u *PruneState, tx kv.RwTx, logger log.Logger) error { return nil },
		},
	}
}
