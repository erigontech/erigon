// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
)

func DefaultStages(ctx context.Context,
	snapshots SnapshotsCfg,
	headers HeadersCfg,
	blockHashCfg BlockHashesCfg,
	bodies BodiesCfg,
	senders SendersCfg,
	exec ExecuteBlockCfg,
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
				return HeadersUnwind(ctx, u, s, txc.Tx, headers, test)
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
				return nil
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
			Description: "Recover senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
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
				return PruneExecutionStage(p, tx, exec, ctx, logger)
			},
		},
		//{
		//	ID:          stages.CustomTrace,
		//	Description: "Re-Execute blocks on history state - with custom tracer",
		//	Disabled:    dbg.StagesOnlyBlocks,
		//	Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return SpawnCustomTrace(s, txc, cfg, ctx, 0, logger)
		//	},
		//	Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return UnwindCustomTrace(u, s, txc, cfg, ctx, logger)
		//	},
		//	Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
		//		cfg := StageCustomTraceCfg(exec.db, exec.prune, exec.dirs, exec.blockReader, exec.chainConfig, exec.engine, exec.genesis, &exec.syncCfg)
		//		return PruneCustomTrace(p, tx, cfg, ctx, logger)
		//	},
		//},
		{
			ID:          stages.TxLookup,
			Description: "Generate txn lookup index",
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

func PipelineStages(ctx context.Context, snapshots SnapshotsCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*Stage {
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
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
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
				return PruneExecutionStage(p, tx, exec, ctx, logger)
			},
		},

		{
			ID:          stages.TxLookup,
			Description: "Generate txn lookup index",
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

// UploaderPipelineStages when uploading - potentially from zero we need to include headers and bodies stages otherwise we won't recover the POW portion of the chain
func UploaderPipelineStages(ctx context.Context, snapshots SnapshotsCfg, headers HeadersCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, bodies BodiesCfg, exec ExecuteBlockCfg, txLookup TxLookupCfg, finish FinishCfg, test bool) []*Stage {
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
				return HeadersUnwind(ctx, u, s, txc.Tx, headers, test)
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
				return nil
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
			Description: "Recover senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
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
				return PruneExecutionStage(p, tx, exec, ctx, logger)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate txn lookup index",
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
func StateStages(ctx context.Context, headers HeadersCfg, bodies BodiesCfg, blockHashCfg BlockHashesCfg, senders SendersCfg, exec ExecuteBlockCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.Headers,
			Description: "Download headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return HeadersUnwind(ctx, u, s, txc.Tx, headers, false)
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
			Description: "Recover senders from txn signatures",
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
				return ForwardPolygonSyncStage(ctx, txc.Tx, s, u, polygonSyncStageCfg)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindPolygonSyncStage(ctx, txc.Tx, u, polygonSyncStageCfg)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				return SpawnRecoverSendersStage(senders, s, u, txc.Tx, 0, ctx, logger)
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				return UnwindSendersStage(u, txc.Tx, senders, ctx)
			},
			Prune: func(p *PruneState, tx kv.RwTx, logger log.Logger) error {
				return nil
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
				return PruneExecutionStage(p, tx, exec, ctx, logger)
			},
		},
		{
			ID:          stages.TxLookup,
			Description: "Generate txn lookup index",
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

func DownloadSyncStages(
	ctx context.Context,
	snapshots SnapshotsCfg,
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
				return nil
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
	//stages.CustomTrace,
	stages.TxLookup,
	stages.Finish,
}

// UnwindOrder represents the order in which the stages needs to be unwound.
// The unwind order is important and not always just stages going backwards.
// Let's say, there is txn pool can be unwound only after execution.
// It's ok to remove some stage from here to disable only unwind of stage
type UnwindOrder []stages.SyncStage
type PruneOrder []stages.SyncStage

var DefaultUnwindOrder = UnwindOrder{
	stages.Finish,
	stages.TxLookup,

	//stages.CustomTrace,
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

	stages.Execution,
	stages.Senders,

	stages.BlockHashes,
}

var StateUnwindOrder = UnwindOrder{
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
