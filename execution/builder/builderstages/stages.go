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

package builderstages

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

var (
	BuilderCreateBlock stages.SyncStage = "BuilderCreateBlock"
	BuilderExecution   stages.SyncStage = "BuilderExecution"
	BuilderFinish      stages.SyncStage = "BuilderFinish"

	BuilderUnwindOrder = stagedsync.UnwindOrder{} // nothing to unwind in block building - because builder does not commit db changes
	BuilderPruneOrder  = stagedsync.PruneOrder{}  // nothing to prune in block building - because builder does not commit db changes
)

func BuilderStages(
	ctx context.Context,
	createBlockCfg BuilderCreateBlockCfg,
	executeBlockCfg stagedsync.ExecuteBlockCfg,
	sendersCfg stagedsync.SendersCfg,
	execCfg BuilderExecCfg,
	finish BuilderFinishCfg,
) []*stagedsync.Stage {
	return []*stagedsync.Stage{
		{
			ID:          BuilderCreateBlock,
			Description: "Builder: construct new block from txn pool",
			Forward: func(badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnBuilderCreateBlockStage(s, sd, tx, createBlockCfg, ctx.Done(), logger)
			},
			Unwind: func(u *stagedsync.UnwindState, s *stagedsync.StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *stagedsync.PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          BuilderExecution,
			Description: "Builder: execute new block from txn pool",
			Forward: func(badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnBuilderExecStage(ctx, s, sd, tx, execCfg, sendersCfg, executeBlockCfg, logger, nil)
			},
			Unwind: func(u *stagedsync.UnwindState, s *stagedsync.StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *stagedsync.PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          BuilderFinish,
			Description: "Builder: create and propagate valid block",
			Forward: func(badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnBuilderFinishStage(s, sd, tx, finish, ctx.Done(), logger)
			},
			Unwind: func(u *stagedsync.UnwindState, s *stagedsync.StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *stagedsync.PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
	}
}
