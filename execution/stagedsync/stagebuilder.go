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
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type ChainEventNotifier interface {
	OnNewHeader(newHeadersRlp [][]byte)
	OnNewPendingLogs(types.Logs)
	OnLogs([]*remoteproto.SubscribeLogsReply)
	HasLogSubscriptions() bool
	OnReceipts([]*remoteproto.SubscribeReceiptsReply)
	HasReceiptSubscriptions() bool
}

func MiningStages(
	ctx context.Context,
	createBlockCfg MiningCreateBlockCfg,
	executeBlockCfg ExecuteBlockCfg,
	sendersCfg SendersCfg,
	execCfg MiningExecCfg,
	finish MiningFinishCfg,
) []*Stage {
	return []*Stage{
		{
			ID:          stages.MiningCreateBlock,
			Description: "Mining: construct new block from txn pool",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnMiningCreateBlockStage(s, sd, tx, createBlockCfg, ctx.Done(), logger)
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.MiningExecution,
			Description: "Mining: execute new block from txn pool",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnMiningExecStage(ctx, s, sd, tx, execCfg, sendersCfg, executeBlockCfg, logger, nil)
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          stages.MiningFinish,
			Description: "Mining: create and propagate valid block",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return SpawnMiningFinishStage(s, sd, tx, finish, ctx.Done(), logger)
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				return nil
			},
			Prune: func(ctx context.Context, p *PruneState, tx kv.RwTx, timeout time.Duration, logger log.Logger) error {
				return nil
			},
		},
	}
}
