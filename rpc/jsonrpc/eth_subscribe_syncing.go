// Copyright 2026 The Erigon Authors
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

package jsonrpc

import (
	"context"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type stageProgress struct {
	StageName   string         `json:"stage_name"`
	BlockNumber hexutil.Uint64 `json:"block_number"`
}

type syncingResult struct {
	Syncing       bool            `json:"syncing"`
	StartingBlock hexutil.Uint64  `json:"startingBlock"`
	CurrentBlock  hexutil.Uint64  `json:"currentBlock"`
	HighestBlock  hexutil.Uint64  `json:"highestBlock"`
	Stages        []stageProgress `json:"stages"`
}

func stagesFromReply(reply []*remoteproto.SyncingReply_StageProgress) []stageProgress {
	stages := make([]stageProgress, len(reply))
	for i, stage := range reply {
		stages[i] = stageProgress{StageName: stage.StageName, BlockNumber: hexutil.Uint64(stage.BlockNumber)}
	}
	return stages
}

// syncingPayloadBuilder turns SyncingReply updates into the client-facing
// payload: a syncingResult while syncing, the boolean false once synced.
// startingBlock is captured at the transition into Syncing=true and holds for
// the whole sync session, mirroring the eth_syncing startingBlock field.
type syncingPayloadBuilder struct {
	startingBlock hexutil.Uint64
	lastSyncing   bool
}

func (b *syncingPayloadBuilder) build(reply *remoteproto.SyncingReply) any {
	if reply.Syncing && !b.lastSyncing {
		b.startingBlock = hexutil.Uint64(reply.CurrentBlock)
	}
	b.lastSyncing = reply.Syncing

	if !reply.Syncing {
		return false
	}
	return syncingResult{
		Syncing:       true,
		StartingBlock: b.startingBlock,
		CurrentBlock:  hexutil.Uint64(reply.CurrentBlock),
		HighestBlock:  hexutil.Uint64(reply.LastNewBlockSeen),
		Stages:        stagesFromReply(reply.Stages),
	}
}

// EthSyncingSubscriptionAPI handles the eth_subscribe "syncing" subscription.
// It is registered as a separate "eth" service alongside APIImpl so that both
// eth_syncing (regular RPC) and eth_subscribe "syncing" (subscription) can
// coexist under the same namespace without a Go method-name conflict.
type EthSyncingSubscriptionAPI struct {
	filters *rpchelper.Filters
	logger  log.Logger
}

func NewEthSyncingSubscriptionAPI(filters *rpchelper.Filters, logger log.Logger) *EthSyncingSubscriptionAPI {
	return &EthSyncingSubscriptionAPI{filters: filters, logger: logger}
}

// Syncing implements eth_subscribe "syncing". The first message is the last
// known sync state, then every SYNCING event pushed by the node follows. The
// payload is a syncingResult object when syncing, or the boolean false when
// not.
func (api *EthSyncingSubscriptionAPI) Syncing(ctx context.Context) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	ch, id := api.filters.SubscribeSyncing(8, rpchelper.ProtocolWS)
	rpcSub := notifier.CreateSubscription()

	go func() {
		defer dbg.LogPanic()
		defer api.filters.UnsubscribeSyncing(id)

		var builder syncingPayloadBuilder
		for {
			select {
			case reply, ok := <-ch:
				if !ok {
					return
				}
				if err := notifier.Notify(rpcSub.ID, builder.build(reply)); err != nil {
					api.logger.Warn("[rpc] error while notifying syncing subscription", "err", err)
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
