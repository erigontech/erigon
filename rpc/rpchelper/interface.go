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

package rpchelper

import (
	"context"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p"
)

// ApiBackend - interface which must be used by API layer
// implementation can work with local Ethereum object or with Remote (grpc-based) one
// this is reason why all methods are accepting context and returning error
type ApiBackend interface {
	Syncing(ctx context.Context) (*remoteproto.SyncingReply, error)
	Etherbase(ctx context.Context) (common.Address, error)
	NetVersion(ctx context.Context) (uint64, error)
	NetPeerCount(ctx context.Context) (uint64, error)
	ProtocolVersion(ctx context.Context) (uint64, error)
	ClientVersion(ctx context.Context) (string, error)
	Subscribe(ctx context.Context, cb func(*remoteproto.SubscribeReply)) error
	SubscribeLogs(ctx context.Context, cb func(*remoteproto.SubscribeLogsReply), requestor *atomic.Value) error
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
	NodeInfo(ctx context.Context, limit uint32) ([]p2p.NodeInfo, error)
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)
	AddPeer(ctx context.Context, url *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error)
	RemovePeer(ctx context.Context, url *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error)
	PendingBlock(ctx context.Context) (*types.Block, error)
}
