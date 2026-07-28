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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
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
	SubscribeReceipts(ctx context.Context, cb func(*remoteproto.SubscribeReceiptsReply), onReady func(func(*remoteproto.ReceiptsFilterRequest) error)) error
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
	NodeInfo(ctx context.Context, limit uint32) ([]p2p.NodeInfo, error)
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)
	AddPeer(ctx context.Context, url *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error)
	RemovePeer(ctx context.Context, url *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error)
	AddTrustedPeer(ctx context.Context, url *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error)
	RemoveTrustedPeer(ctx context.Context, url *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error)
	SetHead(ctx context.Context, req *remoteproto.SetHeadRequest) (*remoteproto.SetHeadReply, error)
	PendingBlock(ctx context.Context) (*types.Block, error)
}

// SetForkResult is the debug_setFork RPC's response payload.
// RestartRequired stays true through Phase 1 — the in-process
// chain.Config swap that would let the running process continue on
// the target chain is not yet wired.
type SetForkResult struct {
	FromChain       string `json:"from_chain"`
	ToChain         string `json:"to_chain"`
	UnwoundFrom     uint64 `json:"unwound_from"`
	UnwoundTo       uint64 `json:"unwound_to"`
	RestartRequired bool   `json:"restart_required"`
	Message         string `json:"message,omitempty"`
}

// ForkController is the optional runtime-fork-transition surface a
// backend may implement. jsonrpc.DebugAPIImpl type-asserts its
// ApiBackend to this interface at construction; nil ⇒ debug_setFork
// returns an actionable "not available" error. In-process erigon
// (node/eth.Ethereum) implements it; standalone rpcdaemon does not.
type ForkController interface {
	SetFork(ctx context.Context, targetChainName string) (*SetForkResult, error)
}

// ChainConfigReconfigurable is the primary "component supports
// runtime chain-config swap" contract. debug_setFork walks the
// registered list of Reconfigurables during a fork transition and
// calls Reconfigure(ctx, newCfg) on each. The component
// encapsulates the Stop → swap → Start dance internally — the
// orchestrator can't sequence it incorrectly and the component can
// short-circuit parts of the cycle if the config diff doesn't need
// a full restart.
//
// This is the component-model contract, not an atomic-pointer
// pattern: chainConfig changes are rare, and the "no active work
// during config swap" invariant makes the transition trivial to
// reason about at every captor. debug_setFork tests that the
// component model actually supports this contract end-to-end.
type ChainConfigReconfigurable interface {
	Reconfigure(ctx context.Context, newCfg *chain.Config) error
}

// ChainConfigRestartable is the alternate contract for components
// that expose Stop / SetChainConfig / Start as separate primitives
// (e.g. because a caller needs finer sequencing across multiple
// components or wants to inspect state between the phases).
// Reconfigure and Restartable are mutually exclusive per component
// — the orchestrator type-asserts to figure out which contract a
// component provides. Reconfigurable is preferred; Restartable is
// the escape hatch.
type ChainConfigRestartable interface {
	// Stop halts the component's background goroutines. After
	// Stop returns the component holds no active work and no
	// goroutine reads its captured chain.Config.
	Stop() error
	// SetChainConfig replaces the captured chain.Config pointer.
	// The component must be Stopped when this is called.
	SetChainConfig(cfg *chain.Config)
	// Start relaunches background goroutines on the new
	// chain.Config. Safe after a matching Stop + SetChainConfig.
	Start(ctx context.Context) error
}
