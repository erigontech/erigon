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

// Package rpc serves the eez JSON-RPC namespace and renders the EEZ system
// transaction types.
package rpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
	erigonrpc "github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// Namespace is the namespace of the EEZ API.
const Namespace = "eez"

// ErrNoFollower reports a node that runs no follower.
var ErrNoFollower = errors.New("no EEZ follower runs in this node")

var registerOnce sync.Once

// Register renders the system transaction types as the reference clients
// do: the transaction body, the gas, and zero gas price and signature. Type
// 0x45 keeps yParity and an empty access list, type 0x46 yParity. Their
// sourceHash, version and blocks fields are not rendered. The types must be
// registered first.
func Register() {
	registerOnce.Do(func() {
		ethapi.RegisterTxRenderer(protocol.SystemTxType, func(_ types.Transaction, r *ethapi.RPCTransaction) {
			r.GasPrice = hexutil.U256{}
			r.MaxFeePerGas, r.MaxPriorityFeePerGas, r.YParity, r.Accesses = nil, nil, nil, nil
		})
		ethapi.RegisterTxRenderer(protocol.NativeTxType, func(_ types.Transaction, r *ethapi.RPCTransaction) {
			r.GasPrice = hexutil.U256{}
			r.MaxFeePerGas, r.MaxPriorityFeePerGas = nil, nil
		})
		ethapi.RegisterTxRenderer(protocol.HistoryTxType, func(_ types.Transaction, r *ethapi.RPCTransaction) {
			r.GasPrice = hexutil.U256{}
			r.MaxFeePerGas, r.MaxPriorityFeePerGas, r.Accesses = nil, nil, nil
		})
	})
}

// API serves the eez namespace.
type API struct {
	follower atomic.Pointer[follower.Follower]
}

// NewAPI returns an API with no follower attached.
func NewAPI() *API { return &API{} }

// Attach makes the API report on f.
func (a *API) Attach(f *follower.Follower) { a.follower.Store(f) }

// API returns the RPC registration of a.
func (a *API) API() erigonrpc.API {
	return erigonrpc.API{Namespace: Namespace, Version: "1.0", Service: a, Public: true}
}

// Settlement is a batch settlement on L1 and the L2 block it ended at.
type Settlement struct {
	L1Block     hexutil.Uint64 `json:"l1Block"`
	L1BlockHash common.Hash    `json:"l1BlockHash"`
	TxHash      common.Hash    `json:"txHash"`
	L2Block     hexutil.Uint64 `json:"l2Block"`
}

func settlement(r deriver.Record) *Settlement {
	return &Settlement{L1Block: hexutil.Uint64(r.L1Block), L1BlockHash: r.L1BlockHash, TxHash: r.TxHash, L2Block: hexutil.Uint64(r.LastL2Block)}
}

// Anchor is L1's finalized block and the rollup's root there, as the
// follower's boot read them. Root is null when the rollup is not registered
// at that block, l2Block until the local chain holds the root.
type Anchor struct {
	L1Block     hexutil.Uint64  `json:"l1Block"`
	L1BlockHash common.Hash     `json:"l1BlockHash"`
	Root        *common.Hash    `json:"root"`
	L2Block     *hexutil.Uint64 `json:"l2Block"`
}

// SignedHead is the latest head of a signing source: a signer and the
// epoch its signing process started at.
type SignedHead struct {
	Signer  common.Address `json:"signer"`
	EpochMs hexutil.Uint64 `json:"epochMs"`
	Counter hexutil.Uint64 `json:"counter"`
	Number  hexutil.Uint64 `json:"number"`
	Hash    common.Hash    `json:"hash"`
}

// FollowerStatus is the result of eez_followerStatus. OverlayPeers and
// SignedHeads are null without the signed-head overlay, HeadSource while
// nothing has moved the head since boot.
type FollowerStatus struct {
	L1Scanned      hexutil.Uint64       `json:"l1Scanned"`
	L1Finalized    hexutil.Uint64       `json:"l1Finalized"`
	Cursor         hexutil.Uint64       `json:"cursor"`
	Safe           hexutil.Uint64       `json:"safe"`
	Finalized      hexutil.Uint64       `json:"finalized"`
	LastSettlement *Settlement          `json:"lastSettlement"`
	SyncMode       follower.SyncMode    `json:"syncMode"`
	Phase          follower.Phase       `json:"phase"`
	Anchor         *Anchor              `json:"anchor"`
	Peers          hexutil.Uint64       `json:"peers"`
	OverlayPeers   *hexutil.Uint64      `json:"overlayPeers"`
	SignedHeads    []SignedHead         `json:"signedHeads"`
	HeadSource     *follower.HeadSource `json:"headSource"`
}

// FollowerStatus is eez_followerStatus: how far the follower derived, where
// it takes its history and its head from, and its peers.
func (a *API) FollowerStatus(context.Context) (*FollowerStatus, error) {
	f := a.follower.Load()
	if f == nil {
		return nil, ErrNoFollower
	}
	s := f.Status()
	out := &FollowerStatus{
		L1Scanned:   hexutil.Uint64(s.L1Scanned),
		L1Finalized: hexutil.Uint64(s.L1Finalized),
		Cursor:      hexutil.Uint64(s.Cursor),
		Safe:        hexutil.Uint64(s.Safe),
		Finalized:   hexutil.Uint64(s.Finalized),
		SyncMode:    s.Mode,
		Phase:       s.Phase,
		Peers:       hexutil.Uint64(s.Peers),
	}
	if s.Last != nil {
		out.LastSettlement = settlement(*s.Last)
	}
	if anchor := s.Anchor; anchor != nil {
		out.Anchor = &Anchor{L1Block: hexutil.Uint64(anchor.L1Block), L1BlockHash: anchor.L1BlockHash, Root: anchor.Root}
		if anchor.L2Block != nil {
			n := hexutil.Uint64(*anchor.L2Block)
			out.Anchor.L2Block = &n
		}
	}
	if s.OverlayPeers != nil {
		n := hexutil.Uint64(*s.OverlayPeers)
		out.OverlayPeers = &n
	}
	if s.SignedHeads != nil {
		out.SignedHeads = make([]SignedHead, len(s.SignedHeads))
		for i, h := range s.SignedHeads {
			out.SignedHeads[i] = SignedHead{
				Signer:  h.Signer,
				EpochMs: hexutil.Uint64(h.Head.EpochMs),
				Counter: hexutil.Uint64(h.Head.Counter),
				Number:  hexutil.Uint64(h.Head.Number),
				Hash:    h.Head.BlockHash,
			}
		}
	}
	if s.HeadSource != "" {
		out.HeadSource = &s.HeadSource
	}
	return out, nil
}

// Settlement is eez_settlement: the settlement that settled L2 block
// number, null for genesis, while none has, and when it predates the
// follower's start or the latest finalized settlement.
func (a *API) Settlement(_ context.Context, number hexutil.Uint64) (*Settlement, error) {
	f := a.follower.Load()
	if f == nil {
		return nil, ErrNoFollower
	}
	r, ok := f.Deriver().Settlement(uint64(number))
	if !ok || number == 0 {
		return nil, nil
	}
	return settlement(r), nil
}

// DecodeBatch is eez_decodeBatch: the batch a postAndVerifyBatch input
// posts.
func (a *API) DecodeBatch(_ context.Context, input hexutil.Bytes) (*Batch, error) {
	return DecodeBatch(input)
}
