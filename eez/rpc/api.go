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

// Package rpc serves the eez JSON-RPC namespace and renders type 0x76.
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

// Register renders type 0x76 as the reference client does: the transaction
// body, the fixed gas, and zero gas price and signature. Transaction type
// 0x76 must be registered first.
func Register() {
	registerOnce.Do(func() {
		ethapi.RegisterTxRenderer(protocol.SystemTxType, func(_ types.Transaction, r *ethapi.RPCTransaction) {
			r.GasPrice = hexutil.U256{}
			r.MaxFeePerGas, r.MaxPriorityFeePerGas, r.YParity, r.Accesses = nil, nil, nil, nil
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

// FollowerStatus is the result of eez_followerStatus.
type FollowerStatus struct {
	L1Scanned      hexutil.Uint64 `json:"l1Scanned"`
	L1Finalized    hexutil.Uint64 `json:"l1Finalized"`
	Cursor         hexutil.Uint64 `json:"cursor"`
	Safe           hexutil.Uint64 `json:"safe"`
	Finalized      hexutil.Uint64 `json:"finalized"`
	LastSettlement *Settlement    `json:"lastSettlement"`
}

// FollowerStatus is eez_followerStatus: how far the follower derived.
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
	}
	if s.Last != nil {
		out.LastSettlement = settlement(*s.Last)
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
