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

package txnprovider

import (
	"context"
	"math"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
)

type TxnProvider interface {
	// ProvideTxns provides transactions ready to be included in a block for block building. Available request options:
	//   - WithParentBlockNum
	//   - WithAmount
	//   - WithGasTarget
	//   - WithTxnIdsFilter
	//   - WithIncludedTxnIds
	//   - WithAvailableRlpSpace
	ProvideTxns(ctx context.Context, opts ...ProvideOption) ([]types.Transaction, error)
}

// RevisionedTxnProvider reports a request-scoped token for block-building input.
type RevisionedTxnProvider interface {
	TxnProvider
	TransactionSetRevision(blockTime, parentBlockNum uint64) uint64
}

type TransactionPolicy struct {
	RequiresSuccess bool
	Dependency      common.Hash
}

type txnRevisionObserverKey struct{}
type txnPolicyObserverKey struct{}

// WithTxnRevisionObserver records the provider snapshot used by a block build.
func WithTxnRevisionObserver(ctx context.Context, observer func(uint64)) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, txnRevisionObserverKey{}, observer)
}

// ObserveTxnRevision reports the transaction snapshot returned by a provider.
func ObserveTxnRevision(ctx context.Context, revision uint64) {
	observer, ok := ctx.Value(txnRevisionObserverKey{}).(func(uint64))
	if ok {
		observer(revision)
	}
}

func WithTxnPolicyObserver(ctx context.Context, observer func(map[common.Hash]TransactionPolicy)) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, txnPolicyObserverKey{}, observer)
}

func ObserveTxnPolicies(ctx context.Context, policies map[common.Hash]TransactionPolicy) {
	observer, ok := ctx.Value(txnPolicyObserverKey{}).(func(map[common.Hash]TransactionPolicy))
	if ok {
		observer(policies)
	}
}

type ProvideOption func(opt *ProvideOptions)

func WithParentBlockNum(blockNum uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.ParentBlockNum = blockNum
	}
}

func WithBlockTime(blockTime uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.BlockTime = blockTime
	}
}

func WithTargetSlot(slot uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.TargetSlot = slot
	}
}

func WithTargetParentHash(parentHash common.Hash) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.TargetParentHash = parentHash
	}
}

func WithTargetGeneration(generation uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.TargetGeneration = generation
	}
}

func WithAmount(amount int) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.Amount = amount
	}
}

func WithGasTarget(gasTarget mdgas.FullMdGas) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.GasTarget = gasTarget
	}
}

func WithTxnIdsFilter(txnIdsFilter mapset.Set[[32]byte]) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.TxnIdsFilter = txnIdsFilter
	}
}

func WithIncludedTxnIds(includedTxnIds mapset.Set[[32]byte]) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.IncludedTxnIds = includedTxnIds
	}
}

func WithAvailableRlpSpace(size int) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.AvailableRlpSpace = size
	}
}

type ProvideOptions struct {
	BlockTime         uint64
	ParentBlockNum    uint64
	TargetSlot        uint64
	TargetParentHash  common.Hash
	TargetGeneration  uint64
	Amount            int
	GasTarget         mdgas.FullMdGas
	TxnIdsFilter      mapset.Set[[32]byte]
	IncludedTxnIds    mapset.Set[[32]byte]
	AvailableRlpSpace int
}

func ApplyProvideOptions(opts ...ProvideOption) ProvideOptions {
	config := defaultProvideOptions
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

var defaultProvideOptions = ProvideOptions{
	ParentBlockNum:    0,           // no parent block to wait for by default
	Amount:            math.MaxInt, // all transactions by default
	GasTarget:         mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, math.MaxUint64),
	TxnIdsFilter:      nil, // no filter by default
	IncludedTxnIds:    nil,
	AvailableRlpSpace: math.MaxInt, // unlimited by default
}
