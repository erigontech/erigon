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

	"github.com/erigontech/erigon-lib/types"
)

type TxnProvider interface {
	// ProvideTxns provides transactions ready to be included in a block for block building. Available request options:
	//   - WithParentBlockNum
	//   - WithAmount
	//   - WithGasTarget
	//   - WithBlobGasTarget
	//   - WithTxnIdsFilter
	ProvideTxns(ctx context.Context, opts ...ProvideOption) ([]types.Transaction, error)
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

func WithAmount(amount int) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.Amount = amount
	}
}

func WithGasTarget(gasTarget uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.GasTarget = gasTarget
	}
}

func WithBlobGasTarget(blobGasTarget uint64) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.BlobGasTarget = blobGasTarget
	}
}

func WithTxnIdsFilter(txnIdsFilter mapset.Set[[32]byte]) ProvideOption {
	return func(opt *ProvideOptions) {
		opt.TxnIdsFilter = txnIdsFilter
	}
}

type ProvideOptions struct {
	BlockTime      uint64
	ParentBlockNum uint64
	Amount         int
	GasTarget      uint64
	BlobGasTarget  uint64
	TxnIdsFilter   mapset.Set[[32]byte]
}

func ApplyProvideOptions(opts ...ProvideOption) ProvideOptions {
	config := defaultProvideOptions
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

var defaultProvideOptions = ProvideOptions{
	ParentBlockNum: 0,                         // no parent block to wait for by default
	Amount:         math.MaxInt,               // all transactions by default
	GasTarget:      math.MaxUint64,            // all transactions by default
	BlobGasTarget:  math.MaxUint64,            // all transactions by default
	TxnIdsFilter:   mapset.NewSet[[32]byte](), // no filter by default
}
