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

	"github.com/erigontech/erigon/core/types"
)

type TxnProvider interface {
	Priority() int
	Yield(ctx context.Context, opts ...YieldOption) (YieldResult, error)
}

type YieldResult struct {
	Transactions []types.Transaction
	TotalGas     uint64
	TotalBlobGas uint64
}

type YieldOption func(opt *YieldParams)

func WithParentBlockNum(blockNum uint64) YieldOption {
	return func(opt *YieldParams) {
		opt.ParentBlockNum = blockNum
	}
}

func WithAmount(amount int) YieldOption {
	return func(opt *YieldParams) {
		opt.Amount = amount
	}
}

func WithGasTarget(gasTarget uint64) YieldOption {
	return func(opt *YieldParams) {
		opt.GasTarget = gasTarget
	}
}

func WithBlobGasTarget(blobGasTarget uint64) YieldOption {
	return func(opt *YieldParams) {
		opt.BlobGasTarget = blobGasTarget
	}
}

func WithTxnIdsFilter(txnIdsFilter mapset.Set[[32]byte]) YieldOption {
	return func(opt *YieldParams) {
		opt.TxnIdsFilter = txnIdsFilter
	}
}

type YieldParams struct {
	ParentBlockNum uint64
	Amount         int
	GasTarget      uint64
	BlobGasTarget  uint64
	TxnIdsFilter   mapset.Set[[32]byte]
}

func YieldParamsFromOptions(opts ...YieldOption) YieldParams {
	config := defaultYieldParams
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

var defaultYieldParams = YieldParams{
	ParentBlockNum: 0,                         // no parent block to wait for by default
	Amount:         math.MaxInt,               // all transactions by default
	GasTarget:      math.MaxUint64,            // all transactions by default
	BlobGasTarget:  math.MaxUint64,            // all transactions by default
	TxnIdsFilter:   mapset.NewSet[[32]byte](), // empty filter by default
}
