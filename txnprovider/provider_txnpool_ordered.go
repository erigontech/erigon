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

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

const orderedTxnPoolProviderPriority = 100

var _ TxnProvider = OrderedTxnPoolProvider{}

// OrderedTxnPoolProvider provides transactions from the devp2p transaction pool by following the ordering function
// described at: https://github.com/erigontech/erigon/tree/main/txnprovider/txpool#ordering-function
type OrderedTxnPoolProvider struct {
	txnPool *txpool.TxPool
}

func NewOrderedTxnPoolProvider(txnPool *txpool.TxPool) OrderedTxnPoolProvider {
	return OrderedTxnPoolProvider{
		txnPool: txnPool,
	}
}

func (p OrderedTxnPoolProvider) Priority() uint64 {
	return orderedTxnPoolProviderPriority
}

func (p OrderedTxnPoolProvider) Yield(ctx context.Context, opts ...YieldOption) ([]types.Transaction, error) {
	params := yieldParamsFromOptions(opts...)
	return p.txnPool.YieldBestTxns(
		ctx,
		params.Amount,
		params.ParentBlockNum,
		params.GasTarget,
		params.BlobGasTarget,
		params.TxnIdsFilter,
	)
}
