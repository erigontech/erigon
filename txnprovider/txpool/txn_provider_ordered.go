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

package txpool

import (
	"context"

	"github.com/erigontech/erigon/txnprovider"
)

const orderedTxnPoolProviderPriority = 100

var _ txnprovider.TxnProvider = OrderedTxnProvider{}

// OrderedTxnProvider provides transactions from the devp2p transaction pool by following the ordering function
// described at: https://github.com/erigontech/erigon/tree/main/txnprovider/txpool#ordering-function
type OrderedTxnProvider struct {
	pool *TxPool
}

func NewOrderedTxnProvider(txnPool *TxPool) OrderedTxnProvider {
	return OrderedTxnProvider{
		pool: txnPool,
	}
}

func (p OrderedTxnProvider) Priority() int {
	return orderedTxnPoolProviderPriority
}

func (p OrderedTxnProvider) Yield(ctx context.Context, opts ...txnprovider.YieldOption) (txnprovider.YieldResult, error) {
	params := txnprovider.YieldParamsFromOptions(opts...)
	txnsRlp, err := p.pool.YieldBest(ctx, params.Amount, params.ParentBlockNum, params.GasTarget, params.BlobGasTarget, params.TxnIdsFilter)
	if err != nil {
		return txnprovider.YieldResult{}, err
	}

	txns, err := txnsRlp.Transactions()
	if err != nil {
		return txnprovider.YieldResult{}, err
	}

	res := txnprovider.YieldResult{
		Transactions: txns,
		TotalGas:     txnsRlp.TotalGas,
		TotalBlobGas: txnsRlp.TotalBlobGas,
	}

	return res, nil
}
