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

package composite

import (
	"context"
	"math"
	"slices"

	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon/txnprovider"
)

var _ txnprovider.TxnProvider = TxnProvider{}

// TxnProvider provides the ability to fetch transactions from multiple transaction providers. It uses the
// priority of each provider to impose an ordering of which provider to use first. Once a provider is exhausted (i.e.
// it has no more transactions to yield) then the composite provider moves on to the next lower priority provider
// and continues doing this until all providers are exhausted or the relevant amount of transactions, gas target,
// blob gas target, etc. have been reached.
type TxnProvider struct {
	providers     []txnprovider.TxnProvider
	totalPriority int
}

func NewTxnProvider(providers []txnprovider.TxnProvider) TxnProvider {
	var totalPriority int
	sorted := make([]txnprovider.TxnProvider, len(providers))
	for i, provider := range providers {
		sorted[i] = provider
		totalPriority += provider.Priority()
	}

	slices.SortFunc(sorted, func(a, b txnprovider.TxnProvider) int {
		return b.Priority() - a.Priority() // descending order
	})

	return TxnProvider{
		providers:     sorted,
		totalPriority: totalPriority,
	}
}

func (p TxnProvider) Priority() int {
	return p.totalPriority
}

func (p TxnProvider) Yield(ctx context.Context, opts ...txnprovider.YieldOption) (txnprovider.YieldResult, error) {
	params := txnprovider.YieldParamsFromOptions(opts...)
	overriddenOpts := slices.Clone(opts)
	result := txnprovider.YieldResult{}
	for _, provider := range p.providers {
		subProviderResult, err := provider.Yield(ctx, overriddenOpts...)
		if err != nil {
			return txnprovider.YieldResult{}, err
		}

		result.Transactions = append(result.Transactions, subProviderResult.Transactions...)
		result.TotalGas += subProviderResult.TotalGas
		result.TotalBlobGas += subProviderResult.TotalBlobGas

		// check for early exits before proceeding to next provider
		if len(result.Transactions) == params.Amount {
			return result, nil
		}
		if safeSubToZero(params.GasTarget, result.TotalGas) < fixedgas.TxGas {
			return result, nil
		}

		// override opts for next provider: note we are making use of the fact that if there are 2 opts for the same
		// attribute such as Amount/GasTarget/etc then the latest one ends up getting used, so we simply always keep
		// the original opts and append the overrides to them
		overriddenOpts = overriddenOpts[:len(opts)]
		if params.Amount != math.MaxInt {
			amount := params.Amount - len(result.Transactions)
			overriddenOpts = append(overriddenOpts, txnprovider.WithAmount(amount))
		}
		if params.GasTarget != math.MaxUint64 {
			gasTarget := safeSubToZero(params.GasTarget, result.TotalGas)
			overriddenOpts = append(overriddenOpts, txnprovider.WithGasTarget(gasTarget))
		}
		if params.BlobGasTarget != math.MaxUint64 {
			blobGasTarget := safeSubToZero(params.BlobGasTarget, result.TotalBlobGas)
			overriddenOpts = append(overriddenOpts, txnprovider.WithBlobGasTarget(blobGasTarget))
		}
	}

	return result, nil
}

func safeSubToZero(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return 0
}
