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
)

var _ TxnProvider = CompositeTxnProvider{}

// CompositeTxnProvider provides the ability to fetch transactions from multiple transaction providers. It uses the
// priority of each provider to impose an ordering of which provider to use first. Once a provider is exhausted (i.e.
// it has no more transactions to yield) then the composite provider moves on to the next lower priority provider
// and continues doing this until all providers are exhausted or the relevant amount of transactions, gas target,
// blob gas target, etc. have been reached.
type CompositeTxnProvider struct {
}

func (c CompositeTxnProvider) Priority() uint64 {
	//TODO implement me
	panic("implement me")
}

func (c CompositeTxnProvider) Yield(_ context.Context, _ ...YieldOption) ([]types.Transaction, error) {
	//TODO implement me
	panic("implement me")
}
