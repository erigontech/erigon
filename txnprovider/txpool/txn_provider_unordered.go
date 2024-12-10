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

var _ txnprovider.TxnProvider = UnorderedTxnProvider{}

// UnorderedTxnProvider provides all the available transactions in the devp2p transaction pool without any ordering.
type UnorderedTxnProvider struct {
	pool *TxPool
}

func (p UnorderedTxnProvider) Priority() int {
	//TODO implement me
	panic("implement me")
}

func (p UnorderedTxnProvider) Yield(_ context.Context, _ ...txnprovider.YieldOption) (txnprovider.YieldResult, error) {
	//TODO implement me
	//     for this we can implement a function YieldAllTxns in txpool.TxPool which uses the p.all attribute
	//     data structure
	panic("implement me")
}
