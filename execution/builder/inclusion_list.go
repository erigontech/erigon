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

package builder

import (
	"context"
	"math"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func (b *Builder) BuildInclusionList(ctx context.Context) (types.Transactions, error) {
	if b.txnProvider == nil {
		return nil, txpool.ErrPoolDisabled
	}

	provideOpts := []txnprovider.ProvideOption{
		txnprovider.WithAvailableRlpSpace(int(params.MaxTransactionsBytesPerInclusionListEIP7805)),
		txnprovider.WithGasTarget(mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, 0)),
	}

	txns, err := b.txnProvider.ProvideTxns(ctx, provideOpts...)
	if err != nil {
		return nil, err
	}
	return types.Transactions(txns), nil
}
