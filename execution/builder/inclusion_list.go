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

// inclusionListTxnProvider offers the EIP-7805 inclusion list ahead of next. Entries without a
// recoverable sender are dropped; the rest go through the same filtering as pool transactions.
type inclusionListTxnProvider struct {
	inclusionList types.Transactions
	next          txnprovider.TxnProvider
}

func newInclusionListTxnProvider(inclusionList types.Transactions, signer *types.Signer, next txnprovider.TxnProvider) *inclusionListTxnProvider {
	recovered := make(types.Transactions, 0, len(inclusionList))
	for _, txn := range inclusionList {
		if _, err := txn.Sender(*signer); err != nil {
			continue
		}
		recovered = append(recovered, txn)
	}
	return &inclusionListTxnProvider{inclusionList: recovered, next: next}
}

func (p *inclusionListTxnProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	options := txnprovider.ApplyProvideOptions(opts...)
	var txns []types.Transaction
	rlpSpace := options.AvailableRlpSpace
	for _, txn := range p.inclusionList {
		if len(txns) == options.Amount {
			return txns, nil
		}
		if options.TxnIdsFilter != nil {
			if options.TxnIdsFilter.Contains(txn.Hash()) {
				continue
			}
			options.TxnIdsFilter.Add(txn.Hash())
		}
		txns = append(txns, txn)
		rlpSpace -= txn.EncodingSize()
	}
	if p.next == nil || len(txns) == options.Amount {
		return txns, nil
	}
	if len(txns) > 0 {
		opts = append(opts, txnprovider.WithAmount(options.Amount-len(txns)), txnprovider.WithAvailableRlpSpace(rlpSpace))
	}
	rest, err := p.next.ProvideTxns(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return append(txns, rest...), nil
}
