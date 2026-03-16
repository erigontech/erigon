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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// fakeTxnProvider is a test double for txnprovider.TxnProvider that serves
// a pre-loaded slice of transactions, respecting the WithAmount option.
type fakeTxnProvider struct {
	txns []types.Transaction
	idx  int
}

func (f *fakeTxnProvider) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o := txnprovider.ApplyProvideOptions(opts...)
	remaining := f.txns[f.idx:]
	count := min(o.Amount, len(remaining))
	result := make([]types.Transaction, count)
	copy(result, remaining[:count])
	f.idx += count
	return result, nil
}

func TestFakeTxnProviderRespectsAmount(t *testing.T) {
	t.Parallel()

	// Create 5 dummy transactions (simple legacy txns with different nonces).
	txns := make([]types.Transaction, 5)
	for i := range txns {
		txns[i] = types.NewTransaction(uint64(i), common.Address{1}, nil, 21000, nil, nil)
	}

	provider := &fakeTxnProvider{txns: txns}

	// First call: request 3, should get 3.
	got, err := provider.ProvideTxns(context.Background(), txnprovider.WithAmount(3))
	require.NoError(t, err)
	require.Len(t, got, 3)

	// Second call: request 3, should get remaining 2.
	got, err = provider.ProvideTxns(context.Background(), txnprovider.WithAmount(3))
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Third call: request any, should get 0.
	got, err = provider.ProvideTxns(context.Background(), txnprovider.WithAmount(10))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestFakeTxnProviderDefaultAmount(t *testing.T) {
	t.Parallel()

	txns := make([]types.Transaction, 3)
	for i := range txns {
		txns[i] = types.NewTransaction(uint64(i), common.Address{1}, nil, 21000, nil, nil)
	}

	provider := &fakeTxnProvider{txns: txns}

	// With no options, default Amount is math.MaxInt — should return all.
	got, err := provider.ProvideTxns(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 3)
}
