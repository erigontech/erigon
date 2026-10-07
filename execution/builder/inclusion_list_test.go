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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type recordingTxnProvider struct {
	txns []types.Transaction
	opts txnprovider.ProvideOptions
}

func (p *recordingTxnProvider) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	p.opts = txnprovider.ApplyProvideOptions(opts...)
	return p.txns, nil
}

func TestBuildInclusionListRequestsSpecLimits(t *testing.T) {
	t.Parallel()

	txns := []types.Transaction{types.NewTransaction(0, common.Address{1}, nil, 21_000, nil, nil)}
	provider := &recordingTxnProvider{txns: txns}
	b := &Builder{txnProvider: provider}

	got, err := b.BuildInclusionList(t.Context())
	require.NoError(t, err)
	require.Equal(t, types.Transactions(txns), got)
	require.Equal(t, int(params.MaxTransactionsBytesPerInclusionListEIP7805), provider.opts.AvailableRlpSpace)
	require.Zero(t, provider.opts.GasTarget.Blob, "blob transactions must not be selected")
	require.Equal(t, uint64(math.MaxUint64), provider.opts.GasTarget.Execution)
	require.Equal(t, uint64(math.MaxUint64), provider.opts.GasTarget.State)
}

func TestBuildInclusionListWithoutTxnProviderFails(t *testing.T) {
	t.Parallel()

	b := &Builder{}

	got, err := b.BuildInclusionList(t.Context())
	require.ErrorIs(t, err, txpool.ErrPoolDisabled)
	require.Nil(t, got)
}
