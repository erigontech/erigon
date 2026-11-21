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

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

func signedTransfer(t *testing.T, signer *types.Signer, nonce uint64) types.Transaction {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil), *signer, key)
	require.NoError(t, err)
	return txn
}

func TestInclusionListTxnProviderOffersInclusionListFirst(t *testing.T) {
	t.Parallel()
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	a, b := signedTransfer(t, signer, 0), signedTransfer(t, signer, 0)
	pooled := signedTransfer(t, signer, 0)
	next := &recordingTxnProvider{txns: []types.Transaction{pooled}}
	provider := newInclusionListTxnProvider(types.Transactions{types.CopyTxs(types.Transactions{a})[0], b}, signer, next)
	yielded := mapset.NewSet[[32]byte]()

	got, err := provider.ProvideTxns(t.Context(), txnprovider.WithAmount(3), txnprovider.WithTxnIdsFilter(yielded), txnprovider.WithAvailableRlpSpace(1000))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{a.Hash(), b.Hash(), pooled.Hash()}, txnHashes(got))
	sender, ok := got[0].GetSender()
	require.True(t, ok, "senders must be recovered for the builder's filter")
	wantSender, err := a.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, wantSender, sender)
	require.True(t, yielded.Contains(a.Hash()))
	require.True(t, yielded.Contains(b.Hash()))
	require.Equal(t, 1, next.opts.Amount)
	require.Equal(t, 1000-a.EncodingSize()-b.EncodingSize(), next.opts.AvailableRlpSpace)
}

func TestInclusionListTxnProviderSkipsYieldedTransactions(t *testing.T) {
	t.Parallel()
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	a, b := signedTransfer(t, signer, 0), signedTransfer(t, signer, 0)
	provider := newInclusionListTxnProvider(types.Transactions{a, b}, signer, nil)
	yielded := mapset.NewSet[[32]byte](a.Hash())

	got, err := provider.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(yielded))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{b.Hash()}, txnHashes(got))

	got, err = provider.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(yielded))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestInclusionListTxnProviderRespectsAmount(t *testing.T) {
	t.Parallel()
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	a, b := signedTransfer(t, signer, 0), signedTransfer(t, signer, 0)
	next := &recordingTxnProvider{txns: []types.Transaction{signedTransfer(t, signer, 0)}}
	provider := newInclusionListTxnProvider(types.Transactions{a, b}, signer, next)

	got, err := provider.ProvideTxns(t.Context(), txnprovider.WithAmount(1), txnprovider.WithTxnIdsFilter(mapset.NewSet[[32]byte]()))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{a.Hash()}, txnHashes(got))
	require.Zero(t, next.opts.Amount, "a full batch must not ask the pool")
}

func TestInclusionListTxnProviderDropsTransactionsWithoutSender(t *testing.T) {
	t.Parallel()
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	unsigned := types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil)
	signed := signedTransfer(t, signer, 0)
	provider := newInclusionListTxnProvider(types.Transactions{unsigned, signed}, signer, nil)

	got, err := provider.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(mapset.NewSet[[32]byte]()))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{signed.Hash()}, txnHashes(got))
}

func txnHashes(txns []types.Transaction) []common.Hash {
	hashes := make([]common.Hash, len(txns))
	for i, txn := range txns {
		hashes[i] = txn.Hash()
	}
	return hashes
}
