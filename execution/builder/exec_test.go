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
	"maps"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

type filterStateReader map[accounts.Address]accounts.Account

func (r filterStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	account, ok := r[address]
	if !ok {
		return nil, nil
	}
	return &account, nil
}

func (r filterStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (filterStateReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}

func (filterStateReader) ReadAccountCode(accounts.Address) ([]byte, error)  { return nil, nil }
func (filterStateReader) ReadAccountCodeSize(accounts.Address) (int, error) { return 0, nil }
func (filterStateReader) ReadAccountIncarnation(accounts.Address) (uint64, error) {
	return 0, nil
}
func (filterStateReader) SetTrace(bool, string) {}
func (filterStateReader) Trace() bool           { return false }
func (filterStateReader) TracePrefix() string   { return "" }

func (r filterStateReader) UpdateAccountData(address accounts.Address, _ *accounts.Account, account *accounts.Account) error {
	r[address] = *account
	return nil
}
func (filterStateReader) UpdateAccountCode(accounts.Address, uint64, accounts.CodeHash, []byte) error {
	return nil
}
func (r filterStateReader) DeleteAccount(address accounts.Address, _ *accounts.Account) error {
	delete(r, address)
	return nil
}
func (filterStateReader) WriteAccountStorage(accounts.Address, uint64, accounts.StorageKey, uint256.Int, uint256.Int) error {
	return nil
}
func (filterStateReader) CreateContract(accounts.Address) error { return nil }

// fakeTxnProvider is a test double for txnprovider.TxnProvider that serves
// a pre-loaded slice of transactions, respecting the WithAmount option.
type fakeTxnProvider struct {
	txns []types.Transaction
	idx  int
}

type emptyPolicyProvider struct{}

func (emptyPolicyProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	txnprovider.ObserveTxnPolicies(ctx, map[common.Hash]txnprovider.TransactionPolicy{})
	return nil, nil
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

func TestGetNextTransactionsClearsPoliciesMissingFromNextSnapshot(t *testing.T) {
	header := types.NewEmptyHeaderForAssembling()
	header.Number.SetUint64(1)
	header.GasLimit = 1_000_000
	policies := map[common.Hash]txnprovider.TransactionPolicy{
		{0x11}: {RequiresSuccess: true, Dependency: common.Hash{0x22}},
	}
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(observed map[common.Hash]txnprovider.TransactionPolicy) {
		maps.Copy(policies, observed)
	})
	cfg := BuilderExecCfg{
		builderState: BuilderState{
			BuilderConfig: &buildercfg.BuilderConfig{},
			BuiltBlock:    &exec.AssembledBlock{Header: header},
		},
		chainConfig: chain.AllProtocolChanges,
		txnProvider: emptyPolicyProvider{},
	}

	txns, err := getNextTransactions(ctx, cfg, chain.AllProtocolChanges.ChainID, header, protocol.GasUsed{}, 50, 0, mapset.NewSet[[32]byte](), nil, nil, policies, log.Root(), &filtrationStats{})
	require.NoError(t, err)
	require.Empty(t, txns)
	require.Empty(t, policies)
}

func TestFilterBadTransactionsDefersDependentPrivateStateChecks(t *testing.T) {
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = nil
	targetKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	targetSender := crypto.PubkeyToAddress(targetKey.PublicKey)
	privateSender := crypto.PubkeyToAddress(privateKey.PublicKey)
	signer := types.LatestSigner(config)
	target, err := types.SignTx(types.NewTransaction(0, privateSender, uint256.NewInt(1_000_000), 21_000, uint256.NewInt(2), nil), *signer, targetKey)
	require.NoError(t, err)
	privateTxn, err := types.SignTx(types.NewTransaction(0, common.Address{0x44}, uint256.NewInt(1), 21_000, uint256.NewInt(2), nil), *signer, privateKey)
	require.NoError(t, err)
	target.SetSender(accounts.InternAddress(targetSender))
	privateTxn.SetSender(accounts.InternAddress(privateSender))
	targetAccount := accounts.NewAccount()
	targetAccount.Balance.SetUint64(1_000_000_000)
	header := types.NewEmptyHeaderForAssembling()
	header.Number.SetUint64(1)
	header.GasLimit = 1_000_000
	header.BaseFee = uint256.NewInt(1)

	filterState := filterStateReader{accounts.InternAddress(targetSender): targetAccount}
	filtered, err := filterBadTransactions(
		[]types.Transaction{target, privateTxn},
		config.ChainID,
		config,
		1,
		header,
		filterState,
		filterState,
		map[common.Hash]txnprovider.TransactionPolicy{
			privateTxn.Hash(): {RequiresSuccess: true, Dependency: target.Hash()},
		},
		mapset.NewSet[[32]byte](),
		log.Root(),
		&filtrationStats{},
	)
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{target, privateTxn}, filtered)
}

func TestFilterBadTransactionsKeepsDependentAfterRotatedTarget(t *testing.T) {
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = nil
	targetKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	targetSender := crypto.PubkeyToAddress(targetKey.PublicKey)
	privateSender := crypto.PubkeyToAddress(privateKey.PublicKey)
	signer := types.LatestSigner(config)
	target, err := types.SignTx(types.NewTransaction(1, privateSender, uint256.NewInt(1_000_000), 21_000, uint256.NewInt(2), nil), *signer, targetKey)
	require.NoError(t, err)
	privateTxn, err := types.SignTx(types.NewTransaction(0, common.Address{0x44}, uint256.NewInt(1), 21_000, uint256.NewInt(2), nil), *signer, privateKey)
	require.NoError(t, err)
	predecessor, err := types.SignTx(types.NewTransaction(0, common.Address{0x45}, nil, 21_000, uint256.NewInt(2), nil), *signer, targetKey)
	require.NoError(t, err)
	target.SetSender(accounts.InternAddress(targetSender))
	privateTxn.SetSender(accounts.InternAddress(privateSender))
	predecessor.SetSender(accounts.InternAddress(targetSender))
	targetAccount := accounts.NewAccount()
	targetAccount.Balance.SetUint64(1_000_000_000)
	header := types.NewEmptyHeaderForAssembling()
	header.Number.SetUint64(1)
	header.GasLimit = 1_000_000
	header.BaseFee = uint256.NewInt(1)
	filterState := filterStateReader{accounts.InternAddress(targetSender): targetAccount}

	filtered, err := filterBadTransactions(
		[]types.Transaction{target, privateTxn, predecessor},
		config.ChainID,
		config,
		1,
		header,
		filterState,
		filterState,
		map[common.Hash]txnprovider.TransactionPolicy{
			privateTxn.Hash(): {RequiresSuccess: true, Dependency: target.Hash()},
		},
		mapset.NewSet[[32]byte](),
		log.Root(),
		&filtrationStats{},
	)
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{predecessor, target, privateTxn}, filtered)
}

func TestSuccessfulTxnIdsExcludesFailedReceipts(t *testing.T) {
	failed, succeeded := types.NewTransaction(0, common.Address{1}, nil, 21_000, nil, nil), types.NewTransaction(1, common.Address{2}, nil, 21_000, nil, nil)
	included := successfulTxnIds(&exec.AssembledBlock{
		Txns: []types.Transaction{failed, succeeded},
		Receipts: types.Receipts{
			{Status: types.ReceiptStatusFailed},
			{Status: types.ReceiptStatusSuccessful},
		},
	})

	require.False(t, included.Contains(failed.Hash()))
	require.True(t, included.Contains(succeeded.Hash()))
}
