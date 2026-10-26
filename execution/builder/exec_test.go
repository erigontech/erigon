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
	"fmt"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"

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

type frameFilterState struct {
	state.NoopReader
	state.NoopWriter
	account *accounts.Account
}

func (s *frameFilterState) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return s.account, nil
}

func (s *frameFilterState) UpdateAccountData(_ accounts.Address, _, account *accounts.Account) error {
	s.account = account
	return nil
}

func TestFrameFilterAllowsSponsoredSender(t *testing.T) {
	for _, deployed := range []bool{false, true} {
		t.Run(fmt.Sprint(deployed), func(t *testing.T) {
			sim := &frameFilterState{}
			if deployed {
				sim.account = &accounts.Account{CodeHash: accounts.InternCodeHash(common.Hash{1})}
			}
			tx := &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1}, Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}}, Fees: types.FrameFees{FeeCap: *uint256.NewInt(1)}}
			got, err := filterBadTransactions([]types.Transaction{tx}, uint256.NewInt(1), chain.AllProtocolChanges, 1, &types.Header{BaseFee: uint256.NewInt(0)}, sim, sim, log.New(), nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, uint64(1), sim.account.Nonce)
			require.True(t, sim.account.Balance.IsZero())
		})
	}
}

func TestFrameFilterKeyedNonce(t *testing.T) {
	t.Parallel()
	const accountNonce = uint64(7)
	for _, test := range []struct {
		name    string
		seq     uint64
		wrapped bool
	}{
		{name: "below account nonce", seq: 0},
		{name: "equal to account nonce", seq: accountNonce},
		{name: "above account nonce", seq: accountNonce + 1},
		{name: "wrapped frame", seq: 0, wrapped: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sim := &frameFilterState{account: &accounts.Account{Nonce: accountNonce}}
			frame := &types.FrameTransaction{
				NonceKeys: []uint256.Int{{1}}, Nonce: test.seq, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1},
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}},
				Fees:   types.FrameFees{FeeCap: *uint256.NewInt(1)},
			}
			var tx types.Transaction = frame
			if test.wrapped {
				tx = &types.FrameTxWrapper{FrameTransaction: frame}
			}
			got, err := filterBadTransactions([]types.Transaction{tx}, uint256.NewInt(1), chain.AllProtocolChanges, 1, &types.Header{BaseFee: uint256.NewInt(0)}, sim, sim, log.New(), nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Same(t, tx, got[0])
			require.Equal(t, accountNonce, sim.account.Nonce)
		})
	}
}

func TestGetNextTransactionsFrameNonceRetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		key         uint64
		wrapped     bool
		wantYielded bool
	}{
		{name: "legacy nonce gap", key: 0},
		{name: "keyed nonce with low fee", key: 1, wantYielded: true},
		{name: "wrapped keyed nonce with low fee", key: 1, wrapped: true, wantYielded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sim := &frameFilterState{account: &accounts.Account{}}
			frame := &types.FrameTransaction{
				NonceKeys: []uint256.Int{*uint256.NewInt(test.key)}, Nonce: 1, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1},
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}},
				Fees:   types.FrameFees{FeeCap: *uint256.NewInt(1)},
			}
			var tx types.Transaction = frame
			if test.wrapped {
				tx = &types.FrameTxWrapper{FrameTransaction: frame}
			}
			header := &types.Header{GasLimit: 30_000_000, BaseFee: uint256.NewInt(2)}
			cfg := BuilderExecCfg{
				builderState: BuilderState{BuiltBlock: &exec.AssembledBlock{Header: header}},
				chainConfig:  chain.AllProtocolChanges,
				txnProvider:  &fakeTxnProvider{txns: []types.Transaction{tx}},
			}
			yielded := mapset.NewSet[[32]byte](tx.Hash())
			got, err := getNextTransactions(t.Context(), cfg, uint256.NewInt(1), header, protocol.GasUsed{}, 1, 0, yielded, sim, sim, log.New(), nil)
			require.NoError(t, err)
			require.Empty(t, got)
			require.Equal(t, test.wantYielded, yielded.Contains(tx.Hash()))
		})
	}
}
