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
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func (s *frameFilterState) GetNonce(accounts.Address) (uint64, error) {
	if s.account == nil {
		return 0, nil
	}
	return s.account.Nonce, nil
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
			tx := &types.FrameTransaction{NonceKeys: nil, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1}, Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}}, Fees: types.FrameFees{MaxFee: *uint256.NewInt(1)}}
			got, nonceTooHigh, err := filterBadTransactions([]types.Transaction{tx}, uint256.NewInt(1), chain.AllProtocolChanges, 1, &types.Header{BaseFee: uint256.NewInt(0)}, sim, sim, sim.GetNonce, log.New(), nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Empty(t, nonceTooHigh)
			require.Equal(t, deployed, sim.account != nil)
			if deployed {
				require.Zero(t, sim.account.Nonce)
				require.True(t, sim.account.Balance.IsZero())
			}
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
				Fees:   types.FrameFees{MaxFee: *uint256.NewInt(1)},
			}
			var tx types.Transaction = frame
			if test.wrapped {
				tx = &types.FrameTxWrapper{FrameTransaction: frame}
			}
			got, nonceTooHigh, err := filterBadTransactions([]types.Transaction{tx}, uint256.NewInt(1), chain.AllProtocolChanges, 1, &types.Header{BaseFee: uint256.NewInt(0)}, sim, sim, sim.GetNonce, log.New(), nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Empty(t, nonceTooHigh)
			require.Same(t, tx, got[0])
			require.Equal(t, accountNonce, sim.account.Nonce)
		})
	}
}

func TestGetNextTransactionsFrameNonceRetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		keys         []uint256.Int
		wrapped      bool
		wantNonceGap bool
	}{
		{name: "legacy nonce gap", wantNonceGap: true},
		{name: "keyed nonce with low fee", keys: []uint256.Int{{1}}},
		{name: "zero key with low fee", keys: []uint256.Int{{}}},
		{name: "wrapped keyed nonce with low fee", keys: []uint256.Int{{1}}, wrapped: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sim := &frameFilterState{account: &accounts.Account{}}
			frame := &types.FrameTransaction{
				NonceKeys: test.keys, Nonce: 1, ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1},
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}},
				Fees:   types.FrameFees{MaxFee: *uint256.NewInt(1)},
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
			nonceTooHigh := map[accounts.Address][]nonceTooHighTxn{}
			got, provided, err := getNextTransactions(t.Context(), cfg, uint256.NewInt(1), header, protocol.GasUsed{}, 1, 0, yielded, nonceTooHigh, sim, sim, sim.GetNonce, log.New(), nil)
			require.NoError(t, err)
			require.Empty(t, got)
			require.Equal(t, 1, provided)
			require.True(t, yielded.Contains(tx.Hash()))
			sender, _ := tx.GetSender()
			if test.wantNonceGap {
				require.Equal(t, []nonceTooHighTxn{{hash: tx.Hash(), nonce: tx.GetNonce()}}, nonceTooHigh[sender])
			} else {
				require.Empty(t, nonceTooHigh)
			}
		})
	}
}

func TestFrameFilterBlobFeeBudget(t *testing.T) {
	t.Parallel()
	for _, shortfall := range []uint64{0, 1} {
		t.Run(fmt.Sprint(shortfall), func(t *testing.T) {
			header := &types.Header{Time: 1, BaseFee: uint256.NewInt(2), ExcessBlobGas: new(uint64)}
			blobBaseFee, err := misc.GetBlobGasPrice(chain.AllProtocolChanges, *header.ExcessBlobGas, header.Time)
			require.NoError(t, err)
			frame := &types.FrameTransaction{
				ChainID: *uint256.NewInt(1), SenderAddress: common.Address{1},
				Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100_000, State: 40_000}}},
				BlobVersionedHashes: []common.Hash{{1}},
			}
			frame.Fees.MaxFee.Mul(uint256.NewInt(frame.GetGasLimit()), header.BaseFee)
			blobCost := new(uint256.Int).Mul(uint256.NewInt(frame.GetBlobGas()), &blobBaseFee)
			frame.Fees.MaxFee.Add(&frame.Fees.MaxFee, blobCost)
			frame.Fees.MaxFee.SubUint64(&frame.Fees.MaxFee, shortfall)
			frame.Fees.TipCap.AddUint64(&frame.Fees.MaxFee, 1)
			sim := &frameFilterState{account: &accounts.Account{}}
			got, _, err := filterBadTransactions([]types.Transaction{frame}, &frame.ChainID, chain.AllProtocolChanges, 1, header, sim, sim, sim.GetNonce, log.New(), nil)
			require.NoError(t, err)
			if shortfall == 0 {
				require.Equal(t, []types.Transaction{frame}, got)
			} else {
				require.Empty(t, got)
			}
		})
	}
}

func TestGetNextTransactionsFrameReleasesNonceGap(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		keys      []uint256.Int
		wrapped   bool
		postNonce uint64
	}{
		{name: "legacy nonce", postNonce: 1},
		{name: "legacy nonce with create", postNonce: 2},
		{name: "rejected legacy frame"},
		{name: "keyed nonce", keys: []uint256.Int{{1}}},
		{name: "keyed nonce with create", keys: []uint256.Int{{1}}, postNonce: 1},
		{name: "wrapped keyed nonce with create", keys: []uint256.Int{{1}}, wrapped: true, postNonce: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender := accounts.InternAddress(common.Address{1})
			st := newFundedState(sender)
			gapped := newTxnFrom(sender, 1, params.TxGas)
			frame := &types.FrameTransaction{
				NonceKeys: test.keys, ChainID: *chain.AllProtocolChanges.ChainID, SenderAddress: sender.Value(),
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}},
			}
			frame.Fees.MaxFee.SetUint64(frame.GetGasLimit())
			var tx types.Transaction = frame
			if test.wrapped {
				tx = &types.FrameTxWrapper{FrameTransaction: frame}
			}
			header := &types.Header{GasLimit: 30_000_000, BaseFee: uint256.NewInt(1)}
			cfg := BuilderExecCfg{
				builderState: BuilderState{BuiltBlock: &exec.AssembledBlock{Header: header}},
				chainConfig:  chain.AllProtocolChanges,
				txnProvider:  &prioritisedProvider{ordered: []types.Transaction{gapped, tx}},
			}
			yielded := mapset.NewSet[[32]byte]()
			nonceTooHigh := map[accounts.Address][]nonceTooHighTxn{}
			got, _, err := getNextTransactions(t.Context(), cfg, cfg.chainConfig.ChainID, header, protocol.GasUsed{}, 1, 0, yielded, nonceTooHigh, st, st, st.GetNonce, log.New(), nil)
			require.NoError(t, err)
			require.Empty(t, got)
			require.Equal(t, []nonceTooHighTxn{{hash: gapped.Hash(), nonce: gapped.GetNonce()}}, nonceTooHigh[sender])
			got, _, err = getNextTransactions(t.Context(), cfg, cfg.chainConfig.ChainID, header, protocol.GasUsed{}, 1, 0, yielded, nonceTooHigh, st, st, st.GetNonce, log.New(), nil)
			require.NoError(t, err)
			require.Equal(t, []types.Transaction{tx}, got)
			require.True(t, yielded.Contains(gapped.Hash()), "selection alone must not release the successor")
			st.accounts[sender].Nonce = test.postNonce
			retry, err := retryNonceTooHighTransactions(nonceTooHigh, yielded, st.GetNonce)
			require.NoError(t, err)
			require.Equal(t, test.postNonce != 0, retry)
			require.Equal(t, test.postNonce == 0, yielded.Contains(gapped.Hash()))
			require.Equal(t, test.postNonce == 0, len(nonceTooHigh[sender]) != 0)
		})
	}
}

// prioritisedProvider mimics the txpool's best(): it walks a fixed priority order, skips IDs in
// the filter and adds every returned txn to it. withheld is left out of the first call, the way
// best() skips a txn that does not fit the gas or blob budget left in a call.
type prioritisedProvider struct {
	ordered  []types.Transaction
	withheld types.Transaction
	served   []types.Transaction
}

func (p *prioritisedProvider) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o := txnprovider.ApplyProvideOptions(opts...)
	firstCall := len(p.served) == 0
	var out []types.Transaction
	for _, txn := range p.ordered {
		if len(out) == o.Amount {
			break
		}
		h := txn.Hash()
		if o.TxnIdsFilter.Contains(h) || firstCall && txn == p.withheld {
			continue
		}
		out = append(out, txn)
		o.TxnIdsFilter.Add(h)
	}
	p.served = append(p.served, out...)
	return out, nil
}

type accountsState struct {
	state.StateReader
	state.StateWriter
	accounts map[accounts.Address]*accounts.Account
}

func (s *accountsState) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	return s.accounts[a], nil
}

func (s *accountsState) GetNonce(a accounts.Address) (uint64, error) {
	if account := s.accounts[a]; account != nil {
		return account.Nonce, nil
	}
	return 0, nil
}

func (s *accountsState) UpdateAccountData(a accounts.Address, _, acc *accounts.Account) error {
	s.accounts[a] = acc
	return nil
}

func newFundedState(addrs ...accounts.Address) *accountsState {
	st := &accountsState{accounts: map[accounts.Address]*accounts.Account{}}
	for _, a := range addrs {
		st.accounts[a] = &accounts.Account{Balance: *uint256.NewInt(1e18), CodeHash: accounts.EmptyCodeHash}
	}
	return st
}

func newTxnFrom(from accounts.Address, nonce, gas uint64) types.Transaction {
	txn := types.NewTransaction(nonce, common.Address{0xc}, uint256.NewInt(0), gas, uint256.NewInt(1), nil)
	txn.SetSender(from)
	return txn
}

// pollTxns calls getNextTransactions a few times, the way execBlock polls until the build deadline.
func pollTxns(t *testing.T, provider txnprovider.TxnProvider, st *accountsState, amount int) []types.Transaction {
	t.Helper()
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1)}
	cfg := BuilderExecCfg{
		builderState: BuilderState{BuilderConfig: &buildercfg.BuilderConfig{}, BuiltBlock: &exec.AssembledBlock{Header: header}},
		chainConfig:  chain.AllProtocolChanges,
		txnProvider:  provider,
	}
	yielded := mapset.NewSet[[32]byte]()
	nonceTooHigh := map[accounts.Address][]nonceTooHighTxn{}
	var got []types.Transaction
	for range 5 {
		txns, _, err := getNextTransactions(context.Background(), cfg, chain.AllProtocolChanges.ChainID, header, protocol.GasUsed{}, amount, 0,
			yielded, nonceTooHigh, st, st, st.GetNonce, log.New(), &filtrationStats{})
		require.NoError(t, err)
		got = append(got, txns...)
		_, err = retryNonceTooHighTransactions(nonceTooHigh, yielded, st.GetNonce)
		require.NoError(t, err)
	}
	return got
}

func TestGetNextTransactionsReachesExecutableTxnBehindNonceTooHighBatch(t *testing.T) {
	t.Parallel()

	const amount = 50
	stuck := accounts.InternAddress(common.Address{0xa})
	ready := accounts.InternAddress(common.Address{0xb})
	var ordered []types.Transaction
	for nonce := uint64(1); nonce <= amount; nonce++ {
		ordered = append(ordered, newTxnFrom(stuck, nonce, 21_000))
	}
	executable := newTxnFrom(ready, 0, 21_000)
	ordered = append(ordered, executable)

	got := pollTxns(t, &prioritisedProvider{ordered: ordered}, newFundedState(stuck, ready), amount)

	require.Len(t, got, 1)
	require.Equal(t, executable.Hash(), got[0].Hash())
}

func TestGetNextTransactionsReconsidersNonceTooHighTxnAfterGapIsFilled(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.Address{0xa})
	first := newTxnFrom(sender, 0, params.TxGas)
	second := newTxnFrom(sender, 1, params.TxGas)
	provider := &prioritisedProvider{ordered: []types.Transaction{first, second}, withheld: first}

	got := pollTxns(t, provider, newFundedState(sender), 50)

	require.Len(t, got, 2)
	require.Equal(t, first.Hash(), got[0].Hash())
	require.Equal(t, second.Hash(), got[1].Hash())
}

func TestGetNextTransactionsKeepsNonceTooHighTxnYieldedWhenSenderAdvancedInSameBatch(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.Address{0xa})
	first := newTxnFrom(sender, 0, params.TxGas)
	gapped := newTxnFrom(sender, 2, params.TxGas)
	provider := &prioritisedProvider{ordered: []types.Transaction{first, gapped}}

	got := pollTxns(t, provider, newFundedState(sender), 50)

	require.Len(t, got, 1)
	require.Equal(t, first.Hash(), got[0].Hash())
	require.Len(t, provider.served, 2, "the gapped txn must not be served again while its sender makes no progress")
}

func TestFilterBadTransactionsReturnsOnlyNonceTooHighLeftovers(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.Address{0xa})
	unfunded := accounts.InternAddress(common.Address{0xb})
	st := newFundedState(sender)
	st.accounts[sender].Nonce = 1
	tooLow := newTxnFrom(sender, 0, params.TxGas)
	accepted1 := newTxnFrom(sender, 1, params.TxGas)
	accepted2 := newTxnFrom(sender, 2, params.TxGas)
	tooHigh := newTxnFrom(sender, 4, params.TxGas)
	noAccount := newTxnFrom(unfunded, 0, params.TxGas)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1)}

	filtered, nonceTooHigh, err := filterBadTransactions([]types.Transaction{tooHigh, accepted2, tooLow, noAccount, accepted1},
		chain.AllProtocolChanges.ChainID, chain.AllProtocolChanges, 1, header, st, st, st.GetNonce, log.New(), nil)

	require.NoError(t, err)
	require.Equal(t, []types.Transaction{accepted1, accepted2}, filtered)
	require.Equal(t, []types.Transaction{tooHigh}, nonceTooHigh)
	require.Equal(t, uint64(3), st.accounts[sender].Nonce)
}

func TestGetNextTransactionsWithoutTxnProviderYieldsNothing(t *testing.T) {
	t.Parallel()

	got := pollTxns(t, nil, newFundedState(), 50)

	require.Empty(t, got)
}
