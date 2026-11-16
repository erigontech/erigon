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

package sequencer_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// spaced is a pool yielding, as the txpool does, up to the requested amount
// of its transactions in order: those in the caller's filter skipped, those
// over the space left skipped unmarked, the space shrinking by each yielded.
type spaced struct {
	mu    sync.Mutex
	txs   []types.Transaction
	calls int
}

func (s *spaced) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	o := txnprovider.ApplyProvideOptions(opts...)
	space := o.AvailableRlpSpace
	var page []types.Transaction
	for _, tx := range s.txs {
		if len(page) == o.Amount || space <= 0 {
			break
		}
		if o.TxnIdsFilter.Contains(tx.Hash()) {
			continue
		}
		size := len(encoded(tx))
		if size > space {
			continue
		}
		space -= size
		o.TxnIdsFilter.Add(tx.Hash())
		page = append(page, tx)
	}
	return page, nil
}

// encoded is tx's binary encoding, as a span carries it.
func encoded(tx types.Transaction) []byte {
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// spanSize is the bytes tx adds to a span: its encoding and its length.
func spanSize(tx types.Transaction) int {
	n := len(encoded(tx))
	return n + len(binary.AppendUvarint(nil, uint64(n)))
}

func sized(t *testing.T, txs ...types.Transaction) int {
	t.Helper()
	size := 0
	for _, tx := range txs {
		size += spanSize(tx)
	}
	return size
}

// produce builds block 1 from pool with budget, 0 for the default, and
// returns its transactions' hashes.
func produce(t *testing.T, h *harness, pool txnprovider.TxnProvider, budget int) []common.Hash {
	t.Helper()
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), DABudget: budget}, h.follower, pool, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
	return hashes(h.block(t, 1).Transactions())
}

// delegate is otherKey's EIP-7702 transaction at nonce carrying the user's
// authorization at authNonce.
func delegate(t *testing.T, nonce, authNonce uint64) types.Transaction {
	t.Helper()
	cc := derivertest.Config()
	auth, err := types.SignAuthorization(derivertest.UserKey, *cc.ChainID, common.HexToAddress("0xde1e6a7e"), authNonce)
	require.NoError(t, err)
	to := common.HexToAddress("0xb0b")
	signer := types.LatestSignerForChainID(cc.ChainID)
	tx, err := types.SignTx(&types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 100_000, To: &to},
			ChainID:  *cc.ChainID,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(10 * common.GWei),
		},
		Authorizations: []types.Authorization{auth},
	}, *signer, otherKey)
	require.NoError(t, err)
	sender, err := signer.Sender(tx)
	require.NoError(t, err)
	tx.SetSender(sender)
	return tx
}

// TestBlockBudgetLeavesOutWhatTheBuilderDrops: a transaction the block does
// not take spends none of its budget. Here a delegation raises the user's
// nonce past the user's pending transaction, which fills what is left.
func TestBlockBudgetLeavesOutWhatTheBuilderDrops(t *testing.T) {
	h := newHarness(t)
	budget := sequencer.DABudget(devnet().K())
	auth := delegate(t, 0, 0)
	transfer := signedTransfer(t, otherKey, 1, 0)
	data := budget - spanSize(auth) - 400
	dropped := signedTransfer(t, derivertest.UserKey, 0, data)
	left := budget - spanSize(auth) - spanSize(dropped)
	dropped = signedTransfer(t, derivertest.UserKey, 0, data+left-40)
	left = budget - sized(t, auth, dropped)
	require.True(t, left >= 0 && left < spanSize(transfer), "the dropped transaction fills what is left: %d", left)
	pool := &spaced{txs: []types.Transaction{auth, dropped, transfer}}
	require.Equal(t, []common.Hash{auth.Hash(), transfer.Hash()}, produce(t, h, pool, 0))
}

// TestBlockBudgetLeavesOutWhatThePoolSkips: transactions the builder sets
// aside for their nonce, the pool holding back the one before them, spend
// none of the block's budget.
func TestBlockBudgetLeavesOutWhatThePoolSkips(t *testing.T) {
	h := newHarness(t)
	budget := sequencer.DABudget(devnet().K())
	var waiting []types.Transaction
	left := budget
	for n := uint64(1); left > 5_000; n++ {
		tx := signedTransfer(t, derivertest.UserKey, n, 4_400)
		left -= spanSize(tx)
		waiting = append(waiting, tx)
	}
	transfer := signedTransfer(t, otherKey, 0, 0)
	nonce, data := uint64(len(waiting)+1), left-spanSize(transfer)
	last := signedTransfer(t, derivertest.UserKey, nonce, data)
	last = signedTransfer(t, derivertest.UserKey, nonce, data+left-spanSize(last)-spanSize(transfer)+30)
	waiting = append(waiting, last)
	left -= spanSize(last)
	require.True(t, left >= 0 && left < spanSize(transfer), "the waiting transactions fill the budget: %d", left)
	pool := &spaced{txs: append(waiting, transfer)}
	require.Equal(t, []common.Hash{transfer.Hash()}, produce(t, h, pool, 0))
}

// TestBlockBudgetBoundsThePoolWalk: transactions no block can carry, ahead
// of an includable one, cost the build no extra pool requests: the pool is
// offered the budget left, not the block's RLP space.
func TestBlockBudgetBoundsThePoolWalk(t *testing.T) {
	h := newHarness(t)
	const budget = 2_000
	var txs []types.Transaction
	for n := range uint64(400) {
		txs = append(txs, signedTransfer(t, derivertest.UserKey, n, budget))
	}
	transfer := signedTransfer(t, otherKey, 0, 0)
	pool := &spaced{txs: append(txs, transfer)}
	require.Equal(t, []common.Hash{transfer.Hash()}, produce(t, h, pool, budget))
	require.LessOrEqual(t, pool.calls, 2, "one request yields the transfer, the next finds the pool dry")
}

// legacyTransfer is key's legacy transfer at nonce carrying data zero bytes.
func legacyTransfer(t *testing.T, nonce uint64, data int) types.Transaction {
	t.Helper()
	cc := derivertest.Config()
	signer := types.LatestSignerForChainID(cc.ChainID)
	to := common.HexToAddress("0xb0b")
	tx, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21_000 + 10*uint64(data), To: &to, Value: *uint256.NewInt(1), Data: make([]byte, data)},
		GasPrice: *uint256.NewInt(10 * common.GWei),
	}, *signer, derivertest.UserKey)
	require.NoError(t, err)
	sender, err := signer.Sender(tx)
	require.NoError(t, err)
	tx.SetSender(sender)
	return tx
}

// TestBlockBudgetIsExact: a block takes a transaction whose span size, its
// encoding and that length's uvarint, is exactly the budget, and none a
// byte over it; typed and legacy, across uvarint and RLP length-prefix
// widths.
func TestBlockBudgetIsExact(t *testing.T) {
	for _, tc := range []struct {
		name string
		tx   types.Transaction
	}{
		{"typed", signedTransfer(t, derivertest.UserKey, 0, 200)},
		{"typed 16K", signedTransfer(t, derivertest.UserKey, 0, 16_300)},
		{"legacy", legacyTransfer(t, 0, 200)},
		{"legacy 16K", legacyTransfer(t, 0, 16_300)},
	} {
		for short := range 7 {
			h := newHarness(t)
			got := produce(t, h, &byPrice{txs: []types.Transaction{tc.tx}}, spanSize(tc.tx)-short)
			if short == 0 {
				require.Equal(t, []common.Hash{tc.tx.Hash()}, got, "%s fits a budget of its span size", tc.name)
			} else {
				require.Empty(t, got, "%s over a budget %d bytes short", tc.name, short)
			}
		}
	}
}
