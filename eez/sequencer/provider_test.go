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

package sequencer

import (
	"context"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

type fixed []types.Transaction

func (f fixed) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return append([]types.Transaction(nil), f...), nil
}

func transfer(t *testing.T, nonce uint64, data int, sender common.Address) types.Transaction {
	t.Helper()
	to := common.HexToAddress("0xb0b")
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 100_000, To: &to, Data: make([]byte, data)}, ChainID: *uint256.NewInt(6290)}
	tx.SetSender(accounts.InternAddress(sender))
	return tx
}

// TestPoolTxns is T-SEQ-2 for the pool: no blob transaction, no EEZ system
// type or sender, and the block's DA within the budget, a large transaction
// leaving room for smaller ones after it.
func TestPoolTxns(t *testing.T) {
	user := common.HexToAddress("0xa11ce")
	system, err := systemtx.New(protocol.SystemTx{ChainID: 6290, To: protocol.EEZL2Address})
	require.NoError(t, err)
	small, large, last := transfer(t, 0, 10, user), transfer(t, 1, 900, user), transfer(t, 2, 10, user)
	pool := fixed{
		&types.BlobTx{DynamicFeeTransaction: types.DynamicFeeTransaction{ChainID: *uint256.NewInt(6290)}},
		system,
		transfer(t, 0, 0, protocol.SystemAddress),
		small, large, last,
	}
	budget := small.EncodingSize() + last.EncodingSize() + 2*txOverhead
	got, err := newPoolTxns(pool, budget, log.New()).ProvideTxns(t.Context())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small, last}, got)

	got, err = newPoolTxns(pool, 1<<20, log.New()).ProvideTxns(t.Context())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small, large, last}, got)
}

func TestDABudget(t *testing.T) {
	// A 6-block slot of full blocks fits one batch at the floor price.
	require.Equal(t, 65_738, DABudget(6))
	require.LessOrEqual(t, uint64(6*DABudget(6))*floorGasPerByte+batchReserve, uint64(maxBatchGas))
}

// paged is a pool yielding pages of n transactions it has not yielded,
// marking each in the caller's filter as the txpool does.
type paged struct {
	txs   []types.Transaction
	n     int
	pages int
}

func (p *paged) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	p.pages++
	yielded := txnprovider.ApplyProvideOptions(opts...).TxnIdsFilter
	var page []types.Transaction
	for _, tx := range p.txs {
		if len(page) == p.n {
			break
		}
		if yielded != nil {
			if yielded.Contains(tx.Hash()) {
				continue
			}
			yielded.Add(tx.Hash())
		}
		page = append(page, tx)
	}
	return page, nil
}

// TestPoolTxnsPagesPastAnEmptiedPage: a page whose transactions the budget
// all leaves out is not the pool running dry; the next pages still fill the
// block, until no transaction can fit.
func TestPoolTxnsPagesPastAnEmptiedPage(t *testing.T) {
	user := common.HexToAddress("0xa11ce")
	var txs []types.Transaction
	for i := range 3 {
		txs = append(txs, transfer(t, uint64(i), 900, user))
	}
	small := transfer(t, 3, 100, user)
	txs = append(txs, small)
	filter := func() txnprovider.ProvideOption { return txnprovider.WithTxnIdsFilter(mapset.NewSet[[32]byte]()) }

	pool := &paged{txs: txs, n: 3}
	got, err := newPoolTxns(pool, small.EncodingSize()+txOverhead, log.New()).ProvideTxns(t.Context(), filter())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small}, got)

	pool = &paged{txs: txs, n: 3}
	got, err = newPoolTxns(pool, minTxDA-1, log.New()).ProvideTxns(t.Context(), filter())
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, 1, pool.pages, "paged on with no room for any transaction")

	// Room for a transaction, but less than each pending one takes: paging
	// stops short of walking the pool.
	var large []types.Transaction
	for i := range 150 {
		large = append(large, transfer(t, uint64(i), 900, user))
	}
	pool = &paged{txs: large, n: 3}
	got, err = newPoolTxns(pool, minTxDA, log.New()).ProvideTxns(t.Context(), filter())
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, maxEmptiedPages, pool.pages)
}

// TestPoolTxnsChargesATransactionOnce: a transaction yielded again, as the
// builder has the pool do for one it set aside for its nonce, does not take
// from the budget a second time.
func TestPoolTxnsChargesATransactionOnce(t *testing.T) {
	user := common.HexToAddress("0xa11ce")
	first, second := transfer(t, 1, 100, user), transfer(t, 0, 100, user)
	yielded := mapset.NewSet[[32]byte]()
	pool := &paged{txs: []types.Transaction{first, second}, n: 1}
	p := newPoolTxns(pool, first.EncodingSize()+second.EncodingSize()+2*txOverhead, log.New())

	got, err := p.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(yielded))
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{first}, got)
	got, err = p.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(yielded))
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{second}, got)

	yielded.Remove(first.Hash())
	got, err = p.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(yielded))
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{first}, got)
}
