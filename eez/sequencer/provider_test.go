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
	"math/big"
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
	small, large, last := transfer(t, 0, 10, user), transfer(t, 1, 900, user), transfer(t, 0, 10, common.HexToAddress("0xb0b0"))
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
	small := transfer(t, 0, 100, common.HexToAddress("0xb0b0"))
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

	// Room for a transaction, but less than each pending one takes, each of
	// which an emptier block would carry: paging stops short of walking the
	// pool.
	first := transfer(t, 0, 1_800, common.HexToAddress("0xf125"))
	budget := first.EncodingSize() + txOverhead + 100
	pending := []types.Transaction{first}
	for i := range 150 {
		pending = append(pending, transfer(t, uint64(i), 900, common.BigToAddress(new(big.Int).SetUint64(uint64(0x5e0000+i)))))
	}
	pool = &paged{txs: pending, n: 3}
	p := newPoolTxns(pool, budget, log.New())
	f := filter()
	got, err = p.ProvideTxns(t.Context(), f)
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{first}, got)
	got, err = p.ProvideTxns(t.Context(), f)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, 1+maxEmptiedPages, pool.pages)

	// Transactions no block can carry do not end the walk: the one behind
	// them is kept.
	var never []types.Transaction
	for i := range 150 {
		never = append(never, transfer(t, uint64(i), 900, user))
	}
	never = append(never, small)
	pool = &paged{txs: never, n: 3}
	got, err = newPoolTxns(pool, small.EncodingSize()+txOverhead, log.New()).ProvideTxns(t.Context(), filter())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small}, got)
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

// TestPoolTxnsSkipsAStalledSender: once a sender's transaction is left out,
// its later nonces cannot execute in the block, so they take nothing from
// the budget another sender's transaction can use.
func TestPoolTxnsSkipsAStalledSender(t *testing.T) {
	user, other := common.HexToAddress("0xa11ce"), common.HexToAddress("0xb0b0")
	theirs := transfer(t, 0, 100, other)
	budget := theirs.EncodingSize() + txOverhead
	blob := &types.BlobTx{DynamicFeeTransaction: types.DynamicFeeTransaction{ChainID: *uint256.NewInt(6290)}}
	blob.SetSender(accounts.InternAddress(user))
	for name, first := range map[string]types.Transaction{
		"over the budget":   transfer(t, 0, 900, user),
		"refused":           blob,
		"over every budget": transfer(t, 0, 2*budget, user),
	} {
		next := transfer(t, 1, 10, user)
		got, err := newPoolTxns(fixed{first, next, theirs}, budget, log.New()).ProvideTxns(t.Context())
		require.NoError(t, err, name)
		require.Equal(t, []types.Transaction{theirs}, got, name)
	}
}
