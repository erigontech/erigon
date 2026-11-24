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
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

var _ poolView = (*txpool.TxPool)(nil)

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
// type or sender. The block, not the provider, applies the DA budget.
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
	p := newPoolTxns(pool, common.Hash{}, 1, log.New())
	got, err := p.ProvideTxns(t.Context())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small, large, last}, got)
	budget := p.TxnsBudget()
	require.Equal(t, 1, budget.Limit)
	require.Equal(t, daSize(large), budget.Size(large))
}

func TestDABudget(t *testing.T) {
	// A 6-block slot of full blocks fits one batch at the floor price.
	require.Equal(t, 65_738, DABudget(6, MaxBatchGas))
	for _, gas := range []uint64{1_521_000, 4_000_000, MaxBatchGas} {
		require.LessOrEqual(t, uint64(6*DABudget(6, gas))*floorGasPerByte+batchReserve, gas, "gas %d", gas)
	}
	require.Zero(t, DABudget(6, batchReserve))
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

// TestPoolTxnsPagesPastRefusedTransactions: a page whose transactions EEZ
// all refuses is not the pool running dry; the next pages hold others.
func TestPoolTxnsPagesPastRefusedTransactions(t *testing.T) {
	var txs []types.Transaction
	for i := range 9 {
		txs = append(txs, transfer(t, uint64(i), 0, protocol.SystemAddress))
	}
	theirs := transfer(t, 0, 10, common.HexToAddress("0xb0b0"))
	pool := &paged{txs: append(txs, theirs), n: 3}
	got, err := newPoolTxns(pool, common.Hash{}, 1, log.New()).ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(mapset.NewSet[[32]byte]()))
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{theirs}, got)
	require.Equal(t, 4, pool.pages)
}

// TestDASize: a transaction's DA size is what it adds to a span, under
// either encoding, for typed and legacy transactions across the widths of
// the span's uvarint and of the RLP length prefixes.
func TestDASize(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(uint256.NewInt(6290))
	to := common.HexToAddress("0xb0b")
	for _, data := range []int{0, 40, 100, 200, 16_300, 16_400, 65_400, 65_600} {
		for _, unsigned := range []types.Transaction{
			&types.LegacyTx{CommonTx: types.CommonTx{GasLimit: 21_000, To: &to, Data: make([]byte, data)}},
			&types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: 21_000, To: &to, Data: make([]byte, data)}, ChainID: *uint256.NewInt(6290)},
		} {
			tx, err := types.SignTx(unsigned, *signer, key)
			require.NoError(t, err)
			var buf bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&buf))
			with := &da.Span{TxCounts: []uint32{1}, Beneficiaries: []common.Address{{}}, ExtraData: [][]byte{nil}, Transactions: [][]byte{buf.Bytes()}}
			without := &da.Span{TxCounts: []uint32{0}, Beneficiaries: []common.Address{{}}, ExtraData: [][]byte{nil}}
			require.Equal(t, len(with.Encode())-len(without.Encode()), daSize(tx), "type %d, %d bytes of data", tx.Type(), data)
			require.Equal(t, len(with.EncodeRollupX())-len(without.EncodeRollupX()), daSize(tx), "type %d, %d bytes of data", tx.Type(), data)
		}
	}
}

// notStarted is a pool that has not started.
type notStarted struct{ fixed }

func (notStarted) Started() bool { return false }

// TestPoolTxnsNeedsAStartedPool: a pool yet to start yields nothing, which a
// block would take for a dry pool; the provider fails instead.
func TestPoolTxnsNeedsAStartedPool(t *testing.T) {
	user := common.HexToAddress("0xa11ce")
	_, err := newPoolTxns(notStarted{fixed{transfer(t, 0, 10, user)}}, common.Hash{}, 1<<20, log.New()).ProvideTxns(t.Context())
	require.ErrorIs(t, err, errPoolNotStarted)
}

// asked is a pool that keeps the options of its last request.
type asked struct {
	fixed
	opts txnprovider.ProvideOptions
}

func (a *asked) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	a.opts = txnprovider.ApplyProvideOptions(opts...)
	return a.fixed.ProvideTxns(ctx, opts...)
}

// TestPoolTxnsAsksForTheParent: the pool is asked for the transactions on the
// parent's hash, not only its number, which a block of another branch shares.
func TestPoolTxnsAsksForTheParent(t *testing.T) {
	pool := &asked{}
	parent := common.Hash{0xa}
	_, err := newPoolTxns(pool, parent, 1<<20, log.New()).ProvideTxns(t.Context(), txnprovider.WithParentBlockNum(4))
	require.NoError(t, err)
	require.Equal(t, uint64(4), pool.opts.ParentBlockNum)
	require.Equal(t, parent, pool.opts.ParentBlockHash)
}

// lagging is a pool that never sees a block, started or not.
type lagging struct {
	fixed
	running bool
	peeks   int
}

func (l *lagging) Started() bool { return l.running }

func (l *lagging) PeekBest(ctx context.Context, _ int, _ *txpool.TxnsRlp, _ uint64) (bool, error) {
	l.peeks++
	<-ctx.Done()
	return false, ctx.Err()
}

func (l *lagging) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestAwait: a pool that has not seen the parent within poolWait fails the
// wait, which names the block; one yet to start is left to the provider,
// which fails at once.
func TestAwait(t *testing.T) {
	defer func(d time.Duration) { poolWait = d }(poolWait)
	poolWait = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := newPoolTxns(&lagging{running: true}, common.Hash{}, 1<<20, log.New()).await(ctx, 7)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "txpool has not seen block 7")
	require.Less(t, time.Since(start), time.Second, "the wait outlasted poolWait")

	idle := &lagging{}
	require.NoError(t, newPoolTxns(idle, common.Hash{}, 1<<20, log.New()).await(t.Context(), 7))
	require.Zero(t, idle.peeks)
}

// TestProvideTxnsBoundsTheWait: a pool that has not seen the parent within
// viewWait fails the build, which holds the follower's goroutine.
func TestProvideTxnsBoundsTheWait(t *testing.T) {
	defer func(d time.Duration) { viewWait = d }(viewWait)
	viewWait = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := newPoolTxns(&lagging{running: true}, common.Hash{0xa}, 1<<20, log.New()).ProvideTxns(ctx, txnprovider.WithParentBlockNum(7))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "txpool has not seen block 0x0a")
	require.Less(t, time.Since(start), time.Second, "the wait outlasted viewWait")
}

// TestMaxTxnSize: for every slot of up to MaxCatchup blocks, the largest
// encoding whose span size fits a block's budget.
func TestMaxTxnSize(t *testing.T) {
	spanSize := func(n uint64) int { return int(n) + len(binary.AppendUvarint(nil, n)) }
	for k := uint64(1); k <= MaxCatchup; k++ {
		budget := DABudget(k, MaxBatchGas)
		size := MaxTxnSize(budget)
		require.LessOrEqual(t, spanSize(size), budget, "K %d", k)
		require.Greater(t, spanSize(size+1), budget, "K %d", k)
	}
}
