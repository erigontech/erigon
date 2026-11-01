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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// queue is a pool that yields its transactions once each, senders
// recovered.
type queue struct {
	mu  sync.Mutex
	txs []types.Transaction
}

func (q *queue) add(t *testing.T, encoded ...[]byte) {
	t.Helper()
	txs, err := deriver.DecodeTxs(encoded)
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(derivertest.Config().ChainID)
	for _, tx := range txs {
		sender, err := signer.Sender(tx)
		require.NoError(t, err)
		tx.SetSender(sender)
	}
	q.mu.Lock()
	q.txs = append(q.txs, txs...)
	q.mu.Unlock()
}

func (q *queue) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	txs := q.txs
	q.txs = nil
	return txs, nil
}

type harness struct {
	c        *derivertest.Composer
	chain    *deriver.ExecChain
	follower *follower.Follower
	pool     *queue
	genesis  *types.Header
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	c := derivertest.NewComposer(t)
	_, chain := derivertest.NewL2(t)
	cfg := follower.DefaultConfig
	cfg.Poll, cfg.Retry, cfg.MaxRetry, cfg.BootAttempts = 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond, 3
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = f.Run(ctx) }()
	genesis, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	return &harness{c: c, chain: chain, follower: f, pool: &queue{}, genesis: genesis}
}

func (h *harness) sequencer(t *testing.T, maxDepth uint64) *sequencer.Sequencer {
	t.Helper()
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), MaxDepth: maxDepth}, h.follower, h.pool, log.New())
	require.NoError(t, err)
	return s
}

func (h *harness) head(t *testing.T) *types.Header {
	t.Helper()
	head, err := h.follower.Head(t.Context())
	require.NoError(t, err)
	return head
}

// devnet is the reference devnet's timing on the devnet chain: K 6, Future 2,
// Live 3.
func devnet() sequencer.Timing { return timing(12_000, 2_000, 4_000, 1_500) }

// TestSequencer is T-SEQ-1 and T-SEQ-2 on the devnet: a steady slot, Live
// ticks up to the Live blocks' end, the remainder of a slot, a Catchup that
// lands on the grid, a Sync block skipped off it; the blocks carry the pool's
// transactions and the header rule derivation rebuilds.
func TestSequencer(t *testing.T) {
	h := newHarness(t)
	s := h.sequencer(t, 0)
	ctx := t.Context()
	g := h.genesis.Time
	l2 := derivertest.Config().L2.(*config.L2Config)
	collector := *derivertest.Config().FeeCollector

	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1), h.c.Transfer(common.HexToAddress("0xb0b"), 2))
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	require.Equal(t, uint64(6), h.head(t).Number.Uint64())
	require.Equal(t, g+12, h.head(t).Time)
	st := s.Status()
	require.Equal(t, [3]uint64{3, 2, 1}, st.Produced)

	first := h.block(t, 1)
	require.Len(t, first.Transactions(), 2, "the first Live block takes the pool")
	for n := uint64(1); n <= 6; n++ {
		b := h.block(t, n)
		require.Equal(t, g+2*n, b.Time())
		require.Equal(t, collector, b.Coinbase())
		require.Empty(t, b.Extra())
		require.Equal(t, l2.BlockGasLimit, b.GasLimit())
		require.Equal(t, common.Hash{}, b.MixDigest())
	}

	// Live ticks fill the next slot's Live blocks, 7 to 9, and stop there.
	for range 4 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	require.Equal(t, uint64(9), h.head(t).Number.Uint64())
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 12, Time: g + 24, L1Head: 2}))
	require.Equal(t, uint64(12), h.head(t).Number.Uint64())
	require.Equal(t, [3]uint64{6, 4, 2}, s.Status().Produced)

	// Six slots behind: one Catchup to the target.
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 48, Time: g + 96, L1Head: 8}))
	require.Equal(t, uint64(48), h.head(t).Number.Uint64())
	require.Equal(t, g+96, h.head(t).Time)

	// A slot whose timestamp is not on the head's cadence: its Sync block is
	// skipped.
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 54, Time: g + 110, L1Head: 9}))
	require.Equal(t, uint64(53), h.head(t).Number.Uint64())
	require.Equal(t, 1, s.Status().OffGridSkips)
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 60, Time: g + 120, L1Head: 10}))
	require.Equal(t, uint64(60), h.head(t).Number.Uint64())
	require.Zero(t, s.Status().OffGridSkips)
}

func (h *harness) block(t *testing.T, n uint64) *types.Block {
	t.Helper()
	b, err := h.chain.BlockByNumber(t.Context(), n)
	require.NoError(t, err)
	require.NotNil(t, b)
	return b
}

// TestSequencerCap: a steady slot stops at the cap on blocks ahead of the
// latest settlement.
func TestSequencerCap(t *testing.T) {
	h := newHarness(t)
	s := h.sequencer(t, 4)
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: h.genesis.Time + 12, L1Head: 1}))
	require.Equal(t, uint64(4), h.head(t).Number.Uint64())
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}))
	require.Equal(t, uint64(4), h.head(t).Number.Uint64())
}

// TestSequencerRejects: the sequencer produces rollup0 chains on their own
// block time.
func TestSequencerRejects(t *testing.T) {
	cc := derivertest.Config()
	_, err := sequencer.New(cc, sequencer.Config{Timing: timing(12_000, 4_000, 4_000, 1_500)}, nil, nil, log.New())
	require.ErrorIs(t, err, sequencer.ErrTiming)
	_, err = sequencer.New(cc, sequencer.Config{Timing: timing(12_000, 2_000, 11_000, 1_500)}, nil, nil, log.New())
	require.ErrorIs(t, err, sequencer.ErrTiming)
}

// TestServiceHead: the latest L1 head replaces one the schedule has not
// taken.
func TestServiceHead(t *testing.T) {
	svc := sequencer.NewService(nil, nil, log.New())
	svc.Head(l1.Header{Number: 1})
	svc.Head(l1.Header{Number: 2})
	require.Equal(t, uint64(2), svc.PendingHead().Number)
}
