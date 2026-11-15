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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol/da"
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
	txs := q.decode(t, encoded...)
	q.mu.Lock()
	q.txs = append(q.txs, txs...)
	q.mu.Unlock()
}

// decode decodes encoded, senders recovered as a pool's are.
func (q *queue) decode(t *testing.T, encoded ...[]byte) []types.Transaction {
	t.Helper()
	txs, err := deriver.DecodeTxs(encoded)
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(derivertest.Config().ChainID)
	for _, tx := range txs {
		sender, err := signer.Sender(tx)
		require.NoError(t, err)
		tx.SetSender(sender)
	}
	return txs
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
	// Every slot is on time.
	s.SetClock(func() time.Time { return time.Unix(int64(g), 0) })
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
	s.SetClock(func() time.Time { return time.Unix(int64(h.genesis.Time), 0) })
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
	svc := sequencer.NewService(nil, nil, nil, log.New())
	svc.Head(l1.Header{Number: 1})
	svc.Head(l1.Header{Number: 2})
	require.Equal(t, uint64(2), svc.PendingHead().Number)
}

// pinless fails every pin.
type pinless struct{}

func (pinless) Pin(context.Context) (interface{ Close() }, error) { return nil, errors.New("no pin") }

func (pinless) Record(context.Context, interface{ Close() }, *types.Header) error { return nil }

// down is a pool whose requests fail while err is set.
type down struct{ err error }

func (d *down) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, d.err
}

// TestWitnessFailureNeedsABlock: a block left without a witness is reported
// once it commits; a build that fails leaves no block to report.
func TestWitnessFailureNeedsABlock(t *testing.T) {
	h := newHarness(t)
	var out bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&out, log.LogfmtFormat()))
	pool := &down{err: errors.New("pool down")}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Witnesses: pinless{}}, h.follower, pool, logger)
	require.NoError(t, err)
	reports := func() int { return strings.Count(out.String(), "produced block without a witness") }

	require.Error(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
	require.Zero(t, h.head(t).Number.Uint64())
	require.Zero(t, reports(), "a failed build reported a block without a witness")

	pool.err = nil
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
	require.Equal(t, uint64(1), h.head(t).Number.Uint64())
	require.Equal(t, 1, reports())
}

// full is a pool that never runs dry: each block's first request returns
// every transaction from the sender's next nonce on.
type full struct {
	chain *deriver.ExecChain
	txs   []types.Transaction
	mu    sync.Mutex
	last  uint64
}

func (f *full) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	head, err := f.chain.Head(ctx)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := head.Number.Uint64() + 1
	if n == f.last {
		return nil, nil
	}
	f.last = n
	nonce, err := f.chain.Nonce(ctx, head.Number.Uint64(), derivertest.UserAddress())
	if err != nil {
		return nil, err
	}
	return f.txs[min(nonce, uint64(len(f.txs))):], nil
}

// TestFullCatchupSettlesPerSlot: on a pool that never runs dry, each block of
// a catch-up fills its DA budget, and the K blocks ending at each Sync
// position fit one posting while K+1 do not: consecutive catch-up anchors
// settle the range.
func TestFullCatchupSettlesPerSlot(t *testing.T) {
	const (
		maxPostingGas = 16_777_216 // EIP-7825 transaction gas cap
		batchReserve  = 1_000_000  // the batch's own entries, proofs and encoding
		txOverhead    = 5          // a transaction's length prefix in the span
	)
	h := newHarness(t)
	cc := derivertest.Config()
	signer := types.LatestSignerForChainID(cc.ChainID)
	to := common.HexToAddress("0xb0b")
	data := bytes.Repeat([]byte{0xff}, 1_000)
	pool := &full{chain: h.chain}
	for nonce := range uint64(2_000) {
		tx, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 70_000, To: &to, Data: data},
			ChainID:  *cc.ChainID,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(10 * common.GWei),
		}, *signer, derivertest.UserKey)
		require.NoError(t, err)
		sender, err := signer.Sender(tx)
		require.NoError(t, err)
		tx.SetSender(sender)
		pool.txs = append(pool.txs, tx)
	}
	s, err := sequencer.New(cc, sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)

	const sync = 24
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: sync, Time: h.genesis.Time + 2*sync, L1Head: 4}))
	require.Equal(t, uint64(sync), h.head(t).Number.Uint64())

	k := devnet().K()
	budget := sequencer.DABudget(k)
	txDA := pool.txs[0].EncodingSize() + txOverhead
	for n := uint64(1); n <= sync; n++ {
		size := 0
		for _, tx := range h.block(t, n).Transactions() {
			size += tx.EncodingSize() + txOverhead
		}
		require.LessOrEqual(t, size, budget, "block %d", n)
		require.Greater(t, size, budget-txDA, "block %d leaves room for another transaction", n)
	}

	// floorGas is the EIP-7623 floor of a posting carrying the DA of blocks
	// from..to.
	floorGas := func(from, to uint64) uint64 {
		span := &da.Span{}
		for n := from; n <= to; n++ {
			b := h.block(t, n)
			span.TxCounts = append(span.TxCounts, uint32(len(b.Transactions())))
			span.Beneficiaries = append(span.Beneficiaries, b.Coinbase())
			span.ExtraData = append(span.ExtraData, b.Extra())
			for _, tx := range b.Transactions() {
				var buf bytes.Buffer
				require.NoError(t, tx.MarshalBinary(&buf))
				span.Transactions = append(span.Transactions, buf.Bytes())
			}
		}
		payload := &da.Payload{RollupID: cc.L2.(*config.L2Config).RollupID, Operations: span.Encode()}
		var tokens uint64
		for _, b := range da.Encode(payload.Messages()...) {
			if b == 0 {
				tokens++
			} else {
				tokens += 4
			}
		}
		return 21_000 + 10*tokens
	}
	for end := k; end <= sync; end += k {
		require.LessOrEqual(t, floorGas(end-k+1, end), uint64(maxPostingGas-batchReserve), "blocks %d..%d", end-k+1, end)
		if end > k {
			require.Greater(t, floorGas(end-k, end), uint64(maxPostingGas-batchReserve), "blocks %d..%d", end-k, end)
		}
	}
}

// oneEach is a pool yielding one transaction per block, the sender's next.
type oneEach struct {
	chain *deriver.ExecChain
	txs   []types.Transaction
	mu    sync.Mutex
	last  uint64
}

func (o *oneEach) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	head, err := o.chain.Head(ctx)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	n := head.Number.Uint64() + 1
	if n == o.last {
		return nil, nil
	}
	o.last = n
	nonce, err := o.chain.Nonce(ctx, head.Number.Uint64(), derivertest.UserAddress())
	if err != nil || nonce >= uint64(len(o.txs)) {
		return nil, err
	}
	return o.txs[nonce : nonce+1], nil
}

// TestLateSyncBlockIsEmpty: a Sync block whose proof would reach L1 after
// its timestamp less the slack is built empty; on time it takes the pool's
// transactions.
func TestLateSyncBlockIsEmpty(t *testing.T) {
	for _, late := range []bool{false, true} {
		h := newHarness(t)
		pool := &oneEach{chain: h.chain}
		for range 10 {
			pool.txs = append(pool.txs, h.pool.decode(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1))...)
		}
		s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
		require.NoError(t, err)
		sync := h.genesis.Time + 12
		deadline := time.Unix(int64(sync), 0).Add(-devnet().Slack - devnet().Proof)
		now := deadline.Add(-time.Second)
		if late {
			now = deadline.Add(time.Second)
		}
		s.SetClock(func() time.Time { return now })

		require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: sync, L1Head: 1}))
		require.Equal(t, uint64(6), h.head(t).Number.Uint64())
		for n := uint64(1); n < 6; n++ {
			require.Len(t, h.block(t, n).Transactions(), 1, "late %v, block %d", late, n)
		}
		if late {
			require.Empty(t, h.block(t, 6).Transactions())
		} else {
			require.Len(t, h.block(t, 6).Transactions(), 1)
		}
	}
}

// TestServiceHaltsOffTheL1Grid: a schedule stopped by an L1 head off the
// grid of the genesis halts the service.
func TestServiceHaltsOffTheL1Grid(t *testing.T) {
	h := newHarness(t)
	halted := make(chan error, 1)
	svc := sequencer.NewService(h.sequencer(t, 0), sequencer.NewSchedule(devnet(), h.genesis.Time), func(err error) { halted <- err }, log.New())
	require.NoError(t, svc.Start())
	defer func() { _ = svc.Stop() }()
	svc.Head(l1.Header{Number: 1, Time: h.genesis.Time + 13})
	select {
	case err := <-halted:
		require.ErrorIs(t, err, sequencer.ErrTiming)
	case <-time.After(10 * time.Second):
		t.Fatal("not halted")
	}
	require.ErrorIs(t, svc.Err(), sequencer.ErrTiming, "the node's exit status reads it")
}

// inOrder is a pool yielding, one per request, the first of its
// transactions the caller has not marked yielded, marking it as the txpool
// does.
type inOrder struct {
	mu  sync.Mutex
	txs []types.Transaction
}

func (o *inOrder) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	yielded := txnprovider.ApplyProvideOptions(opts...).TxnIdsFilter
	for _, tx := range o.txs {
		if !yielded.Contains(tx.Hash()) {
			yielded.Add(tx.Hash())
			return []types.Transaction{tx}, nil
		}
	}
	return nil, nil
}

// TestBlockBudgetChargesARetryOnce: a transaction the builder sets aside for
// its nonce and takes again later counts once against the block's DA budget.
func TestBlockBudgetChargesARetryOnce(t *testing.T) {
	for _, exact := range []bool{false, true} {
		h := newHarness(t)
		txs := h.pool.decode(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1), h.c.Transfer(common.HexToAddress("0xb0b"), 2))
		budget := 0
		if exact {
			budget = txs[0].EncodingSize() + txs[1].EncodingSize() + 2*5
		}
		// Nonce 1 first: the builder sets it aside until nonce 0 executes.
		pool := &inOrder{txs: []types.Transaction{txs[1], txs[0]}}
		s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), DABudget: budget}, h.follower, pool, log.New())
		require.NoError(t, err)
		require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
		require.Len(t, h.block(t, 1).Transactions(), 2, "exact budget %v", exact)
	}
}
