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
	"crypto/ecdsa"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
	// mirror builds the blocks a composer hands over.
	mirror *deriver.ExecChain
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
	return h.sequencerOf(t, sequencer.Config{MaxDepth: maxDepth})
}

// sequencerOf returns the sequencer of cfg on the devnet timing.
func (h *harness) sequencerOf(t *testing.T, cfg sequencer.Config) *sequencer.Sequencer {
	t.Helper()
	cfg.Timing = devnet()
	s, err := sequencer.New(derivertest.Config(), cfg, h.follower, h.pool, log.New())
	require.NoError(t, err)
	return s
}

// prebuilt returns the block of txs on parent with the sequencer's header
// inputs, built on a copy of the chain as a composer builds a Sync block.
func (h *harness) prebuilt(t *testing.T, parent *types.Header, txs ...[]byte) *types.Block {
	t.Helper()
	ctx := t.Context()
	if h.mirror == nil {
		_, h.mirror = derivertest.NewL2(t)
	}
	var blocks []*types.Block
	for n := uint64(1); n <= parent.Number.Uint64(); n++ {
		blocks = append(blocks, h.block(t, n))
	}
	if len(blocks) > 0 {
		require.NoError(t, h.mirror.Insert(ctx, blocks))
	}
	decoded, err := deriver.DecodeTxs(txs)
	require.NoError(t, err)
	l2 := derivertest.Config().L2.(*config.L2Config)
	in := deriver.HeaderInputs{
		Number:     parent.Number.Uint64() + 1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Coinbase:   *derivertest.Config().FeeCollector,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}
	built, err := h.mirror.Extend(ctx, parent, in, decoded, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	return built.Block
}

// composed is a Sync slot a composer was asked for.
type composed struct {
	parent     common.Hash
	ts, target uint64
	mode       sequencer.Mode
}

// slotComposer records the Sync slots it is asked for and answers with
// answer's block, nil without one.
type slotComposer struct {
	calls     []composed
	answer    func(parent *types.Header) *types.Block
	recover   func(ctx context.Context)
	recovered int
}

func (c *slotComposer) ComposeSyncSlot(_ context.Context, parent *types.Header, ts, target uint64, mode sequencer.Mode) *types.Block {
	c.calls = append(c.calls, composed{parent: parent.Hash(), ts: ts, target: target, mode: mode})
	if c.answer == nil {
		return nil
	}
	return c.answer(parent)
}

func (c *slotComposer) RecoverFailed(ctx context.Context) {
	c.recovered++
	if c.recover != nil {
		c.recover(ctx)
	}
}

// pin is the head a witness capture pinned.
type pin common.Hash

func (pin) Close() {}

// witnesses records, for each block whose witness it built, the head pinned
// for it.
type witnesses struct {
	follower *follower.Follower
	pinned   map[common.Hash]common.Hash
}

func (w *witnesses) Pin(ctx context.Context) (interface{ Close() }, error) {
	head, err := w.follower.Head(ctx)
	if err != nil {
		return nil, err
	}
	return pin(head.Hash()), nil
}

func (w *witnesses) Record(_ context.Context, p interface{ Close() }, h *types.Header) error {
	w.pinned[h.Hash()] = common.Hash(p.(pin))
	return nil
}

// TestComposer is T-SEQ-1 for the Sync slot hook: an on-time slot is
// Steady and aimed at the next L1 block, a late one Empty, a Catchup stays
// Catchup however late; the composer's block is committed with its witness
// and ends a run of off-grid skips, a stale one is not committed, and
// recovery runs before a Sync slot or a Live tick reads the head.
func TestComposer(t *testing.T) {
	h := newHarness(t)
	c := &slotComposer{}
	w := &witnesses{follower: h.follower, pinned: map[common.Hash]common.Hash{}}
	s := h.sequencerOf(t, sequencer.Config{Composer: c, Witnesses: w})
	ctx := t.Context()
	g := h.genesis.Time
	hash := func(n uint64) common.Hash { return h.block(t, n).Hash() }
	call := func(i int) composed {
		t.Helper()
		require.Len(t, c.calls, i+1)
		return c.calls[i]
	}
	// moveBack has the next recovery move the head back to block n.
	moveBack := func(n uint64) common.Hash {
		to := hash(n)
		c.recover = func(ctx context.Context) {
			require.NoError(t, h.follower.HeadTo(ctx, to))
			c.recover = nil
		}
		return to
	}

	s.SetClock(func() time.Time { return onTime(g + 12) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	require.Equal(t, uint64(6), h.head(t).Number.Uint64(), "the pool fills the Sync block the composer leaves")
	require.Equal(t, composed{parent: hash(5), ts: g + 12, target: 2, mode: sequencer.ModeSteady}, call(0))

	// Recovery that moves the head back is seen by the Sync slot it precedes.
	for range 3 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	five := moveBack(5)
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 12, Time: g + 24, L1Head: 2}))
	require.Equal(t, uint64(12), h.head(t).Number.Uint64())
	require.Equal(t, five, h.block(t, 6).ParentHash())
	require.Equal(t, composed{parent: hash(11), ts: g + 24, target: 3, mode: sequencer.ModeCatchup}, call(1))

	// A slot off the grid is skipped; the composer's Sync block ends the run.
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 18, Time: g + 37, L1Head: 3}))
	require.Equal(t, 1, s.Status().OffGridSkips)
	var built *types.Block
	c.answer = func(parent *types.Header) *types.Block {
		built = h.prebuilt(t, parent, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
		return built
	}
	s.SetClock(func() time.Time { return onTime(g + 36).Add(time.Millisecond) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 18, Time: g + 36, L1Head: 3}))
	require.Equal(t, composed{parent: hash(17), ts: g + 36, target: 0, mode: sequencer.ModeEmpty}, call(2))
	require.Equal(t, built.Hash(), h.head(t).Hash())
	require.Len(t, h.block(t, 18).Transactions(), 1)
	require.Zero(t, s.Status().OffGridSkips)
	require.Equal(t, [3]uint64{15, 4, 3}, s.Status().Produced)
	for n := uint64(1); n <= 18; n++ {
		require.Equal(t, hash(n-1), w.pinned[hash(n)], "witness of block %d", n)
	}

	c.answer = nil
	s.SetClock(func() time.Time { return onTime(g + 96) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 48, Time: g + 96, L1Head: 8}))
	require.Equal(t, uint64(48), h.head(t).Number.Uint64())
	require.Equal(t, composed{parent: hash(47), ts: g + 96, target: 9, mode: sequencer.ModeCatchup}, call(3))

	// A block on a parent that is no longer the head is not committed.
	c.answer = func(*types.Header) *types.Block { return built }
	s.SetClock(func() time.Time { return onTime(g + 108) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 54, Time: g + 108, L1Head: 9}))
	require.Equal(t, composed{parent: hash(53), ts: g + 108, target: 10, mode: sequencer.ModeSteady}, call(4))
	require.Equal(t, uint64(53), h.head(t).Number.Uint64())
	require.Equal(t, 9, c.recovered)

	// Recovery that moves the head back is seen by the Live tick it precedes.
	fifty := moveBack(50)
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 60}))
	require.Equal(t, uint64(51), h.head(t).Number.Uint64())
	require.Equal(t, fifty, h.head(t).ParentHash)

	// A Catchup capped below the target ends on the grid at its own time, and
	// stays Catchup at its trigger's instant, long past that time.
	c.answer = nil
	s.SetClock(func() time.Time { return time.Unix(int64(g+786), 0) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 396, Time: g + 792, L1Head: 66}))
	require.Equal(t, uint64(348), h.head(t).Number.Uint64())
	require.Equal(t, composed{parent: hash(347), ts: g + 696, target: 67, mode: sequencer.ModeCatchup}, call(5))
}

// settle posts the batch that settles the local blocks from..to as they are,
// and waits for the follower to make to safe. It returns the L1 block.
func (h *harness) settle(t *testing.T, from, to uint64) uint64 {
	t.Helper()
	l1Block := h.c.Post(derivertest.Posting{Batch: h.compose(t, from, to)}).Header.Number
	require.Eventually(t, func() bool { return h.follower.Status().Safe == to }, 10*time.Second, 5*time.Millisecond)
	return l1Block
}

// compose returns the batch that settles the local blocks from..to as they
// are.
func (h *harness) compose(t *testing.T, from, to uint64) *derivertest.Batch {
	t.Helper()
	var blocks []derivertest.Block
	for n := from; n <= to; n++ {
		b := h.block(t, n)
		txs := make([][]byte, len(b.Transactions()))
		for i, tx := range b.Transactions() {
			var buf bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&buf))
			txs[i] = buf.Bytes()
		}
		blocks = append(blocks, derivertest.Block{Coinbase: b.Coinbase(), Extra: b.Extra(), Txs: txs})
	}
	batch := h.c.Compose(nil, blocks...)
	require.Equal(t, h.block(t, to).Hash(), batch.Blocks[len(batch.Blocks)-1].Hash())
	return batch
}

// TestSubstitute is T-SEQ-5 for a failed bundle: at the composer's request
// the head moves back below the failed Sync block and an empty sibling, its
// witness recorded, takes its place; once L1 settles that height nothing is
// replaced.
func TestSubstitute(t *testing.T) {
	h := newHarness(t)
	c := &slotComposer{}
	w := &witnesses{follower: h.follower, pinned: map[common.Hash]common.Hash{}}
	s := h.sequencerOf(t, sequencer.Config{Composer: c, Witnesses: w})
	ctx := t.Context()
	g := h.genesis.Time
	collector := *derivertest.Config().FeeCollector

	c.answer = func(parent *types.Header) *types.Block {
		return h.prebuilt(t, parent, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	}
	s.SetClock(func() time.Time { return onTime(g + 12) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	require.Len(t, c.calls, 1)
	require.Equal(t, sequencer.ModeSteady, c.calls[0].mode)
	failed := h.block(t, 6)
	require.Len(t, failed.Transactions(), 1)
	c.answer = nil
	for range 2 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	require.Equal(t, uint64(8), h.head(t).Number.Uint64())

	// The failed block's transaction is back in the pool, for a later block.
	var buf bytes.Buffer
	require.NoError(t, failed.Transactions()[0].MarshalBinary(&buf))
	h.pool.add(t, buf.Bytes())
	parent := h.block(t, 5).Header()
	var sibling *types.Header
	c.recover = func(ctx context.Context) {
		var err error
		sibling, err = s.Substitute(ctx, failed.Header())
		require.NoError(t, err)
		c.recover = nil
	}
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	require.NotNil(t, sibling)
	six := h.block(t, 6)
	require.Equal(t, sibling.Hash(), six.Hash())
	require.Equal(t, parent.Hash(), six.ParentHash())
	require.Empty(t, six.Transactions())
	require.Equal(t, g+12, six.Time())
	require.Equal(t, collector, six.Coinbase())
	require.Equal(t, parent.Hash(), w.pinned[six.Hash()])
	require.Equal(t, uint64(7), h.head(t).Number.Uint64(), "the tick builds on the sibling")
	require.Equal(t, six.Hash(), h.head(t).ParentHash)
	require.Len(t, h.block(t, 7).Transactions(), 1)
	require.Equal(t, [3]uint64{6, 2, 2}, s.Status().Produced)

	h.settle(t, 1, 6)
	head := h.head(t)
	for _, b := range []*types.Block{six, failed} {
		_, err := s.Substitute(ctx, b.Header())
		require.ErrorIs(t, err, follower.ErrBelowSafe, "block %x", b.Hash())
	}
	require.Equal(t, head.Hash(), h.head(t).Hash())
}

// raceCommitter runs between after a successful HeadBefore, where derivation
// can run between two follower commands.
type raceCommitter struct {
	*follower.Follower
	between func()
}

func (r *raceCommitter) HeadBefore(ctx context.Context, block *types.Header) (*types.Header, error) {
	parent, err := r.Follower.HeadBefore(ctx, block)
	if err == nil && r.between != nil {
		r.between()
	}
	return parent, err
}

// TestSubstituteRace: L1 settling the failed block between the rollback and
// the sibling fails the sibling as a stale parent, and the settled block
// stays the head.
func TestSubstituteRace(t *testing.T) {
	h := newHarness(t)
	c := &slotComposer{}
	rc := &raceCommitter{Follower: h.follower}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Composer: c}, rc, h.pool, log.New())
	require.NoError(t, err)
	ctx := t.Context()
	g := h.genesis.Time

	c.answer = func(parent *types.Header) *types.Block {
		return h.prebuilt(t, parent, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	}
	s.SetClock(func() time.Time { return onTime(g + 12) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	c.answer = nil
	failed := h.block(t, 6)
	require.Len(t, failed.Transactions(), 1)
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))

	batch := h.compose(t, 1, 6)
	rc.between = func() {
		rc.between = nil
		h.c.Post(derivertest.Posting{Batch: batch})
		require.Eventually(t, func() bool { return h.follower.Status().Safe == 6 }, 10*time.Second, 5*time.Millisecond)
	}
	_, err = s.Substitute(ctx, failed.Header())
	require.ErrorIs(t, err, follower.ErrStaleParent)
	require.Equal(t, failed.Hash(), h.head(t).Hash())
}

// TestSettledRival is T-SEQ-5 for a rival batch: the blocks L1 settles
// replace the speculative ones, and production resumes on them.
func TestSettledRival(t *testing.T) {
	h := newHarness(t)
	s := h.sequencer(t, 0)
	ctx := t.Context()
	g := h.genesis.Time

	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	for range 3 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	require.Equal(t, uint64(9), h.head(t).Number.Uint64())

	var blocks []derivertest.Block
	for range 6 {
		blocks = append(blocks, derivertest.Block{Coinbase: common.HexToAddress("0xa11ce"), Extra: []byte{}, Txs: [][]byte{}})
	}
	rival := h.c.Compose(nil, blocks...)
	h.c.Post(derivertest.Posting{Batch: rival})
	require.Eventually(t, func() bool { return h.follower.Status().Safe == 6 }, 10*time.Second, 5*time.Millisecond)
	derivertest.RequireChain(t, h.chain, rival.Blocks...)
	require.Equal(t, rival.Blocks[5].Hash(), h.head(t).Hash(), "the speculative suffix is gone")

	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 12, Time: g + 24, L1Head: 2}))
	require.Equal(t, uint64(12), h.head(t).Number.Uint64())
	require.Equal(t, rival.Blocks[5].Hash(), h.block(t, 7).ParentHash())
}

// TestL1Reorg is T-SEQ-5 for an L1 reorg below the head: the head moves
// back to the block the remaining settlements end at, a Catchup rebuilds on
// the grid from there, and a late failure of a Sync block the reorg dropped
// replaces nothing.
func TestL1Reorg(t *testing.T) {
	h := newHarness(t)
	c := &slotComposer{}
	s := h.sequencerOf(t, sequencer.Config{Composer: c})
	ctx := t.Context()
	g := h.genesis.Time

	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	first := h.settle(t, 1, 6)
	c.answer = func(parent *types.Header) *types.Block {
		return h.prebuilt(t, parent, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	}
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 12, Time: g + 24, L1Head: 2}))
	c.answer = nil
	failed := h.block(t, 12)
	require.Len(t, failed.Transactions(), 1)
	h.settle(t, 7, 12)
	for range 3 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 18}))
	}
	six, seven := h.block(t, 6), h.block(t, 7)
	require.Equal(t, uint64(15), h.head(t).Number.Uint64())

	h.c.ReorgL1(first)
	require.Eventually(t, func() bool {
		head, err := h.follower.Head(ctx)
		return err == nil && head.Hash() == six.Hash() && h.follower.Status().Safe == 6
	}, 10*time.Second, 5*time.Millisecond)
	_, ok, err := h.chain.CanonicalHash(ctx, 7)
	require.NoError(t, err)
	require.False(t, ok, "block 7 left the canonical chain")

	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 18, Time: g + 36, L1Head: 3}))
	require.Equal(t, uint64(18), h.head(t).Number.Uint64())
	require.Equal(t, g+36, h.head(t).Time)
	require.Equal(t, seven.Hash(), h.block(t, 7).Hash(), "an empty pool rebuilds the same block")

	require.Equal(t, failed.ParentHash(), h.block(t, 11).Hash())
	head := h.head(t)
	_, err = s.Substitute(ctx, failed.Header())
	require.ErrorIs(t, err, follower.ErrNotCanonical)
	require.Equal(t, head.Hash(), h.head(t).Hash())
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

// onTime is the latest instant a proof of the Sync block at ts reaches L1 the
// slack before it, on the devnet timing.
func onTime(ts uint64) time.Time {
	tm := devnet()
	return time.Unix(int64(ts), 0).Add(-tm.Slack - tm.Proof)
}

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
	txDA := spanSize(pool.txs[0])
	for n := uint64(1); n <= sync; n++ {
		size := sized(t, h.block(t, n).Transactions()...)
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
			budget = sized(t, txs...)
		}
		// Nonce 1 first: the builder sets it aside until nonce 0 executes.
		pool := &inOrder{txs: []types.Transaction{txs[1], txs[0]}}
		s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), DABudget: budget}, h.follower, pool, log.New())
		require.NoError(t, err)
		require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
		require.Len(t, h.block(t, 1).Transactions(), 2, "exact budget %v", exact)
	}
}

// byPrice is a pool yielding, as the txpool does, up to the requested amount
// of its transactions in order, skipping and marking those in the caller's
// filter.
type byPrice struct {
	mu  sync.Mutex
	txs []types.Transaction
}

func (b *byPrice) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := txnprovider.ApplyProvideOptions(opts...)
	var page []types.Transaction
	for _, tx := range b.txs {
		if len(page) == o.Amount {
			break
		}
		if o.TxnIdsFilter.Contains(tx.Hash()) {
			continue
		}
		o.TxnIdsFilter.Add(tx.Hash())
		page = append(page, tx)
	}
	return page, nil
}

// signedTransfer is key's transfer at nonce carrying data bytes of calldata.
func signedTransfer(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, data int) types.Transaction {
	t.Helper()
	cc := derivertest.Config()
	signer := types.LatestSignerForChainID(cc.ChainID)
	to := common.HexToAddress("0xb0b")
	tx, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21_000 + 40*uint64(data), To: &to, Value: *uint256.NewInt(1), Data: make([]byte, data)},
		ChainID:  *cc.ChainID,
		TipCap:   *uint256.NewInt(1),
		FeeCap:   *uint256.NewInt(10 * common.GWei),
	}, *signer, key)
	require.NoError(t, err)
	sender, err := signer.Sender(tx)
	require.NoError(t, err)
	tx.SetSender(sender)
	return tx
}

// otherKey funds the devnet's first prefunded account.
var otherKey, _ = crypto.HexToECDSA("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")

// TestBlockBudgetIgnoresTheFollowersOfAnOversizedTransaction: an account
// whose first transaction no block can carry spends none of the budget on
// its later ones, which cannot execute; another account's transfer is
// produced.
func TestBlockBudgetIgnoresTheFollowersOfAnOversizedTransaction(t *testing.T) {
	h := newHarness(t)
	budget := sequencer.DABudget(devnet().K())
	txs := []types.Transaction{signedTransfer(t, derivertest.UserKey, 0, budget)}
	left := budget
	for n := uint64(1); n <= 14; n++ {
		tx := signedTransfer(t, derivertest.UserKey, n, 4_400)
		left -= spanSize(tx)
		txs = append(txs, tx)
	}
	last := signedTransfer(t, derivertest.UserKey, 15, left-200)
	left -= spanSize(last)
	transfer := signedTransfer(t, otherKey, 0, 0)
	require.Greater(t, spanSize(transfer), left, "the account's later transactions fill the budget")
	pool := &byPrice{txs: append(txs, last, transfer)}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
	require.Equal(t, []common.Hash{transfer.Hash()}, hashes(h.block(t, 1).Transactions()))
}

// TestNeverIncludableTransactionsDoNotStarve: transactions no block can
// carry, ahead of an includable one in the pool, do not end the block.
func TestNeverIncludableTransactionsDoNotStarve(t *testing.T) {
	h := newHarness(t)
	const budget = 2_000
	var txs []types.Transaction
	for n := range uint64(400) {
		txs = append(txs, signedTransfer(t, derivertest.UserKey, n, budget))
	}
	transfer := signedTransfer(t, otherKey, 0, 0)
	pool := &byPrice{txs: append(txs, transfer)}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), DABudget: budget}, h.follower, pool, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 6}))
	require.Equal(t, []common.Hash{transfer.Hash()}, hashes(h.block(t, 1).Transactions()))
}

func hashes(txs types.Transactions) []common.Hash {
	var out []common.Hash
	for _, tx := range txs {
		out = append(out, tx.Hash())
	}
	return out
}
