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
	"encoding/json"
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
	"github.com/erigontech/erigon/eez/composer"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// queue is a pool that yields its transactions once each, senders
// recovered, to the blocks on a parent from from on.
type queue struct {
	mu   sync.Mutex
	txs  []types.Transaction
	from uint64
}

// hold withholds the transactions from the blocks on a parent below from.
func (q *queue) hold(from uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.from = from
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

func (q *queue) ProvideTxns(_ context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if txnprovider.ApplyProvideOptions(opts...).ParentBlockNum < q.from {
		return nil, nil
	}
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
	// stop stops the follower and waits for it.
	stop func()
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	c := derivertest.NewComposer(t)
	_, chain := derivertest.NewL2(t)
	h := &harness{c: c, chain: chain, pool: &queue{}}
	h.start(t, h.newFollower(t, nil))
	genesis, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	h.genesis = genesis
	return h
}

// newFollower returns a follower of the chain that hands its L1 heads to
// onHead.
func (h *harness) newFollower(t *testing.T, onHead func(l1.Header)) *follower.Follower {
	t.Helper()
	return h.followerOf(t, func(cfg *follower.Config) { cfg.OnHead = onHead })
}

// followerOf returns a follower of the chain, its configuration set by set.
func (h *harness) followerOf(t *testing.T, set func(*follower.Config)) *follower.Follower {
	t.Helper()
	cfg := follower.DefaultConfig
	cfg.Poll, cfg.Retry, cfg.MaxRetry, cfg.BootAttempts = 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond, 3
	set(&cfg)
	f, err := follower.New(h.c.L1, h.chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	return f
}

// start runs f as the follower until t ends or stop.
func (h *harness) start(t *testing.T, f *follower.Follower) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.Run(ctx)
	}()
	h.follower = f
	h.stop = func() {
		cancel()
		<-done
	}
	t.Cleanup(h.stop)
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

// slotComposer records the Sync slots it is asked for and the Sync blocks
// committed for them, and answers with answer's block, else with empty.
type slotComposer struct {
	calls     []composed
	answer    func(parent *types.Header) *types.Block
	txs       []types.Transaction
	empty     bool
	committed []common.Hash
	recover   func(ctx context.Context)
	recovered int
}

func (c *slotComposer) ComposeSyncSlot(_ context.Context, parent *types.Header, ts, target uint64, mode sequencer.Mode) sequencer.Composed {
	c.calls = append(c.calls, composed{parent: parent.Hash(), ts: ts, target: target, mode: mode})
	if c.txs != nil {
		return sequencer.Composed{Txs: c.txs}
	}
	if c.answer == nil {
		return sequencer.Composed{Empty: c.empty}
	}
	return sequencer.Composed{Block: c.answer(parent)}
}

func (c *slotComposer) SyncCommitted(_ context.Context, sync *types.Header) {
	c.committed = append(c.committed, sync.Hash())
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
	require.Equal(t, []common.Hash{hash(6)}, c.committed)

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
	require.Equal(t, []common.Hash{hash(6), hash(12), hash(18), hash(48)}, c.committed, "a Sync block not committed is not reported")

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

	// An empty answer leaves the pool's transactions to the next block.
	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	h.pool.hold(353)
	c.empty = true
	s.SetClock(func() time.Time { return onTime(g + 708) })
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 354, Time: g + 708, L1Head: 67}))
	require.Equal(t, uint64(354), h.head(t).Number.Uint64())
	require.Equal(t, sequencer.ModeSteady, call(6).mode)
	require.Empty(t, h.block(t, 354).Transactions(), "the pool filled an empty Sync block")
	require.Equal(t, hash(354), c.committed[len(c.committed)-1])
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 360}))
	require.Len(t, h.block(t, 355).Transactions(), 1)
}

// settle posts the batch that settles the local blocks from..to as they are,
// and waits for the follower to make to safe. It returns the L1 block.
func (h *harness) settle(t *testing.T, from, to uint64) uint64 {
	t.Helper()
	l1Block := h.post(t, from, to)
	require.Eventually(t, func() bool { return h.follower.Status().Safe == to }, 10*time.Second, 5*time.Millisecond)
	return l1Block
}

// post posts the batch that settles the local blocks from..to as they are,
// and returns its L1 block.
func (h *harness) post(t *testing.T, from, to uint64) uint64 {
	t.Helper()
	return h.c.Post(derivertest.Posting{Batch: h.compose(t, from, to)}).Header.Number
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

// failingBuild fails the next failures block builds, leaving the head.
type failingBuild struct {
	*follower.Follower
	err      error
	failures int
}

func (f *failingBuild) Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, hook follower.CommitHook) (*types.BlockWithReceipts, error) {
	if f.failures > 0 {
		f.failures--
		return nil, f.err
	}
	return f.Follower.Produce(ctx, in, txs, hook)
}

// TestSubstituteRetries: a sibling that fails to build leaves the head on
// the failed block's parent, where a retry of the substitution builds it
// and the triggers fail with the pending substitution until then; one L1
// overtook, by settling the height, is not retried.
func TestSubstituteRetries(t *testing.T) {
	assembly := errors.New("assembly failed")
	// failedSync commits Sync block 6 of a composed transaction, whose
	// bundle fails, and puts the transaction back in the pool.
	failedSync := func(t *testing.T) (*harness, *slotComposer, *failingBuild, *sequencer.Sequencer, *types.Block) {
		h := newHarness(t)
		c := &slotComposer{}
		fb := &failingBuild{Follower: h.follower, err: assembly}
		s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Composer: c}, fb, h.pool, log.New())
		require.NoError(t, err)
		g := h.genesis.Time
		c.answer = func(parent *types.Header) *types.Block {
			return h.prebuilt(t, parent, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
		}
		s.SetClock(func() time.Time { return onTime(g + 12) })
		require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
		c.answer = nil
		failed := h.block(t, 6)
		require.Len(t, failed.Transactions(), 1)
		var buf bytes.Buffer
		require.NoError(t, failed.Transactions()[0].MarshalBinary(&buf))
		h.pool.add(t, buf.Bytes())
		return h, c, fb, s, failed
	}
	requireSibling := func(t *testing.T, h *harness, sibling *types.Header, failed *types.Block) {
		t.Helper()
		six := h.block(t, 6)
		require.Equal(t, sibling.Hash(), six.Hash())
		require.Equal(t, failed.ParentHash(), six.ParentHash())
		require.Empty(t, six.Transactions())
		require.Equal(t, failed.Time(), six.Time())
	}
	t.Run("a direct retry", func(t *testing.T) {
		h, _, fb, s, failed := failedSync(t)
		fb.failures = 1
		_, err := s.Substitute(t.Context(), failed.Header())
		require.ErrorIs(t, err, assembly)
		require.Equal(t, failed.ParentHash(), h.head(t).Hash())
		sibling, err := s.Substitute(t.Context(), failed.Header())
		require.NoError(t, err)
		requireSibling(t, h, sibling, failed)
	})
	t.Run("triggers wait for the retry", func(t *testing.T) {
		h, c, fb, s, failed := failedSync(t)
		fb.failures = 1
		c.recover = func(ctx context.Context) {
			_, err := s.Substitute(ctx, failed.Header())
			require.ErrorIs(t, err, assembly)
			c.recover = nil
		}
		for range 2 {
			err := s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12})
			require.ErrorIs(t, err, sequencer.ErrSubstitutionPending)
			require.ErrorIs(t, err, assembly, "the build's error")
			require.Equal(t, failed.ParentHash(), h.head(t).Hash(), "nothing produced on the parent")
		}
		// Another block's substitution leaves this one pending.
		other := types.CopyHeader(failed.Header())
		other.Extra = []byte{1}
		_, err := s.Substitute(t.Context(), other)
		require.ErrorIs(t, err, follower.ErrNotCanonical)
		require.ErrorIs(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}), sequencer.ErrSubstitutionPending)
		var sibling *types.Header
		c.recover = func(ctx context.Context) {
			var err error
			sibling, err = s.Substitute(ctx, failed.Header())
			require.NoError(t, err)
			c.recover = nil
		}
		require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}))
		require.NotNil(t, sibling)
		requireSibling(t, h, sibling, failed)
		require.Equal(t, sibling.Hash(), h.block(t, 7).ParentHash(), "the tick builds on the sibling")
		require.Len(t, h.block(t, 7).Transactions(), 1)
	})
	t.Run("L1 settles the height first", func(t *testing.T) {
		h, _, fb, s, failed := failedSync(t)
		fb.failures = 1
		_, err := s.Substitute(t.Context(), failed.Header())
		require.ErrorIs(t, err, assembly)
		var blocks []derivertest.Block
		for range 6 {
			blocks = append(blocks, derivertest.Block{Coinbase: common.HexToAddress("0xa11ce"), Extra: []byte{}, Txs: [][]byte{}})
		}
		rival := h.c.Compose(nil, blocks...)
		h.c.Post(derivertest.Posting{Batch: rival})
		require.Eventually(t, func() bool { return h.follower.Status().Safe == 6 }, 10*time.Second, 5*time.Millisecond)
		_, err = s.Substitute(t.Context(), failed.Header())
		require.ErrorIs(t, err, follower.ErrBelowSafe, "the retry does not build on the moved head")
		require.Equal(t, rival.Blocks[5].Hash(), h.head(t).Hash())
		require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}))
		require.Equal(t, uint64(7), h.head(t).Number.Uint64(), "production resumes on L1's block")
	})
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

// TestRestart is T-SEQ-5 for a restart with an unsettled suffix: boot takes
// L1's settlement of the first blocks without rebuilding them, the rest stay
// the head, and the schedule resumes on them from the L1 head boot caught up
// to.
func TestRestart(t *testing.T) {
	h := newHarness(t)
	s := h.sequencer(t, 0)
	ctx := t.Context()
	g := h.genesis.Time

	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	for range 3 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	require.Len(t, h.block(t, 7).Transactions(), 1)
	suffix := h.head(t)
	require.Equal(t, uint64(9), suffix.Number.Uint64())

	// While the node is down, L1 settles the first slot and reaches the block
	// that anchors the next one.
	h.stop()
	h.post(t, 1, 6)
	l1Head, err := h.c.L1.Head(ctx)
	require.NoError(t, err)
	require.NoError(t, h.c.L1.Append(&recording.Block{Header: l1.Header{Number: l1Head.Number + 1, Hash: common.Hash{0xa1}, ParentHash: l1Head.Hash, Time: g + 12}}))

	var svc *sequencer.Service
	f := h.newFollower(t, func(head l1.Header) { svc.Head(head) })
	restarted, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, f, h.pool, log.New())
	require.NoError(t, err)
	svc = sequencer.NewService(restarted, sequencer.NewSchedule(devnet(), g), func(err error) { t.Errorf("sequencer halted: %v", err) }, log.New())
	require.NoError(t, svc.Start())
	t.Cleanup(func() { _ = svc.Stop() })
	h.start(t, f)

	require.Eventually(t, func() bool { return h.head(t).Number.Uint64() == 12 }, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, uint64(6), f.Status().Safe)
	require.Equal(t, suffix.Hash(), h.block(t, 10).ParentHash(), "production resumes on the kept suffix")
}

// TestRestartDropsTheTail: a node that composes drops its unsettled suffix
// at boot, before the schedule hears of L1, and production resumes on the
// safe block.
func TestRestartDropsTheTail(t *testing.T) {
	h := newHarness(t)
	s := h.sequencer(t, 0)
	ctx := t.Context()
	g := h.genesis.Time

	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1))
	for range 3 {
		require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	}
	require.Len(t, h.block(t, 7).Transactions(), 1)
	require.Equal(t, uint64(9), h.head(t).Number.Uint64())

	h.stop()
	h.post(t, 1, 6)
	l1Head, err := h.c.L1.Head(ctx)
	require.NoError(t, err)
	require.NoError(t, h.c.L1.Append(&recording.Block{Header: l1.Header{Number: l1Head.Number + 1, Hash: common.Hash{0xa1}, ParentHash: l1Head.Hash, Time: g + 12}}))

	var svc *sequencer.Service
	var f *follower.Follower
	type seen struct {
		number uint64
		err    error
	}
	heads := make(chan seen, 1)
	f = h.followerOf(t, func(cfg *follower.Config) {
		cfg.DropUnsettledTail = true
		cfg.OnHead = func(head l1.Header) {
			var s seen
			l2, err := f.Head(context.Background())
			if s.err = err; err == nil {
				s.number = l2.Number.Uint64()
			}
			select {
			case heads <- s:
			default:
			}
			svc.Head(head)
		}
	})
	restarted, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, f, h.pool, log.New())
	require.NoError(t, err)
	svc = sequencer.NewService(restarted, sequencer.NewSchedule(devnet(), g), func(err error) { t.Errorf("sequencer halted: %v", err) }, log.New())
	require.NoError(t, svc.Start())
	t.Cleanup(func() { _ = svc.Stop() })
	h.start(t, f)

	select {
	case s := <-heads:
		require.NoError(t, s.err)
		require.Equal(t, uint64(6), s.number, "the schedule heard of L1 before the tail was dropped")
	case <-time.After(10 * time.Second):
		t.Fatal("no L1 head after boot")
	}
	require.Eventually(t, func() bool { return h.head(t).Number.Uint64() == 12 }, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, uint64(6), f.Status().Safe)
}

// TestComposerTxs: a composer's transactions are the Sync block's, in
// order, whatever the pool holds, and the block's witness is recorded.
func TestComposerTxs(t *testing.T) {
	h := newHarness(t)
	w := &witnesses{follower: h.follower, pinned: map[common.Hash]common.Hash{}}
	txs := h.pool.decode(t, h.c.Transfer(common.HexToAddress("0xb0b"), 1), h.c.Transfer(common.HexToAddress("0xb0b"), 2))
	h.pool.hold(5)
	h.pool.add(t, h.c.Transfer(common.HexToAddress("0xca1"), 3))
	c := &slotComposer{txs: txs}
	s := h.sequencerOf(t, sequencer.Config{Composer: c, Witnesses: w})
	g := h.genesis.Time
	s.SetClock(func() time.Time { return onTime(g + 12) })
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: 6, Time: g + 12, L1Head: 1}))
	sync := h.block(t, 6)
	require.Equal(t, sync.Hash(), h.head(t).Hash())
	var got []common.Hash
	for _, tx := range sync.Transactions() {
		got = append(got, tx.Hash())
	}
	require.Equal(t, []common.Hash{txs[0].Hash(), txs[1].Hash()}, got)
	require.Equal(t, []common.Hash{sync.Hash()}, c.committed)
	require.Equal(t, h.block(t, 5).Hash(), w.pinned[sync.Hash()])
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
	pool := newFull(t, h)
	s, err := sequencer.New(cc, sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)

	const sync = 24
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: sync, Time: h.genesis.Time + 2*sync, L1Head: 4}))
	require.Equal(t, uint64(sync), h.head(t).Number.Uint64())

	k := devnet().K()
	budget := sequencer.DABudget(k, sequencer.MaxBatchGas)
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

// TestFullCatchupKeepsTheGridEmpty: with a composer, the catch-up blocks on
// the grid stay empty on a pool that never runs dry, so that a range over
// one posting settles in chunks ending there; the others fill their budget.
func TestFullCatchupKeepsTheGridEmpty(t *testing.T) {
	h := newHarness(t)
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Composer: &slotComposer{empty: true}}, h.follower, newFull(t, h), log.New())
	require.NoError(t, err)
	const sync = 24
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: sync, Time: h.genesis.Time + 2*sync, L1Head: 4}))
	require.Equal(t, uint64(sync), h.head(t).Number.Uint64())
	k := devnet().K()
	for n := uint64(1); n <= sync; n++ {
		if txs := h.block(t, n).Transactions(); n%k == 0 {
			require.Empty(t, txs, "grid block %d", n)
		} else {
			require.NotEmpty(t, txs, "block %d", n)
		}
	}
}

// TestLiveTickKeepsTheGridEmpty: with a composer, a Live tick after a Sync
// block skipped off the grid leaves the grid height empty on a full pool.
func TestLiveTickKeepsTheGridEmpty(t *testing.T) {
	h := newHarness(t)
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Composer: &slotComposer{empty: true}}, h.follower, newFull(t, h), log.New())
	require.NoError(t, err)
	ctx := t.Context()
	g := h.genesis.Time
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 12, Time: g + 24, L1Head: 2}))
	require.NoError(t, s.Handle(ctx, sequencer.SyncSlot{Height: 18, Time: g + 37, L1Head: 3}))
	require.Equal(t, 1, s.Status().OffGridSkips)
	require.Equal(t, uint64(17), h.head(t).Number.Uint64())
	require.NotEmpty(t, h.block(t, 17).Transactions())
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 24}))
	require.Equal(t, uint64(18), h.head(t).Number.Uint64())
	require.Empty(t, h.block(t, 18).Transactions(), "grid block 18")
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 24}))
	require.NotEmpty(t, h.block(t, 19).Transactions())
}

// chunkProver attests any window, recording its bounds.
type chunkProver struct{ windows [][2]uint64 }

func (p *chunkProver) ProveBy(_ context.Context, w *prove.Window, _ time.Time, _ time.Duration) ([]byte, error) {
	p.windows = append(p.windows, [2]uint64{w.From, w.To})
	return bytes.Repeat([]byte{0x11}, 65), nil
}

// chunkPoster signs postings and sends them nowhere.
type chunkPoster struct{ key *ecdsa.PrivateKey }

func (p *chunkPoster) Sign(_ context.Context, input []byte, gas uint64) (types.Transaction, error) {
	to := common.Address{0xee}
	return types.SignTx(&types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: gas, To: &to, Data: input}, ChainID: *uint256.NewInt(1)},
		*types.LatestSignerForChainID(uint256.NewInt(1)), p.key)
}

func (*chunkPoster) Send(context.Context, []types.Transaction, post.Target) (post.Sent, error) {
	return post.Sent{Block: 1}, nil
}

// idleL1 is an L1 on which no posting lands.
type idleL1 struct{}

func (idleL1) Head(context.Context) (*l1.Header, error)                  { return &l1.Header{}, nil }
func (idleL1) Receipt(context.Context, common.Hash) (*l1.Receipt, error) { return nil, nil }
func (idleL1) HasTransaction(context.Context, common.Hash) (bool, error) { return false, nil }

type noWitnesses struct{}

func (noWitnesses) Get(context.Context, uint64, common.Hash) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

// TestFullCatchupSettlesInChunks: the composer settles a catch-up of full
// blocks over one posting's gas in a chunk ending at an empty grid block.
func TestFullCatchupSettlesInChunks(t *testing.T) {
	h := newHarness(t)
	cc := derivertest.Config()
	k := devnet().K()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	prover := &chunkProver{}
	c := composer.New(composer.Config{
		Batcher: &composer.Batcher{RollupID: cc.L2.(*config.L2Config).RollupID, ProofSystem: common.Address{0xab}, Signer: types.LatestSignerForChainID(cc.ChainID)},
		K:       k, MaxBlocks: composer.GridCap(300, k), MaxGas: composer.MaxPostBatchGas, Proof: time.Second,
	}, h.chain, noWitnesses{}, prover, &chunkPoster{key: key},
		&composer.Observer{Node: idleL1{}, Every: time.Millisecond, Logger: log.New()},
		func() uint64 { return 0 }, func() uint64 { return 0 }, h.follower.HeadTo, log.New())
	t.Cleanup(c.Close)
	s, err := sequencer.New(cc, sequencer.Config{Timing: devnet(), Composer: c}, h.follower, newFull(t, h), log.New())
	require.NoError(t, err)
	const sync = 24
	require.NoError(t, s.Handle(t.Context(), sequencer.SyncSlot{Height: sync, Time: h.genesis.Time + 2*sync, L1Head: 4}))
	require.Equal(t, uint64(sync), h.head(t).Number.Uint64())
	require.Equal(t, [][2]uint64{{1, k}}, prover.windows, "the one chunk within the gas limit")
}

// newFull returns a pool of h's chain that never runs dry.
func newFull(t *testing.T, h *harness) *full {
	t.Helper()
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
	return pool
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
	budget := sequencer.DABudget(devnet().K(), sequencer.MaxBatchGas)
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
