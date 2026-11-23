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

package composer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
)

func TestDecide(t *testing.T) {
	c := &Composer{cfg: Config{K: 4, MaxBlocks: 8}}
	const ts, target = 1000, 50
	for _, r := range []struct {
		name         string
		cursor, sync uint64
		mode         sequencer.Mode
		target       uint64
		inFlight     bool
		want         emission
	}{
		{"steady", 0, 8, sequencer.ModeSteady, target, false, emission{emitAnchor, 0, 8, post.Target{Block: target, Time: ts}}},
		{"catch-up", 0, 8, sequencer.ModeCatchup, target, false, emission{emitAnchor, 0, 8, post.Target{}}},
		{"empty", 0, 8, sequencer.ModeEmpty, 0, false, emission{}},
		{"in flight", 0, 8, sequencer.ModeSteady, target, true, emission{}},
		{"cap", 4, 12, sequencer.ModeSteady, target, false, emission{emitAnchor, 4, 12, post.Target{Block: target, Time: ts}}},
		{"backlog", 0, 12, sequencer.ModeSteady, target, false, emission{emitChunk, 0, 12, post.Target{Block: target}}},
		{"backlog in catch-up", 0, 12, sequencer.ModeCatchup, target, false, emission{emitChunk, 0, 12, post.Target{Block: target}}},
		{"backlog of an empty slot", 0, 12, sequencer.ModeEmpty, 0, false, emission{emitChunk, 0, 12, post.Target{}}},
		{"backlog in flight", 0, 12, sequencer.ModeSteady, target, true, emission{}},
	} {
		t.Run(r.name, func(t *testing.T) {
			require.Equal(t, r.want, c.decide(r.cursor, r.sync, ts, r.target, r.mode, r.inFlight))
		})
	}
}

var testSigner = types.LatestSignerForChainID(uint256.NewInt(6290))

// testChain is a chain of blocks 0..n, those in full carrying a transfer.
// As the node's block reader does, it reads no block above its head.
type testChain struct {
	blocks []*types.Block
	head   uint64
}

func newTestChain(t *testing.T, n uint64, full ...uint64) *testChain {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	c := &testChain{}
	var parent common.Hash
	for i := range n + 1 {
		h := &types.Header{Number: *uint256.NewInt(i), ParentHash: parent, Time: 1000 + 2*i, TxHash: empty.RootHash, Extra: []byte{}}
		var txs []types.Transaction
		for _, f := range full {
			if f == i {
				to := common.Address{1}
				tx, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: i, GasLimit: 21000, To: &to, Data: bytes.Repeat([]byte{1}, 200)}}, *testSigner, key)
				require.NoError(t, err)
				txs = []types.Transaction{tx}
			}
		}
		b := types.NewBlock(h, txs, nil, nil, nil, nil)
		c.blocks = append(c.blocks, b)
		parent = b.Hash()
	}
	c.head = n
	return c
}

func (c *testChain) BlockByNumber(_ context.Context, n uint64) (*types.Block, error) {
	if n > c.head {
		return nil, nil
	}
	return c.blocks[n], nil
}

func (c *testChain) HeaderByNumber(ctx context.Context, n uint64) (*types.Header, error) {
	b, err := c.BlockByNumber(ctx, n)
	if b == nil {
		return nil, err
	}
	return b.Header(), err
}

func (c *testChain) header(n uint64) *types.Header { return c.blocks[n].Header() }

type witnessStore struct{ missing map[uint64]bool }

func (w witnessStore) Get(_ context.Context, n uint64, _ common.Hash) (json.RawMessage, error) {
	if w.missing[n] {
		return nil, nil
	}
	return json.RawMessage(`{"state":["0x01"],"codes":[],"keys":["0x02"],"headers":[]}`), nil
}

var testProof = bytes.Repeat([]byte{0x11}, 65)

type proven struct {
	from, to   uint64
	batch      *protocol.ProofSystemBatch
	start      time.Time
	per        time.Duration
	witnessed  []uint64
	witnessErr error
}

type testProver struct {
	mu     sync.Mutex
	proven []proven
	err    error
	// before runs as a session begins.
	before func()
}

func (p *testProver) ProveBy(ctx context.Context, w *prove.Window, start time.Time, per time.Duration) ([]byte, error) {
	if p.before != nil {
		p.before()
	}
	got := proven{from: w.From, to: w.To, batch: w.Batch, start: start, per: per}
	if w.Anchor == nil || w.Anchor.Number.Uint64()+1 != w.From {
		return nil, errors.New("window without its anchor")
	}
	for n := w.From; n <= w.To; n++ {
		b, wit, err := w.Block(ctx, n)
		if err != nil {
			got.witnessErr = err
			break
		}
		if b.NumberU64() == n && wit != nil && len(wit.State) == 1 && len(wit.Keys) == 1 {
			got.witnessed = append(got.witnessed, n)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.proven = append(p.proven, got)
	if got.witnessErr != nil {
		return nil, got.witnessErr
	}
	return bytes.Clone(testProof), p.err
}

type testPoster struct {
	key    *ecdsa.PrivateKey
	mu     sync.Mutex
	inputs [][]byte
	gas    []uint64
	txs    []types.Transaction
	sent   []post.Target
	sends  int
	err    error
}

func (p *testPoster) Sign(_ context.Context, input []byte, gas uint64) (types.Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	to := common.Address{0xee}
	tx, err := types.SignTx(&types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: uint64(len(p.inputs)), GasLimit: gas, To: &to, Data: input}, ChainID: *uint256.NewInt(1)}, *types.LatestSignerForChainID(uint256.NewInt(1)), p.key)
	if err != nil {
		return nil, err
	}
	p.inputs, p.gas, p.txs = append(p.inputs, input), append(p.gas, gas), append(p.txs, tx)
	return tx, nil
}

func (p *testPoster) Send(_ context.Context, txs []types.Transaction, target post.Target) (post.Sent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sends++
	p.sent = append(p.sent, target)
	if p.err != nil {
		return post.Sent{}, p.err
	}
	return post.Sent{Block: max(target.Block, 99), Time: target.Time}, nil
}

func (p *testPoster) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// targets returns the targets of the postings signed so far, once all are
// sent.
func (h *harness) targets(t *testing.T) []post.Target {
	t.Helper()
	p := h.poster
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sends == len(p.inputs)
	}, 5*time.Second, time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sent)
}

func (h *harness) resolution(height uint64) (Resolution, bool) {
	h.c.ledger.mu.Lock()
	defer h.c.ledger.mu.Unlock()
	e := h.c.ledger.entries[height]
	if e == nil {
		return Pending, false
	}
	return e.resolution, true
}

// quiet is an L1 on which no posting lands before the observers stop.
type quiet struct{}

func (quiet) Head(context.Context) (*l1.Header, error)                  { return &l1.Header{Number: 1}, nil }
func (quiet) Receipt(context.Context, common.Hash) (*l1.Receipt, error) { return nil, nil }

type harness struct {
	chain  *testChain
	prover *testProver
	poster *testPoster
	cursor atomic.Uint64
	final  atomic.Uint64
	c      *Composer
	// rewound are the blocks headTo moved the head back to.
	rewound []common.Hash
}

func newHarness(t *testing.T, chain *testChain, maxBlocks uint64, node Receipts) *harness {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	h := &harness{chain: chain, prover: &testProver{}, poster: &testPoster{key: key}}
	if node == nil {
		node = quiet{}
	}
	cfg := Config{
		Batcher: &Batcher{RollupID: 1, ProofSystem: common.Address{0xab}, Signer: testSigner},
		K:       4, MaxBlocks: maxBlocks, MaxGas: MaxPostBatchGas, Proof: time.Second, Slack: 100 * time.Millisecond,
	}
	obs := &Observer{Node: node, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New()}
	headTo := func(_ context.Context, hash common.Hash) error {
		h.rewound = append(h.rewound, hash)
		return nil
	}
	h.c = New(cfg, chain, witnessStore{}, h.prover, h.poster, obs, h.cursor.Load, h.final.Load, headTo, log.New())
	t.Cleanup(h.c.Close)
	return h
}

// slot runs the Sync slot at sync the way the sequencer does.
func (h *harness) slot(t *testing.T, sync, target uint64, mode sequencer.Mode) {
	t.Helper()
	ctx := t.Context()
	h.c.RecoverFailed(ctx)
	ts := h.chain.header(sync).Time
	require.Equal(t, sequencer.Composed{Empty: true}, h.c.ComposeSyncSlot(ctx, h.chain.header(sync-1), ts, target, mode))
	h.c.SyncCommitted(ctx, h.chain.header(sync))
}

// requireWindow checks the posting of blocks from..to: proven over the
// window with its witnesses, posted with the proof, settling the cursor's
// block to the terminal.
func (h *harness) requireWindow(t *testing.T, i int, from, to uint64, target post.Target) {
	t.Helper()
	p := h.prover.proven[i]
	require.Equal(t, [2]uint64{from, to}, [2]uint64{p.from, p.to})
	var want []uint64
	for n := from; n <= to; n++ {
		want = append(want, n)
	}
	require.Equal(t, want, p.witnessed)
	u := p.batch.Entries[0].RollupUpdates[0]
	require.Equal(t, h.chain.blocks[from-1].Hash(), u.CurrentRoot)
	require.Equal(t, h.chain.blocks[to].Hash(), u.NewRoot)
	require.Equal(t, [][]byte{testProof}, p.batch.Proofs)
	input, err := protocol.EncodePostAndVerifyBatch(p.batch)
	require.NoError(t, err)
	require.Equal(t, input, h.poster.inputs[i])
	require.Equal(t, uint64(MaxPostBatchGas), h.poster.gas[i])
	require.Equal(t, target, h.targets(t)[i])
}

func TestComposerPostsAnAnchor(t *testing.T) {
	h := newHarness(t, newTestChain(t, 16, 3, 9), 8, nil)
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	ts := h.chain.header(8).Time
	h.requireWindow(t, 0, 1, 8, post.Target{Block: 50, Time: ts})
	p := h.prover.proven[0]
	require.Equal(t, time.Unix(int64(ts), 0).Add(-100*time.Millisecond-time.Second), p.start)
	require.Equal(t, time.Second, p.per)
	at, ok := h.c.ledger.Blocking(0)
	require.True(t, ok)
	require.Equal(t, uint64(8), at)
	require.Equal(t, h.poster.txs[0].Hash(), h.c.ledger.entries[8].Post)

	// One batch in flight: derivation must pass it first.
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	h.cursor.Store(8)
	h.slot(t, 16, 54, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 2)
	h.requireWindow(t, 1, 9, 16, post.Target{Block: 54, Time: h.chain.header(16).Time})
}

func TestComposerCatchup(t *testing.T) {
	h := newHarness(t, newTestChain(t, 8), 8, nil)
	before := time.Now()
	h.slot(t, 8, 50, sequencer.ModeCatchup)
	h.requireWindow(t, 0, 1, 8, post.Target{})
	p := h.prover.proven[0]
	require.Zero(t, p.per)
	require.WithinRange(t, p.start, before.Add(time.Second), time.Now().Add(time.Second))
}

func TestComposerEmptySlot(t *testing.T) {
	h := newHarness(t, newTestChain(t, 8), 8, nil)
	h.slot(t, 8, 0, sequencer.ModeEmpty)
	require.Empty(t, h.prover.proven)
	require.Empty(t, h.targets(t))
	_, ok := h.c.ledger.Blocking(0)
	require.False(t, ok)
}

// TestComposerChunk: a backlog over the cap posts up to the highest empty
// grid block within it, here block 4, as block 8 carries a transfer.
func TestComposerChunk(t *testing.T) {
	h := newHarness(t, newTestChain(t, 12, 2, 8), 8, nil)
	h.slot(t, 12, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	h.requireWindow(t, 0, 1, 4, post.Target{Block: 50})
	require.Zero(t, h.prover.proven[0].per)
	at, ok := h.c.ledger.Blocking(0)
	require.True(t, ok)
	require.Equal(t, uint64(4), at)
}

// TestComposerFallsBackToAChunk: an anchor over the gas limit gives way to
// the largest chunk within it, unpinned.
func TestComposerFallsBackToAChunk(t *testing.T) {
	chain := newTestChain(t, 12, 9, 10, 11)
	h := newHarness(t, chain, 12, nil)
	chunk, err := h.c.cfg.Batcher.AnchorBatch(chain.blocks[0].Hash(), chain.blocks[1:9])
	require.NoError(t, err)
	h.c.cfg.MaxGas, err = PostBatchGas(chunk)
	require.NoError(t, err)
	h.slot(t, 12, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	p := h.prover.proven[0]
	require.Equal(t, [2]uint64{1, 8}, [2]uint64{p.from, p.to})
	require.Equal(t, post.Target{}, h.targets(t)[0])
	require.Equal(t, h.c.cfg.MaxGas, h.poster.gas[0])
}

func TestComposerPostsNothingItCannotProve(t *testing.T) {
	t.Run("prover failure", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 8, nil)
		h.prover.err = errors.New("prover down")
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.NotEmpty(t, h.prover.proven)
		require.Empty(t, h.targets(t))
		_, ok := h.c.ledger.Blocking(0)
		require.False(t, ok)
		h.prover.err = nil
		h.slot(t, 12, 52, sequencer.ModeSteady)
		require.Len(t, h.targets(t), 1)
	})
	t.Run("missing witness", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, nil)
		// Block 3 is in the anchor's window and in the chunk's after it.
		h.c.witnesses = witnessStore{missing: map[uint64]bool{3: true}}
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.Empty(t, h.targets(t))
		require.Len(t, h.prover.proven, 2)
		for _, p := range h.prover.proven {
			require.ErrorContains(t, p.witnessErr, "no witness of block 3")
		}
		require.Equal(t, []common.Hash{h.chain.blocks[2].Hash()}, h.rewound, "the head moves below the block to rebuild it")
	})
	t.Run("send failure", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 12, nil)
		h.poster.fail(errors.New("builder down"))
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Failed }, 5*time.Second, time.Millisecond)
		h.poster.fail(nil)
		h.slot(t, 12, 52, sequencer.ModeSteady)
		require.Len(t, h.targets(t), 2)
		h.requireWindow(t, 1, 1, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
	})
	t.Run("another block committed", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, nil)
		ctx := t.Context()
		h.c.ComposeSyncSlot(ctx, h.chain.header(7), h.chain.header(8).Time, 50, sequencer.ModeSteady)
		h.c.SyncCommitted(ctx, h.chain.header(7))
		require.Empty(t, h.prover.proven)
	})
}

// TestComposerRewoundUnder: blocks a rewind took after the slot was decided
// fail its posting, and none reads as a block without a witness.
func TestComposerRewoundUnder(t *testing.T) {
	t.Run("before the posting", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, nil)
		ctx := t.Context()
		h.c.ComposeSyncSlot(ctx, h.chain.header(7), h.chain.header(8).Time, 50, sequencer.ModeSteady)
		h.chain.head = 2
		h.c.SyncCommitted(ctx, h.chain.header(8))
		require.Empty(t, h.prover.proven)
		require.Empty(t, h.targets(t))
		require.Empty(t, h.rewound)
	})
	t.Run("while proving", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, nil)
		h.prover.before = func() { h.chain.head = 2 }
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.Len(t, h.prover.proven, 1)
		require.ErrorContains(t, h.prover.proven[0].witnessErr, "no canonical block 3")
		require.Empty(t, h.targets(t))
		require.Empty(t, h.rewound)
	})
}

func TestComposerRecovers(t *testing.T) {
	t.Run("a failed batch posts again", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 12, nil)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		h.targets(t)
		h.c.ledger.MarkFailed(8, h.poster.txs[0].Hash(), false)
		h.slot(t, 12, 52, sequencer.ModeSteady)
		require.Len(t, h.targets(t), 2)
		h.requireWindow(t, 1, 1, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
	})
	t.Run("an L1 reorg rolls a settlement out", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 12, nil)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		// Sends run off the slot: the first is sent before the second.
		h.targets(t)
		h.c.ledger.MarkSettled(8, h.poster.txs[0].Hash())
		h.cursor.Store(8)
		h.c.RecoverFailed(t.Context())
		h.cursor.Store(4)
		h.slot(t, 12, 52, sequencer.ModeSteady)
		require.Len(t, h.targets(t), 2)
		h.requireWindow(t, 1, 5, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
	})
	t.Run("finality ends a settlement", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, nil)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		h.c.ledger.MarkSettled(8, h.poster.txs[0].Hash())
		h.cursor.Store(8)
		h.final.Store(8)
		h.c.RecoverFailed(t.Context())
		require.Empty(t, h.c.ledger.entries)
	})
}

// settling is an L1 on which every posting settles at its terminal.
type settling struct {
	terminal common.Hash
	t        *testing.T
}

func (s settling) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 1}, nil }

func (s settling) Receipt(context.Context, common.Hash) (*l1.Receipt, error) {
	return receipt(1, batchPosted(s.t, eezL1), performed(s.t, eezL1, 1, s.terminal)), nil
}

func TestComposerObserves(t *testing.T) {
	chain := newTestChain(t, 8)
	h := newHarness(t, chain, 8, settling{terminal: chain.blocks[8].Hash(), t: t})
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Eventually(t, func() bool {
		h.c.ledger.mu.Lock()
		defer h.c.ledger.mu.Unlock()
		e := h.c.ledger.entries[8]
		return e != nil && e.resolution == Settled
	}, 5*time.Second, time.Millisecond)
}

// TestComposerFallsBackFromAFreshCursor: the chunk replacing a failed anchor
// starts where derivation is when it is posted.
func TestComposerFallsBackFromAFreshCursor(t *testing.T) {
	h := newHarness(t, newTestChain(t, 12), 12, nil)
	h.prover.err = errors.New("prover down")
	ctx := t.Context()
	h.c.RecoverFailed(ctx)
	h.c.ComposeSyncSlot(ctx, h.chain.header(11), h.chain.header(12).Time, 50, sequencer.ModeSteady)
	h.cursor.Store(4)
	h.c.SyncCommitted(ctx, h.chain.header(12))
	require.Len(t, h.prover.proven, 2)
	require.Equal(t, [2]uint64{1, 12}, [2]uint64{h.prover.proven[0].from, h.prover.proven[0].to})
	require.Equal(t, [2]uint64{5, 8}, [2]uint64{h.prover.proven[1].from, h.prover.proven[1].to})
}

// TestComposerChunkWithinTheGasLimit: a candidate over the gas limit gives
// way to the next one down the grid.
func TestComposerChunkWithinTheGasLimit(t *testing.T) {
	chain := newTestChain(t, 12, 5, 6, 7)
	h := newHarness(t, chain, 8, nil)
	small, err := h.c.cfg.Batcher.AnchorBatch(chain.blocks[0].Hash(), chain.blocks[1:5])
	require.NoError(t, err)
	h.c.cfg.MaxGas, err = PostBatchGas(small)
	require.NoError(t, err)
	h.slot(t, 12, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	require.Equal(t, [2]uint64{1, 4}, [2]uint64{h.prover.proven[0].from, h.prover.proven[0].to})
	require.Equal(t, post.Target{Block: 50}, h.targets(t)[0])
}

// landing is an L1 on which postings settle at terminal until it drops them.
type landing struct {
	t        *testing.T
	terminal atomic.Pointer[common.Hash]
	gone     atomic.Bool
	asked    atomic.Int64
}

func (l *landing) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 1}, nil }

func (l *landing) Receipt(context.Context, common.Hash) (*l1.Receipt, error) {
	l.asked.Add(1)
	if l.gone.Load() {
		return nil, nil
	}
	return receipt(1, batchPosted(l.t, eezL1), performed(l.t, eezL1, 1, *l.terminal.Load())), nil
}

func newLanding(t *testing.T, terminal common.Hash) *landing {
	l := &landing{t: t}
	l.terminal.Store(&terminal)
	return l
}

// TestComposerSettlementLeftL1: a settlement whose posting L1 dropped before
// derivation saw it fails, and the next slot posts again, though the cursor
// never moved.
func TestComposerSettlementLeftL1(t *testing.T) {
	chain := newTestChain(t, 12)
	node := newLanding(t, chain.blocks[8].Hash())
	h := newHarness(t, chain, 12, node)
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1, "the settlement holds the gate while on L1")
	node.gone.Store(true)
	h.c.RecoverFailed(t.Context())
	_, ok := h.resolution(8)
	require.False(t, ok, "dropped")
	h.c.ComposeSyncSlot(t.Context(), h.chain.header(11), h.chain.header(12).Time, 52, sequencer.ModeSteady)
	h.c.SyncCommitted(t.Context(), h.chain.header(12))
	require.Len(t, h.targets(t), 2)
	h.requireWindow(t, 1, 1, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
}

// TestComposerDisposesOfPartialSettlements: derivation passing a partial
// settlement settles it, and finality ends it.
func TestComposerDisposesOfPartialSettlements(t *testing.T) {
	h := newHarness(t, newTestChain(t, 8), 8, nil)
	h.slot(t, 8, 50, sequencer.ModeSteady)
	h.targets(t)
	h.c.ledger.MarkSettledPartial(8, h.poster.txs[0].Hash())
	h.cursor.Store(8)
	h.final.Store(8)
	h.c.RecoverFailed(t.Context())
	require.Empty(t, h.c.ledger.entries)
}

// TestComposerAuditsFinality: a finalized settlement's posting is looked up
// on L1 before it leaves the ledger.
func TestComposerAuditsFinality(t *testing.T) {
	chain := newTestChain(t, 8)
	node := newLanding(t, chain.blocks[8].Hash())
	h := newHarness(t, chain, 8, node)
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
	before := node.asked.Load()
	h.cursor.Store(8)
	h.final.Store(8)
	h.c.RecoverFailed(t.Context())
	require.Empty(t, h.c.ledger.entries)
	require.Eventually(t, func() bool { return node.asked.Load() > before }, 5*time.Second, time.Millisecond)
}
