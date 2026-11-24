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
	"fmt"
	"math"
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
	"github.com/erigontech/erigon/eez/follower"
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
		{"steady", 0, 8, sequencer.ModeSteady, target, false, emission{kind: emitAnchor, cursor: 0, sync: 8, target: post.Target{Block: target, Time: ts}}},
		{"catch-up", 0, 8, sequencer.ModeCatchup, target, false, emission{kind: emitAnchor, cursor: 0, sync: 8}},
		{"empty", 0, 8, sequencer.ModeEmpty, 0, false, emission{}},
		{"in flight", 0, 8, sequencer.ModeSteady, target, true, emission{}},
		{"cap", 4, 12, sequencer.ModeSteady, target, false, emission{kind: emitAnchor, cursor: 4, sync: 12, target: post.Target{Block: target, Time: ts}}},
		{"backlog", 0, 12, sequencer.ModeSteady, target, false, emission{kind: emitChunk, cursor: 0, sync: 12, target: post.Target{Block: target}}},
		{"backlog in catch-up", 0, 12, sequencer.ModeCatchup, target, false, emission{kind: emitChunk, cursor: 0, sync: 12, target: post.Target{Block: target}}},
		{"backlog of an empty slot", 0, 12, sequencer.ModeEmpty, 0, false, emission{kind: emitChunk, cursor: 0, sync: 12}},
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
	// reads counts the blocks and headers read.
	reads atomic.Int64
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
	c.reads.Add(1)
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
	l1Parent   common.Hash
	time       uint64
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
	got := proven{l1Parent: w.L1Parent, time: w.Time, from: w.From, to: w.To, batch: w.Batch, start: start, per: per}
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
	key     *ecdsa.PrivateKey
	mu      sync.Mutex
	inputs  [][]byte
	gas     []uint64
	txs     []types.Transaction
	sent    []post.Target
	bundles [][]common.Hash
	sends   int
	err     error
	// pooled has the postings sent through the mempool.
	pooled bool
	// hang has Sign wait for its context.
	hang bool
}

func (p *testPoster) Sign(ctx context.Context, input []byte, gas uint64) (types.Transaction, error) {
	if p.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
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
	var hashes []common.Hash
	for _, tx := range txs {
		hashes = append(hashes, tx.Hash())
	}
	p.bundles = append(p.bundles, hashes)
	if errors.Is(p.err, post.ErrUnsure) {
		return post.Sent{Block: max(target.Block, 99), Pooled: true}, p.err
	}
	if p.err != nil {
		return post.Sent{}, p.err
	}
	return post.Sent{Block: max(target.Block, 99), Time: target.Time, Pooled: p.pooled}, nil
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

func (quiet) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (quiet) Head(context.Context) (*l1.Header, error)                  { return &l1.Header{Number: 1}, nil }
func (quiet) Receipt(context.Context, common.Hash) (*l1.Receipt, error) { return nil, nil }
func (quiet) HasTransaction(context.Context, common.Hash) (bool, error) { return false, nil }

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
	kept := h.c.ledger.entries[8].Batch
	require.Equal(t, h.prover.proven[0].batch.Entries, kept.Entries)
	require.Nil(t, kept.CallData, "the ledger keeps no DA")
	require.Nil(t, kept.Proofs)

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
		node := &rolling{}
		h := newHarness(t, newTestChain(t, 12), 12, node)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		// Sends run off the slot: the first is sent before the second.
		h.targets(t)
		h.c.ledger.MarkSettled(8, h.poster.txs[0].Hash())
		h.cursor.Store(8)
		h.c.RecoverFailed(t.Context())
		h.cursor.Store(4)
		h.c.RecoverFailed(t.Context())
		// Off L1 and out of the node's pool, its posting fails once L1 moves
		// on; the recovery that watches it again takes the verdict when it
		// comes first.
		require.Eventually(t, func() bool {
			node.head.Add(1)
			r, ok := h.resolution(8)
			return !ok || r == Failed
		}, 5*time.Second, time.Millisecond)
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

func (s settling) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (s settling) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 1}, nil }

func (settling) HasTransaction(context.Context, common.Hash) (bool, error) { return true, nil }

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

// TestComposerWaitsOnAPooledPosting: a posting sent through the mempool
// holds the gate while the node has it, up to pooledGrace L1 blocks past its
// target; past them it fails, and the next posting replaces it at its nonce.
func TestComposerWaitsOnAPooledPosting(t *testing.T) {
	node := &rolling{}
	node.head.Store(99)
	node.has.Store(true)
	h := newHarness(t, newTestChain(t, 12), 12, node)
	h.poster.pooled = true
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	node.head.Store(99 + pooledGrace)
	asked := node.asked.Load()
	require.Eventually(t, func() bool { return node.asked.Load() > asked+20 }, 5*time.Second, time.Millisecond)
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Pending, r)
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1, "the pooled posting holds the gate within its grace")
	node.head.Store(100 + pooledGrace)
	require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Failed }, 5*time.Second, time.Millisecond)
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 2)
	h.requireWindow(t, 1, 1, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
}

// TestComposerAuditsLandedSettlements: finality audits the posting of a
// settlement that landed, not one derivation passed without it.
func TestComposerAuditsLandedSettlements(t *testing.T) {
	t.Run("landed", func(t *testing.T) {
		chain := newTestChain(t, 8)
		node := newLanding(t, chain.blocks[8].Hash())
		h := newHarness(t, chain, 8, node)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
		h.cursor.Store(8)
		h.final.Store(8)
		asked := node.asked.Load()
		h.c.RecoverFailed(t.Context())
		h.c.Close()
		require.Equal(t, asked+1, node.asked.Load(), "audited")
	})
	t.Run("passed without landing", func(t *testing.T) {
		node := &rolling{}
		node.head.Store(1000)
		h := newHarness(t, newTestChain(t, 8), 8, node)
		h.slot(t, 8, 50, sequencer.ModeSteady)
		require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Failed }, 5*time.Second, time.Millisecond)
		h.cursor.Store(8)
		h.final.Store(8)
		asked := node.asked.Load()
		h.c.RecoverFailed(t.Context())
		h.c.Close()
		_, ok := h.resolution(8)
		require.False(t, ok, "finalized")
		require.Equal(t, asked, node.asked.Load(), "not audited")
	})
}

// TestComposerChunkReadsOnce: falling back from a backlog to its smallest
// chunk reads the backlog's blocks once, not once per larger candidate.
func TestComposerChunkReadsOnce(t *testing.T) {
	const sync = 48
	var full []uint64
	for n := uint64(1); n < sync; n++ {
		if n%4 != 0 {
			full = append(full, n)
		}
	}
	chain := newTestChain(t, sync, full...)
	h := newHarness(t, chain, sync, nil)
	small, err := h.c.cfg.Batcher.AnchorBatch(chain.blocks[0].Hash(), chain.blocks[1:5])
	require.NoError(t, err)
	h.c.cfg.MaxGas, err = PostBatchGas(small)
	require.NoError(t, err)
	h.slot(t, sync, 50, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1)
	require.Equal(t, [2]uint64{1, 4}, [2]uint64{h.prover.proven[0].from, h.prover.proven[0].to})
	// The anchor's blocks, the chunk's below it, the cursor and the window's.
	require.LessOrEqual(t, chain.reads.Load(), int64(sync+(sync-4)+1+4))
}

// TestComposerWatchesAnUnsureSend: a posting whose send failed without an
// answer may have reached L1: it holds the gate while the node has it, and
// fails once the node has lost it.
func TestComposerWatchesAnUnsureSend(t *testing.T) {
	node := &rolling{}
	node.head.Store(99)
	node.has.Store(true)
	h := newHarness(t, newTestChain(t, 12), 12, node)
	h.poster.fail(fmt.Errorf("%w: connection reset", post.ErrUnsure))
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Eventually(t, func() bool { return node.asked.Load() > 20 }, 5*time.Second, time.Millisecond)
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Pending, r)
	h.poster.fail(nil)
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1, "the unsure posting holds the gate")
	node.has.Store(false)
	require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Failed }, 5*time.Second, time.Millisecond)
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 2)
}

// hung is an L1 node that answers no read before its context ends.
type hung struct{}

func (hung) Head(ctx context.Context) (*l1.Header, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hung) HeaderByNumber(ctx context.Context, _ uint64) (*l1.Header, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hung) Receipt(ctx context.Context, _ common.Hash) (*l1.Receipt, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hung) HasTransaction(ctx context.Context, _ common.Hash) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// TestComposerOutlivesAHungL1: the L1 reads the composer makes on the
// sequencer's goroutine end within L1Read, so a node that stops answering
// does not hold block production.
func TestComposerOutlivesAHungL1(t *testing.T) {
	within := func(t *testing.T, f func()) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			f()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the sequencer waits on L1")
		}
	}
	t.Run("a settlement checked", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 12, hung{})
		h.c.cfg.L1Read = 50 * time.Millisecond
		h.slot(t, 8, 50, sequencer.ModeSteady)
		h.targets(t)
		h.c.ledger.MarkSettled(8, h.poster.txs[0].Hash())
		ctx := t.Context()
		within(t, func() {
			h.c.RecoverFailed(ctx)
			h.c.ComposeSyncSlot(ctx, h.chain.header(11), h.chain.header(12).Time, 52, sequencer.ModeSteady)
		})
		r, _ := h.resolution(8)
		require.Equal(t, Settled, r, "an unanswered read changes nothing")
	})
	t.Run("a posting signed", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 12), 12, hung{})
		h.c.cfg.L1Read = 50 * time.Millisecond
		h.poster.hang = true
		ctx := t.Context()
		within(t, func() {
			h.c.ComposeSyncSlot(ctx, h.chain.header(7), h.chain.header(8).Time, 50, sequencer.ModeSteady)
			h.c.SyncCommitted(ctx, h.chain.header(8))
		})
		_, ok := h.c.ledger.Blocking(0)
		require.False(t, ok, "nothing posted")
	})
	t.Run("a partial batch disposed of", func(t *testing.T) {
		h := newHarness(t, newTestChain(t, 8), 8, hung{})
		h.c.cfg.L1Read = 50 * time.Millisecond
		pool := NewHeldPool()
		h.c.Recover(pool, nil)
		require.NoError(t, pool.Push(held(addr(0xa), 0, Inbound, 1), 0))
		h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Terminal: h.chain.header(8), Txs: pool.PopN(1)})
		h.c.ledger.MarkSettledPartial(8, common.Hash{0xf})
		h.cursor.Store(8)
		within(t, func() { h.c.RecoverFailed(t.Context()) })
		r, _ := h.resolution(8)
		require.Equal(t, SettledPartial, r, "an unanswered read changes nothing")
	})
}

// rolling is an L1 node whose head and pool the test moves; no posting has
// a receipt. While down, its head reads fail.
type rolling struct {
	head  atomic.Uint64
	has   atomic.Bool
	down  atomic.Bool
	asked atomic.Int64
}

func (r *rolling) Head(context.Context) (*l1.Header, error) {
	if r.down.Load() {
		return nil, errors.New("node down")
	}
	return &l1.Header{Number: r.head.Load()}, nil
}

func (r *rolling) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	if r.down.Load() {
		return nil, errors.New("node down")
	}
	return &l1.Header{Number: n}, nil
}

func (r *rolling) Receipt(context.Context, common.Hash) (*l1.Receipt, error) {
	r.asked.Add(1)
	return nil, nil
}

func (r *rolling) HasTransaction(context.Context, common.Hash) (bool, error) {
	return r.has.Load(), nil
}

// TestComposerRewatchesFromAKnownHead: a settlement watched again while the
// L1 head cannot be read waits for it, so the grace of a posting the node
// holds counts from the head, not from block zero.
func TestComposerRewatchesFromAKnownHead(t *testing.T) {
	node := &rolling{}
	node.head.Store(1)
	h := newHarness(t, newTestChain(t, 12), 12, node)
	ctx := t.Context()
	h.slot(t, 8, 50, sequencer.ModeSteady)
	h.targets(t)
	h.cursor.Store(8)
	h.c.RecoverFailed(ctx)
	node.has.Store(true)
	node.head.Store(100)
	node.down.Store(true)
	h.cursor.Store(4)
	h.c.RecoverFailed(ctx)
	time.Sleep(50 * time.Millisecond)
	node.down.Store(false)
	time.Sleep(200 * time.Millisecond)
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Pending, r, "the node holds it within its grace from block 100")
}

// TestComposerIgnoresAStaleWatch: a bundle's first watch, still running
// when an L1 reorg rolls its settlement out and the posting is watched again
// from the pool, does not fail it once L1 passes the bundle's block.
func TestComposerIgnoresAStaleWatch(t *testing.T) {
	node := &rolling{}
	node.head.Store(1)
	h := newHarness(t, newTestChain(t, 12), 12, node)
	ctx := t.Context()
	h.slot(t, 8, 50, sequencer.ModeSteady)
	h.targets(t)
	h.cursor.Store(8)
	h.c.RecoverFailed(ctx)
	r, _ := h.resolution(8)
	require.Equal(t, Settled, r, "derivation confirmed it")
	node.has.Store(true)
	node.head.Store(98)
	h.cursor.Store(4)
	h.c.RecoverFailed(ctx)
	node.head.Store(100)
	asked := node.asked.Load()
	require.Eventually(t, func() bool { return node.asked.Load() > asked+40 }, 5*time.Second, time.Millisecond)
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Pending, r, "the posting back in the pool may land")
	h.slot(t, 12, 52, sequencer.ModeSteady)
	require.Len(t, h.targets(t), 1, "the gate holds")
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

// slotted is an L1 on which no posting lands, its block of number n having
// time times[n]; failed fails the header reads.
type slotted struct {
	times  map[uint64]uint64
	failed error
}

func (slotted) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 1 << 20}, nil }

func (s slotted) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	if s.failed != nil {
		return nil, s.failed
	}
	return &l1.Header{Number: n, Time: s.times[n]}, nil
}

func (slotted) Receipt(context.Context, common.Hash) (*l1.Receipt, error) { return nil, nil }
func (slotted) HasTransaction(context.Context, common.Hash) (bool, error) { return false, nil }

// TestComposerChargesOnlyItsOwnFailures: a batch that failed in its L1 slot,
// or that the builder refused, failed for what it holds; one whose slot no
// block took, or that never reached the builder, did not.
func TestComposerChargesOnlyItsOwnFailures(t *testing.T) {
	for _, c := range []struct {
		name    string
		shift   uint64 // of the time of the target block from the pin
		read    error
		send    error
		skipped bool
	}{
		{name: "a failure in its slot"},
		{name: "a missed slot", shift: 12, skipped: true},
		{name: "a slot that cannot be read", read: errors.New("node down"), skipped: true},
		{name: "a refused bundle", send: fmt.Errorf("%w: simulation reverted (-32000)", post.ErrBundleRefused)},
		{name: "a transport failure", send: errors.New("connection refused"), skipped: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			chain := newTestChain(t, 8)
			ts := chain.header(8).Time
			h := newHarness(t, chain, 8, slotted{times: map[uint64]uint64{100: ts + c.shift}, failed: c.read})
			h.poster.err = c.send
			h.slot(t, 8, 100, sequencer.ModeSteady)
			require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Failed }, 5*time.Second, time.Millisecond)
			h.c.ledger.mu.Lock()
			defer h.c.ledger.mu.Unlock()
			require.Equal(t, c.skipped, h.c.ledger.entries[8].SlotSkipped)
		})
	}
}

// TestComposerReleasesSettledTransactions: once derivation passes a
// settled batch, its held transactions are no longer reserved.
func TestComposerReleasesSettledTransactions(t *testing.T) {
	h := newHarness(t, newTestChain(t, 8), 8, nil)
	pool := NewHeldPool()
	h.c.Recover(pool, nil)
	tx := held(addr(0xa), 0, Inbound, 1)
	require.NoError(t, pool.Push(tx, 0))
	h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Txs: pool.PopN(1)})
	h.c.ledger.MarkSettled(8, common.Hash{0xf})
	h.cursor.Store(8)
	h.c.RecoverFailed(t.Context())
	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Empty(t, pool.inFlight)
}

// TestComposerRequeuesRolledOutTransactions: a settled batch an L1 reorg
// rolls out, which derivation never confirmed, is watched again with its
// transactions reserved; once its posting has left L1 for good, recovery
// requeues them.
func TestComposerRequeuesRolledOutTransactions(t *testing.T) {
	node := &rolling{}
	node.head.Store(1)
	h := newHarness(t, newTestChain(t, 8), 8, node)
	pool := NewHeldPool()
	h.c.Recover(pool, func(context.Context, *types.Header) (*types.Header, error) { return nil, nil })
	tx := held(addr(0xa), 0, Inbound, 1)
	require.NoError(t, pool.Push(tx, 0))
	h.cursor.Store(5)
	h.c.RecoverFailed(t.Context())
	h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Terminal: h.chain.header(8), Txs: pool.PopN(1)})
	h.c.ledger.MarkSettled(8, common.Hash{0xf})
	h.cursor.Store(4)
	h.c.RecoverFailed(t.Context())
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Pending, r, "watched again")
	require.Eventually(t, func() bool {
		node.head.Add(1)
		h.c.RecoverFailed(t.Context())
		_, ok := h.resolution(8)
		return !ok
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, []*Held{tx}, pool.PopN(math.MaxInt), "requeued")
}

// TestComposerDisposesOfPartialTransactions: once derivation passes a batch
// L1 kept part of, the triggers L1 has are spent and let go; the rest return
// to the pool, and only the one where L1 stopped is charged.
func TestComposerDisposesOfPartialTransactions(t *testing.T) {
	for _, c := range []struct {
		name     string
		attempts uint32 // of the first trigger
		lookup   error
		// spentFirst has the first trigger run on L1.
		spentFirst bool
		allSpent   bool
		// queued are the transactions back in the pool, with their attempts.
		queued map[common.Hash]uint32
		kept   bool // the batch waits for the next call
	}{
		{name: "the rest requeued, the first unspent charged", queued: map[common.Hash]uint32{{2}: 1, {4}: 0, {5}: 0}},
		{name: "the charge evicts the chain", attempts: 2, queued: map[common.Hash]uint32{{4}: 0}},
		{name: "none charged behind a spent trigger", spentFirst: true, queued: map[common.Hash]uint32{{4}: 0, {5}: 0}},
		{name: "a lookup failing", lookup: errors.New("node down"), queued: map[common.Hash]uint32{{5}: 0}, kept: true},
		{name: "every trigger spent", allSpent: true, queued: map[common.Hash]uint32{{5}: 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			node := &triggers{landed: map[common.Hash]bool{}, failed: c.lookup}
			h := newHarness(t, newTestChain(t, 8), 8, node)
			pool := NewHeldPool()
			h.c.Recover(pool, nil)
			cut := held(addr(0xb), 0, Inbound, 2)
			cut.Attempts = c.attempts
			behind := held(addr(0xb), 1, Inbound, 5)
			landed, dropped := held(addr(0xc), 0, Inbound, 3), held(addr(0xd), 0, Inbound, 4)
			node.landed[landed.Hash] = true
			if c.spentFirst {
				node.landed[cut.Hash] = true
			}
			if c.allSpent {
				node.landed[cut.Hash], node.landed[dropped.Hash] = true, true
			}
			for _, tx := range []*Held{cut, behind, landed, dropped} {
				require.NoError(t, pool.Push(tx, 0))
			}
			txs := pool.PopN(math.MaxInt)
			// b's second transaction missed the batch: it waits in the pool.
			pool.PushFront([]*Held{behind})
			txs = slices.DeleteFunc(txs, func(tx *Held) bool { return tx == behind })
			h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Terminal: h.chain.header(8), Txs: txs})
			h.c.ledger.MarkSettledPartial(8, common.Hash{0xf})
			h.cursor.Store(8)

			h.c.RecoverFailed(t.Context())
			r, _ := h.resolution(8)
			if c.kept {
				require.Equal(t, SettledPartial, r)
			} else {
				require.Equal(t, Settled, r)
				h.c.ledger.mu.Lock()
				var spent []common.Hash
				for _, tx := range h.c.ledger.entries[8].Txs {
					spent = append(spent, tx.Hash)
				}
				h.c.ledger.mu.Unlock()
				require.Contains(t, spent, landed.Hash, "kept for a reorg")
				require.Equal(t, c.allSpent, slices.Contains(spent, dropped.Hash), "only what ran is kept")
			}
			got := map[common.Hash]uint32{}
			for _, tx := range pool.PopN(math.MaxInt) {
				got[tx.Hash] = tx.Attempts
			}
			require.Equal(t, c.queued, got)
			if !c.kept {
				pool.mu.Lock()
				defer pool.mu.Unlock()
				require.Len(t, pool.inFlight, len(got), "only the requeued, now drained, are reserved")
			}
		})
	}
}

// TestPartialCulprit: L1 stopped a partial batch at its first trigger when
// unspent; a later trigger may only be stuck behind one that ran and
// reverted.
func TestPartialCulprit(t *testing.T) {
	spentOf := func(txs ...*Held) map[common.Hash]bool {
		m := map[common.Hash]bool{}
		for _, tx := range txs {
			m[tx.Hash] = true
		}
		return m
	}
	b := addr(0xb)
	t.Run("the first when unspent", func(t *testing.T) {
		dropped, behind := held(b, 4, Inbound, 4), held(b, 5, Inbound, 5)
		require.Equal(t, dropped, partialCulprit([]*Held{dropped, behind}, spentOf()))
	})
	t.Run("none behind a spent one", func(t *testing.T) {
		reverted, stuck := held(b, 3, Inbound, 3), held(b, 4, Inbound, 4)
		require.Nil(t, partialCulprit([]*Held{reverted, stuck}, spentOf(reverted)))
	})
	t.Run("none when all are spent", func(t *testing.T) {
		x, y := held(b, 1, Inbound, 1), held(b, 2, Inbound, 2)
		require.Nil(t, partialCulprit([]*Held{x, y}, spentOf(x, y)))
	})
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

// landing is an L1 on which postings settle at terminal until it drops them,
// back to its pool when pooled, or, reorged, includes them again as again
// has them.
type landing struct {
	t        *testing.T
	terminal atomic.Pointer[common.Hash]
	gone     atomic.Bool
	pooled   atomic.Bool
	head     atomic.Uint64
	again    atomic.Pointer[func() *l1.Receipt]
	asked    atomic.Int64
}

func (l *landing) Head(context.Context) (*l1.Header, error) {
	return &l1.Header{Number: l.head.Load()}, nil
}

func (l *landing) HasTransaction(context.Context, common.Hash) (bool, error) {
	return !l.gone.Load() || l.pooled.Load(), nil
}

func (l *landing) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (l *landing) Receipt(context.Context, common.Hash) (*l1.Receipt, error) {
	l.asked.Add(1)
	if l.gone.Load() {
		return nil, nil
	}
	if again := l.again.Load(); again != nil {
		return (*again)(), nil
	}
	return receipt(1, batchPosted(l.t, eezL1), performed(l.t, eezL1, 1, *l.terminal.Load())), nil
}

func newLanding(t *testing.T, terminal common.Hash) *landing {
	l := &landing{t: t}
	l.terminal.Store(&terminal)
	l.head.Store(1)
	return l
}

// TestComposerSettlementLeftL1: a settlement whose posting an L1 reorg
// dropped, or included again without settling, before derivation saw it
// fails, and the next slot posts again, though the cursor never moved.
func TestComposerSettlementLeftL1(t *testing.T) {
	for name, leave := range map[string]func(l *landing){
		"dropped": func(l *landing) { l.gone.Store(true) },
		"reverted": func(l *landing) {
			again := func() *l1.Receipt { return receipt(0) }
			l.again.Store(&again)
		},
		"posted without settling": func(l *landing) {
			again := func() *l1.Receipt { return receipt(1, batchPosted(t, eezL1)) }
			l.again.Store(&again)
		},
	} {
		t.Run(name, func(t *testing.T) {
			chain := newTestChain(t, 12)
			node := newLanding(t, chain.blocks[8].Hash())
			h := newHarness(t, chain, 12, node)
			h.slot(t, 8, 50, sequencer.ModeSteady)
			require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
			h.slot(t, 12, 52, sequencer.ModeSteady)
			require.Len(t, h.targets(t), 1, "the settlement holds the gate while on L1")
			leave(node)
			require.Eventually(t, func() bool {
				h.c.RecoverFailed(t.Context())
				node.head.Add(1)
				_, ok := h.resolution(8)
				return !ok
			}, 5*time.Second, time.Millisecond, "dropped")
			h.c.ComposeSyncSlot(t.Context(), h.chain.header(11), h.chain.header(12).Time, 52, sequencer.ModeSteady)
			h.c.SyncCommitted(t.Context(), h.chain.header(12))
			require.Len(t, h.targets(t), 2)
			h.requireWindow(t, 1, 1, 12, post.Target{Block: 52, Time: h.chain.header(12).Time})
		})
	}
}

// TestComposerRejudgesFromTheBlockScan: a settlement derivation has yet to
// see is judged again from the scan of its block, which shows the entries its
// triggers applied after the posting's own logs.
func TestComposerRejudgesFromTheBlockScan(t *testing.T) {
	chain := newTestChain(t, 12)
	terminal := chain.blocks[8].Hash()
	node := newLanding(t, terminal)
	h := newHarness(t, chain, 12, node)
	h.c.observer.Scan = func(context.Context, uint64) ([]l1.ScannedBatch, error) {
		h.poster.mu.Lock()
		defer h.poster.mu.Unlock()
		return []l1.ScannedBatch{{TxHash: h.poster.txs[0].Hash(), Settlement: protocol.Settlement{Applied: []protocol.AppliedEntry{{}}, FinalState: terminal}}}, nil
	}
	h.slot(t, 8, 50, sequencer.ModeSteady)
	require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
	posted := func() *l1.Receipt { return receipt(1, batchPosted(t, eezL1)) }
	node.again.Store(&posted)
	h.c.RecoverFailed(t.Context())
	r, ok := h.resolution(8)
	require.True(t, ok)
	require.Equal(t, Settled, r, "the posting's own logs show no settlement, its block's scan does")
}

// TestComposerSettlementBackInThePool: a settlement whose posting an L1 reorg
// returned to the node's pool holds the gate, its nonce taken, until it
// lands again.
func TestComposerSettlementBackInThePool(t *testing.T) {
	for name, seen := range map[string]bool{"before derivation saw it": false, "after derivation saw it": true} {
		t.Run(name, func(t *testing.T) {
			chain := newTestChain(t, 12)
			node := newLanding(t, chain.blocks[8].Hash())
			h := newHarness(t, chain, 12, node)
			h.slot(t, 8, 50, sequencer.ModeSteady)
			require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
			if seen {
				h.cursor.Store(8)
				h.c.RecoverFailed(t.Context())
				h.cursor.Store(4)
			}
			node.pooled.Store(true)
			node.gone.Store(true)
			h.c.RecoverFailed(t.Context())
			r, ok := h.resolution(8)
			require.True(t, ok)
			require.Equal(t, Pending, r)
			h.slot(t, 12, 52, sequencer.ModeSteady)
			require.Len(t, h.targets(t), 1, "the posting back in the pool holds the gate")
			node.gone.Store(false)
			require.Eventually(t, func() bool { r, _ := h.resolution(8); return r == Settled }, 5*time.Second, time.Millisecond)
		})
	}
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

// triggers is an L1 on which the held transactions in landed have receipts.
type triggers struct {
	landed map[common.Hash]bool
	failed error
}

func (l *triggers) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (l *triggers) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 1}, nil }

func (l *triggers) Receipt(_ context.Context, h common.Hash) (*l1.Receipt, error) {
	if l.failed != nil {
		return nil, l.failed
	}
	if l.landed[h] {
		return &l1.Receipt{Status: 1}, nil
	}
	return nil, nil
}

func (l *triggers) HasTransaction(_ context.Context, h common.Hash) (bool, error) {
	return l.landed[h], l.failed
}

// TestComposerChargeEvictsTheChain: a transaction out of attempts takes
// the rest of its chain in the batch with it.
func TestComposerChargeEvictsTheChain(t *testing.T) {
	h := newHarness(t, newTestChain(t, 8), 8, &triggers{landed: map[common.Hash]bool{}})
	pool := NewHeldPool()
	h.c.Recover(pool, func(context.Context, *types.Header) (*types.Header, error) { return nil, nil })
	root := held(addr(0xa), 0, Inbound, 1)
	root.Attempts = 2
	require.NoError(t, pool.Push(root, 0))
	require.NoError(t, pool.Push(held(addr(0xa), 1, Inbound, 2), 0))
	h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Terminal: h.chain.header(8), Txs: pool.PopN(2)})
	h.c.ledger.MarkFailed(8, common.Hash{0xf}, false)
	h.c.RecoverFailed(t.Context())
	require.Empty(t, pool.PopN(math.MaxInt))
}

func TestComposerRecoversHeldTransactions(t *testing.T) {
	a, b := addr(0xa), addr(0xb)
	for _, c := range []struct {
		name       string
		attempts   uint32 // of a's transaction
		skipped    bool
		landed     bool // a's transaction
		substitute error
		lookup     error
		// queued are the transactions back in the pool, with their attempts.
		queued   map[common.Hash]uint32
		replaced bool
		kept     bool // the failure stays for the next trigger
	}{
		{name: "requeued, charged", queued: map[common.Hash]uint32{{1}: 1, {2}: 0, {3}: 1}, replaced: true},
		{name: "a skipped slot is free", skipped: true, queued: map[common.Hash]uint32{{1}: 0, {2}: 0, {3}: 0}, replaced: true},
		{name: "a landed trigger is released", landed: true, queued: map[common.Hash]uint32{{2}: 0, {3}: 1}, replaced: true},
		{name: "the third failure evicts the chain", attempts: 2, queued: map[common.Hash]uint32{{3}: 1}, replaced: true},
		{name: "a height L1 settled", substitute: follower.ErrBelowSafe, queued: map[common.Hash]uint32{{2}: 0}},
		{name: "a block no longer canonical", substitute: follower.ErrNotCanonical, queued: map[common.Hash]uint32{{1}: 1, {2}: 0, {3}: 1}, replaced: true},
		{name: "a stale parent", substitute: follower.ErrStaleParent, queued: map[common.Hash]uint32{{1}: 1, {2}: 0, {3}: 1}, replaced: true},
		{name: "a substitution failing", substitute: errors.New("chain busy"), queued: map[common.Hash]uint32{{2}: 0}, kept: true},
		{name: "a lookup failing", lookup: errors.New("node down"), queued: map[common.Hash]uint32{{2}: 0}, kept: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			node := &triggers{landed: map[common.Hash]bool{}, failed: c.lookup}
			h := newHarness(t, newTestChain(t, 8), 8, node)
			pool := NewHeldPool()
			var replaced []common.Hash
			h.c.Recover(pool, func(_ context.Context, failed *types.Header) (*types.Header, error) {
				replaced = append(replaced, failed.Hash())
				return nil, c.substitute
			})
			txA := held(a, 0, Inbound, 1)
			txA.Attempts = c.attempts
			require.NoError(t, pool.Push(txA, 0))
			require.NoError(t, pool.Push(held(a, 1, Inbound, 2), 0))
			require.NoError(t, pool.Push(held(b, 0, Inbound, 3), 0))
			drained := pool.PopN(3)
			// a's second transaction did not make the batch: it waits in the pool.
			inBatch := []*Held{drained[0], drained[2]}
			pool.PushFront([]*Held{drained[1]})
			if c.landed {
				node.landed[txA.Hash] = true
			}
			h.c.ledger.Begin(Posted[*Held]{Height: 8, Post: common.Hash{0xf}, Terminal: h.chain.header(8), Txs: inBatch})
			h.c.ledger.MarkFailed(8, common.Hash{0xf}, c.skipped)

			h.c.RecoverFailed(t.Context())
			if c.replaced {
				require.Equal(t, []common.Hash{h.chain.blocks[8].Hash()}, replaced)
			}
			_, failing := h.c.ledger.Blocking(0)
			require.Equal(t, c.kept, failing)
			got := map[common.Hash]uint32{}
			for _, tx := range pool.PopN(math.MaxInt) {
				got[tx.Hash] = tx.Attempts
			}
			require.Equal(t, c.queued, got)
			if !c.kept {
				pool.mu.Lock()
				require.Len(t, pool.inFlight, len(got), "only the requeued, now drained, are reserved")
				pool.mu.Unlock()
			}
		})
	}
}
