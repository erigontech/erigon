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

package follower_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

var (
	alice = common.HexToAddress("0xa11ce")
	bob   = common.HexToAddress("0xb0b")
)

var testConfig = follower.Config{
	Poll:          5 * time.Millisecond,
	FinalityEvery: 1,
	ReorgDepth:    l1.DefaultReorgDepth,
	Retry:         5 * time.Millisecond,
	MaxRetry:      20 * time.Millisecond,
	BootAttempts:  3,
}

// countingChain counts the blocks derivation builds.
type countingChain struct {
	deriver.Chain
	mu    sync.Mutex
	built int
}

func (c *countingChain) Extend(ctx context.Context, parent *types.Header, in deriver.HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	c.mu.Lock()
	c.built++
	c.mu.Unlock()
	return c.Chain.Extend(ctx, parent, in, txs, safe, finalized)
}

func (c *countingChain) Built() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.built
}

type node struct {
	m        *execmoduletester.ExecModuleTester
	chain    *countingChain
	follower *follower.Follower
	dir      string
}

func newNode(t *testing.T, reader l1.ChainReader) *node {
	t.Helper()
	m, chain := derivertest.NewL2(t)
	n := &node{m: m, dir: t.TempDir()}
	return n.restart(t, reader, chain)
}

// restart returns a node with a fresh follower on chain, the checkpoint
// directory kept.
func (n *node) restart(t *testing.T, reader l1.ChainReader, chain deriver.Chain) *node {
	t.Helper()
	cfg := testConfig
	cfg.Dir = n.dir
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	counting := &countingChain{Chain: chain}
	f, err := follower.New(reader, counting, derivertest.Config(), cfg, logger)
	require.NoError(t, err)
	return &node{m: n.m, chain: counting, follower: f, dir: n.dir}
}

// requireBlocks waits for the node's chain to hold want.
func (n *node) requireBlocks(t *testing.T, want ...*types.Block) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, b := range want {
			h, ok, err := n.chain.CanonicalHash(context.Background(), b.NumberU64())
			if err != nil || !ok || h != b.Hash() {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond)
}

func (n *node) run(t *testing.T) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- n.follower.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, errc
}

func transfers(c *derivertest.Composer, n int) [][]byte {
	txs := make([][]byte, n)
	for i := range txs {
		txs[i] = c.Transfer(bob, uint64(i+1))
	}
	return txs
}

// TestFollow boots from the registry's deploy block, follows new
// settlements and L1 finality, and stops with its context.
func TestFollow(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 2)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}},
	)
	l1First := c.Post(derivertest.Posting{Batch: first}).Header.Number

	n := newNode(t, c.L1)
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, first.Blocks...)
	status := n.follower.Status()
	require.Equal(t, l1First, status.L1Scanned)
	require.Equal(t, uint64(2), status.Cursor)
	require.Equal(t, uint64(2), status.Safe)
	require.Equal(t, l1First, status.Last.L1Block)

	cancel, done := n.run(t)
	second := c.Compose([]da.Action{{
		SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
		Data: []byte{}, Success: true, ReturnData: []byte{},
	}}, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 1)}})
	c.Post(derivertest.Posting{Batch: second})
	n.requireBlocks(t, second.Blocks...)

	require.NoError(t, c.L1.Finalize(l1First))
	require.Eventually(t, func() bool {
		fc, err := n.m.ExecModule.GetForkChoice(context.Background())
		return err == nil && fc.FinalizedHash == first.Blocks[1].Hash() && fc.SafeHash == second.Blocks[0].Hash()
	}, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		s := n.follower.Status()
		return s.Finalized == 2 && s.Safe == 3 && s.L1Finalized == l1First
	}, 10*time.Second, 5*time.Millisecond)
	r, ok := n.follower.Deriver().Settlement(3)
	require.True(t, ok)
	require.Equal(t, l1First+1, r.L1Block)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// tampered composes a block whose settlement the DA payload contradicts.
func tampered(c *derivertest.Composer) *derivertest.Batch {
	b := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 9)}})
	b.Payload.Span.Beneficiaries[0] = bob
	b.Batch.CallData = da.Encode((&da.Payload{RollupID: 1, Operations: b.Payload.Span.Encode()}).Messages()...)
	return b
}

// TestHaltOnDivergence is T-DRV-5 on canonical L1: a settled block the
// follower derives otherwise halts it.
func TestHaltOnDivergence(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: first})

	n := newNode(t, c.L1)
	_, done := n.run(t)
	n.requireBlocks(t, first.Blocks...)
	c.Post(derivertest.Posting{Batch: tampered(c)})

	select {
	case err := <-done:
		require.ErrorIs(t, err, deriver.ErrDiverged)
	case <-time.After(10 * time.Second):
		t.Fatal("follower did not halt")
	}

	// A boot that meets the divergence halts too.
	again := n.restart(t, c.L1, n.chain.Chain)
	require.ErrorIs(t, again.follower.Boot(t.Context()), deriver.ErrDiverged)
}

// staleLogs serves logs and transactions from a stale copy of the chain
// once, as a lagging L1 node behind a load balancer would, and headers from
// the live chain.
type staleLogs struct {
	*recording.MemoryChain
	stale *recording.MemoryChain
	mu    sync.Mutex
	used  bool
}

func (r *staleLogs) source(to uint64, topics [][]common.Hash) *recording.MemoryChain {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used || len(topics) != 1 || len(topics[0]) != 4 {
		return r.MemoryChain
	}
	if head, _ := r.stale.Head(context.Background()); head == nil || to < head.Number {
		return r.MemoryChain
	}
	r.used = true
	return r.stale
}

func (r *staleLogs) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	return r.source(to, topics).Logs(ctx, from, to, address, topics)
}

func (r *staleLogs) TransactionByBlockHashAndIndex(ctx context.Context, hash common.Hash, index uint64) (types.Transaction, error) {
	if tx, err := r.stale.TransactionByBlockHashAndIndex(ctx, hash, index); err == nil {
		return tx, nil
	}
	return r.MemoryChain.TransactionByBlockHashAndIndex(ctx, hash, index)
}

// snapshot copies the canonical blocks of c.
func snapshot(t *testing.T, c *recording.MemoryChain) *recording.MemoryChain {
	t.Helper()
	blocks := c.Canonical()
	for _, b := range blocks {
		b.Recorded = true
	}
	id, err := c.ChainID(context.Background())
	require.NoError(t, err)
	cp, err := recording.NewMemoryChain(id, blocks[0])
	require.NoError(t, err)
	for _, b := range blocks[1:] {
		require.NoError(t, cp.Append(b))
	}
	return cp
}

// TestRetryOnReorgedDivergence is T-DRV-5 on reorged L1: a divergence whose
// L1 block is no longer canonical is retried, and the replacement derived.
func TestRetryOnReorgedDivergence(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	l1First := c.Post(derivertest.Posting{Batch: first}).Header.Number
	c.Post(derivertest.Posting{Batch: tampered(c)})
	stale := snapshot(t, c.L1)

	c.ReorgL1(l1First)
	c.Rewind(first.Blocks[0].Header())
	replacement := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 2)}})
	c.Post(derivertest.Posting{Batch: replacement})

	reader := &staleLogs{MemoryChain: c.L1, stale: stale}
	n := newNode(t, reader)
	require.NoError(t, n.follower.Boot(t.Context()))
	require.True(t, reader.used, "the stale logs were read")
	n.requireBlocks(t, append(first.Blocks, replacement.Blocks...)...)
}

// TestResume is T-DRV-4 on restart: a usable checkpoint resumes derivation
// where it stopped; an unusable one falls back to the newest settlement the
// local chain holds.
func TestResume(t *testing.T) {
	c := derivertest.NewComposer(t)
	var all []*types.Block
	for range 2 {
		b := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
		c.Post(derivertest.Posting{Batch: b})
		all = append(all, b.Blocks...)
	}
	n := newNode(t, c.L1)
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, all...)
	require.Equal(t, 2, n.chain.Built())
	cp, err := deriver.LoadCheckpoint(n.dir)
	require.NoError(t, err)
	require.NotNil(t, cp)

	t.Run("checkpoint", func(t *testing.T) {
		again := n.restart(t, c.L1, n.chain.Chain)
		require.NoError(t, again.follower.Boot(t.Context()))
		require.Zero(t, again.chain.Built())
		last, _ := again.follower.Deriver().Last()
		require.Equal(t, cp.TxHash, last.TxHash)
		require.Equal(t, uint64(2), again.follower.Deriver().Safe())
	})
	t.Run("resume point", func(t *testing.T) {
		stale := *cp
		stale.L1BlockHash = common.Hash{1}
		require.NoError(t, stale.Save(n.dir))
		again := n.restart(t, c.L1, n.chain.Chain)
		require.NoError(t, again.follower.Boot(t.Context()))
		require.Zero(t, again.chain.Built())
		last, _ := again.follower.Deriver().Last()
		require.Equal(t, cp.TxHash, last.TxHash)
		require.Equal(t, uint64(2), again.follower.Deriver().Cursor())
	})
	t.Run("malformed checkpoint", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(n.dir, deriver.CheckpointFile), []byte("garbage"), 0o644))
		again := n.restart(t, c.L1, n.chain.Chain)
		require.NoError(t, again.follower.Boot(t.Context()))
		require.Zero(t, again.chain.Built())
		require.Equal(t, uint64(2), again.follower.Deriver().Cursor())
	})
}

// TestWrongL1 refuses an L1 source on another chain.
func TestWrongL1(t *testing.T) {
	other, err := recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Number: 12, Hash: common.Hash{1}}})
	require.NoError(t, err)
	n := newNode(t, other)
	require.ErrorIs(t, n.follower.Boot(t.Context()), follower.ErrL1ChainID)
}

// TestService runs the follower as a node service over a recording of L1,
// and stops it with the node.
func TestService(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 2)})
	c.Post(derivertest.Posting{Batch: first})
	dir := t.TempDir()
	require.NoError(t, recording.Write(dir, c.L1))
	rec, err := recording.Load(dir)
	require.NoError(t, err)

	n := newNode(t, rec)
	halted := make(chan error, 1)
	s := follower.NewService(n.follower, func(err error) { halted <- err })
	require.NoError(t, s.Start())
	n.requireBlocks(t, first.Blocks...)
	require.NoError(t, s.Stop())
	require.NoError(t, s.Err())
	require.Empty(t, halted)

	// A follower that halts reports why.
	c.Post(derivertest.Posting{Batch: tampered(c)})
	require.NoError(t, recording.Write(dir, c.L1))
	rec, err = recording.Load(dir)
	require.NoError(t, err)
	again := n.restart(t, rec, n.chain.Chain)
	s = follower.NewService(again.follower, func(err error) { halted <- err })
	require.NoError(t, s.Start())
	select {
	case err := <-halted:
		require.ErrorIs(t, err, deriver.ErrDiverged)
	case <-time.After(10 * time.Second):
		t.Fatal("follower did not halt")
	}
	require.NoError(t, s.Stop())
	require.ErrorIs(t, s.Err(), deriver.ErrDiverged)
}
