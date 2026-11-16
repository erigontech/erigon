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
	"errors"
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
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// behind is a pool whose view of the chain stays at a block until see moves
// it; a wait for another block reports itself on waiting. Its provider waits
// for the parent's hash too, when asked; its peek, for the height alone.
type behind struct {
	mu      sync.Mutex
	seen    uint64
	hash    common.Hash
	free    bool
	moved   chan struct{}
	waiting chan uint64
}

func newBehind() *behind {
	return &behind{moved: make(chan struct{}), waiting: make(chan uint64, 16)}
}

func (b *behind) see(n uint64, hash common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen, b.hash = n, hash
	close(b.moved)
	b.moved = make(chan struct{})
}

// release ends every wait.
func (b *behind) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.free = true
	close(b.moved)
	b.moved = make(chan struct{})
}

func (b *behind) wait(ctx context.Context, n uint64, hash common.Hash) error {
	for reported := false; ; reported = true {
		b.mu.Lock()
		ready := b.free || b.seen >= n && (hash == (common.Hash{}) || b.hash == hash)
		moved := b.moved
		b.mu.Unlock()
		if ready {
			return nil
		}
		if !reported {
			b.waiting <- n
		}
		select {
		case <-moved:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *behind) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o := txnprovider.ApplyProvideOptions(opts...)
	return nil, b.wait(ctx, o.ParentBlockNum, o.ParentBlockHash)
}

func (b *behind) PeekBest(ctx context.Context, _ int, _ *txpool.TxnsRlp, onTopOf uint64) (bool, error) {
	return true, b.wait(ctx, onTopOf, common.Hash{})
}

// TestStalePoolLeavesTheFollowerFree: a build waits for the pool to see its
// parent before the follower takes it, so the follower's goroutine, which
// derivation shares, still performs another writer's command while the pool
// lags.
func TestStalePoolLeavesTheFollowerFree(t *testing.T) {
	h := newHarness(t)
	pool := newBehind()
	// Releases a build left waiting, before the follower stops.
	t.Cleanup(pool.release)
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)
	ctx := t.Context()

	// The pool has seen genesis, the parent of block 1.
	pool.see(0, h.genesis.Hash())
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	head := h.head(t)
	require.Equal(t, uint64(1), head.Number.Uint64())

	done := make(chan error, 1)
	go func() { done <- s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}) }()
	select {
	case n := <-pool.waiting:
		require.Equal(t, uint64(1), n)
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not wait for the pool")
	}
	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	require.NoError(t, h.follower.HeadTo(wctx, head.Hash()), "another writer's command waits for the pool")

	pool.see(1, head.Hash())
	require.NoError(t, <-done)
	require.Equal(t, uint64(2), h.head(t).Number.Uint64())
}

// TestBuildWaitsForThePoolOnTheParent: with the head moved back under the
// pool's view, the build waits for the pool to see the parent, though the
// pool has seen a block of its height.
func TestBuildWaitsForThePoolOnTheParent(t *testing.T) {
	h := newHarness(t)
	pool := newBehind()
	t.Cleanup(pool.release)
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)
	ctx := t.Context()

	pool.see(0, h.genesis.Hash())
	require.NoError(t, s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}))
	first := h.head(t)
	require.Equal(t, uint64(1), first.Number.Uint64())
	pool.see(1, first.Hash())
	require.NoError(t, h.follower.HeadTo(ctx, h.genesis.Hash()))

	done := make(chan error, 1)
	go func() { done <- s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}) }()
	select {
	case n := <-pool.waiting:
		require.Equal(t, uint64(0), n)
	case err := <-done:
		t.Fatalf("built on the pool's view of the block moved off: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not wait for the pool")
	}
	pool.see(0, h.genesis.Hash())
	require.NoError(t, <-done)
	require.Equal(t, uint64(1), h.head(t).Number.Uint64())
}

// pins is a witness capture that counts its open pins and keeps the head each
// was taken at and the blocks it recorded.
type pins struct {
	head func() (*types.Header, error)

	mu       sync.Mutex
	open     int
	at       []common.Hash
	recorded []common.Hash
}

func (p *pins) Pin(context.Context) (interface{ Close() }, error) {
	head, err := p.head()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.open++
	p.at = append(p.at, head.Hash())
	return closer{p}, nil
}

func (p *pins) Record(_ context.Context, _ interface{ Close() }, h *types.Header) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recorded = append(p.recorded, h.Hash())
	return nil
}

func (p *pins) opened() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.open
}

type closer struct{ p *pins }

func (c closer) Close() {
	c.p.mu.Lock()
	defer c.p.mu.Unlock()
	c.p.open--
}

var errHeld = errors.New("held")

// held holds a build, and the follower's goroutine with it, until released,
// then ends it with err: no block, or an empty one.
type held struct {
	entered chan struct{}
	release <-chan struct{}
	err     error
}

func (h held) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	close(h.entered)
	select {
	case <-h.release:
	case <-ctx.Done():
	}
	return nil, h.err
}

// queuing is the follower, telling queued when a build is handed to it.
type queuing struct {
	*follower.Follower
	queued chan struct{}
}

func (q queuing) Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, hook follower.CommitHook) (*types.BlockWithReceipts, error) {
	q.queued <- struct{}{}
	return q.Follower.Produce(ctx, in, txs, hook)
}

// wait waits for a build handed to q.
func (q queuing) wait(t *testing.T) {
	t.Helper()
	select {
	case <-q.queued:
	case <-time.After(10 * time.Second):
		t.Fatal("the trigger handed no build to the follower")
	}
}

// holdFollower has the follower build block 1 on genesis from a held
// provider ending with err, and returns the build's result and its release.
func holdFollower(t *testing.T, h *harness, err error) (<-chan error, func()) {
	t.Helper()
	release := make(chan struct{})
	free := sync.OnceFunc(func() { close(release) })
	t.Cleanup(free)
	busy := held{entered: make(chan struct{}), release: release, err: err}
	l2 := derivertest.Config().L2.(*config.L2Config)
	in := deriver.DeriveInputs(l2, h.genesis, &da.Span{Beneficiaries: []common.Address{{}}, ExtraData: [][]byte{{}}}, 0)
	other := make(chan error, 1)
	go func() {
		_, err := h.follower.Produce(t.Context(), in, busy, nil)
		other <- err
	}()
	select {
	case <-busy.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the follower did not take the other build")
	}
	return other, free
}

// TestWitnessPinWaitsForTheCommand: a block's witness pin, a read transaction
// on the chain, is taken once the follower takes the block's build, at its
// parent, not while the build waits behind the follower's other work.
func TestWitnessPinWaitsForTheCommand(t *testing.T) {
	h := newHarness(t)
	w := &pins{head: func() (*types.Header, error) { return h.follower.Head(context.Background()) }}
	q := queuing{Follower: h.follower, queued: make(chan struct{}, 1)}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Witnesses: w}, q, h.pool, log.New())
	require.NoError(t, err)
	ctx := t.Context()

	other, free := holdFollower(t, h, errHeld)
	done := make(chan error, 1)
	go func() { done <- s.Handle(ctx, sequencer.LiveTick{SyncHeight: 12}) }()
	q.wait(t)
	require.Never(t, func() bool { return w.opened() > 0 }, 300*time.Millisecond, 10*time.Millisecond, "a pin is open while the build waits")

	free()
	require.ErrorIs(t, <-other, errHeld)
	require.NoError(t, <-done)
	head := h.head(t)
	require.Equal(t, uint64(1), head.Number.Uint64())
	require.Zero(t, w.opened())
	require.Equal(t, []common.Hash{h.genesis.Hash()}, w.at, "pinned at the block's parent")
	require.Equal(t, []common.Hash{head.Hash()}, w.recorded)
}

// TestWitnessPinNeedsTheParent: a build whose parent another writer replaced
// while it waited takes no pin.
func TestWitnessPinNeedsTheParent(t *testing.T) {
	h := newHarness(t)
	w := &pins{head: func() (*types.Header, error) { return h.follower.Head(context.Background()) }}
	q := queuing{Follower: h.follower, queued: make(chan struct{}, 1)}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet(), Witnesses: w}, q, h.pool, log.New())
	require.NoError(t, err)

	other, free := holdFollower(t, h, nil)
	done := make(chan error, 1)
	go func() { done <- s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}) }()
	// The trigger read genesis as the head.
	q.wait(t)
	free()
	require.NoError(t, <-other)
	require.NoError(t, <-done, "a moved head ends the trigger quietly")
	require.Equal(t, uint64(1), h.head(t).Number.Uint64())
	require.Empty(t, w.at)
	require.Empty(t, w.recorded)
}
