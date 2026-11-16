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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// behind is a pool whose view of the chain stays at a block until see moves
// it; a wait for a later block reports itself on waiting.
type behind struct {
	mu      sync.Mutex
	seen    uint64
	moved   chan struct{}
	waiting chan uint64
}

func newBehind() *behind {
	return &behind{moved: make(chan struct{}), waiting: make(chan uint64, 16)}
}

func (b *behind) see(n uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen = n
	close(b.moved)
	b.moved = make(chan struct{})
}

func (b *behind) wait(ctx context.Context, n uint64) error {
	for reported := false; ; reported = true {
		b.mu.Lock()
		seen, moved := b.seen, b.moved
		b.mu.Unlock()
		if seen >= n {
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
	return nil, b.wait(ctx, txnprovider.ApplyProvideOptions(opts...).ParentBlockNum)
}

func (b *behind) PeekBest(ctx context.Context, _ int, _ *txpool.TxnsRlp, onTopOf uint64) (bool, error) {
	return true, b.wait(ctx, onTopOf)
}

// TestStalePoolLeavesTheFollowerFree: a build waits for the pool to see its
// parent before the follower takes it, so the follower's goroutine, which
// derivation shares, still performs another writer's command while the pool
// lags.
func TestStalePoolLeavesTheFollowerFree(t *testing.T) {
	h := newHarness(t)
	pool := newBehind()
	// Releases a build left waiting, before the follower stops.
	t.Cleanup(func() { pool.see(math.MaxUint64) })
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, h.follower, pool, log.New())
	require.NoError(t, err)
	ctx := t.Context()

	// The pool has seen genesis, the parent of block 1.
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

	pool.see(1)
	require.NoError(t, <-done)
	require.Equal(t, uint64(2), h.head(t).Number.Uint64())
}
