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
package l1_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
)

func hashOf(number uint64, branch byte) common.Hash {
	var b [9]byte
	binary.BigEndian.PutUint64(b[:8], number)
	b[8] = branch
	return crypto.Keccak256Hash(b[:])
}

// linear returns a chain of blocks 0..head on branch 0.
func linear(t *testing.T, head uint64) *recording.MemoryChain {
	t.Helper()
	c, err := recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Hash: hashOf(0, 0)}})
	require.NoError(t, err)
	for n := uint64(1); n <= head; n++ {
		require.NoError(t, c.Append(&recording.Block{Header: l1.Header{Number: n, Hash: hashOf(n, 0), ParentHash: hashOf(n-1, 0)}}))
	}
	return c
}

// extend appends blocks up to head on branch, from the current head.
func extend(t *testing.T, c *recording.MemoryChain, head uint64, branch byte) {
	t.Helper()
	tip, err := c.Head(context.Background())
	require.NoError(t, err)
	parent := tip.Hash
	for n := tip.Number + 1; n <= head; n++ {
		require.NoError(t, c.Append(&recording.Block{Header: l1.Header{Number: n, Hash: hashOf(n, branch), ParentHash: parent}}))
		parent = hashOf(n, branch)
	}
}

// fork replaces the blocks above ancestor with a branch up to head.
func fork(t *testing.T, c *recording.MemoryChain, ancestor, head uint64, branch byte) {
	t.Helper()
	at, err := c.HeaderByNumber(context.Background(), ancestor)
	require.NoError(t, err)
	var blocks []*recording.Block
	parent := at.Hash
	for n := ancestor + 1; n <= head; n++ {
		blocks = append(blocks, &recording.Block{Header: l1.Header{Number: n, Hash: hashOf(n, branch), ParentHash: parent}})
		parent = hashOf(n, branch)
	}
	require.NoError(t, c.Reorg(ancestor, blocks...))
}

func seeded(t *testing.T, c *recording.MemoryChain, at uint64, depth int) *l1.Watcher {
	t.Helper()
	w := l1.NewWatcher(c, depth)
	require.NoError(t, w.Seed(context.Background(), at))
	return w
}

func header(n uint64, branch byte) l1.Header {
	h := l1.Header{Number: n, Hash: hashOf(n, branch)}
	if n > 0 {
		h.ParentHash = hashOf(n-1, branch)
	}
	return h
}

func TestWatcherFollowsTheHead(t *testing.T) {
	ctx := context.Background()
	c := linear(t, 5)
	w := seeded(t, c, 5, 4)

	events, err := w.Poll(ctx)
	require.NoError(t, err)
	require.Empty(t, events)

	extend(t, c, 6, 0)
	events, err = w.Poll(ctx)
	require.NoError(t, err)
	h6, _ := c.HeaderByNumber(ctx, 6)
	require.Equal(t, []l1.Event{l1.NewHead{Head: *h6}}, events)

	extend(t, c, 9, 0)
	events, err = w.Poll(ctx)
	require.NoError(t, err)
	h9, _ := c.HeaderByNumber(ctx, 9)
	require.Equal(t, []l1.Event{l1.NewHead{Head: *h9}}, events)
	require.Equal(t, *h9, w.Tip())
}

func TestWatcherReorg(t *testing.T) {
	ctx := context.Background()
	c := linear(t, 9)
	w := seeded(t, c, 9, 4)
	old := w.Tip()

	fork(t, c, 7, 10, 1)
	events, err := w.Poll(ctx)
	require.NoError(t, err)
	h7, _ := c.HeaderByNumber(ctx, 7)
	h10, _ := c.HeaderByNumber(ctx, 10)
	require.Equal(t, []l1.Event{l1.Reorg{Ancestor: *h7, OldHead: old}, l1.NewHead{Head: *h10}}, events)

	// A shorter replacement still finds its ancestor.
	fork(t, c, 8, 9, 2)
	events, err = w.Poll(ctx)
	require.NoError(t, err)
	h8, _ := c.HeaderByNumber(ctx, 8)
	h9, _ := c.HeaderByNumber(ctx, 9)
	require.Equal(t, []l1.Event{l1.Reorg{Ancestor: *h8, OldHead: *h10}, l1.NewHead{Head: *h9}}, events)
}

func TestWatcherReorgTooDeep(t *testing.T) {
	c := linear(t, 9)
	w := seeded(t, c, 9, 4)
	fork(t, c, 4, 10, 1)
	_, err := w.Poll(context.Background())
	require.ErrorIs(t, err, l1.ErrReorgTooDeep)
	require.Equal(t, header(9, 0), w.Tip(), "a failed poll commits nothing")
}

func TestWatcherJumpsWideGaps(t *testing.T) {
	ctx := context.Background()
	c := linear(t, 9)
	w := seeded(t, c, 9, 4)
	extend(t, c, 20, 0)
	events, err := w.Poll(ctx)
	require.NoError(t, err)
	h20, _ := c.HeaderByNumber(ctx, 20)
	require.Equal(t, []l1.Event{l1.NewHead{Head: *h20}}, events)

	// The ring was refilled behind the new tip, so a shallow reorg there is found.
	fork(t, c, 18, 21, 1)
	events, err = w.Poll(ctx)
	require.NoError(t, err)
	require.IsType(t, l1.Reorg{}, events[0])
	require.Equal(t, uint64(18), events[0].(l1.Reorg).Ancestor.Number)
}

func TestWatcherJumpAcrossAReorg(t *testing.T) {
	ctx := context.Background()
	c := linear(t, 9)
	w := seeded(t, c, 9, 4)
	fork(t, c, 7, 20, 1)
	events, err := w.Poll(ctx)
	require.NoError(t, err)
	h7, _ := c.HeaderByNumber(ctx, 7)
	h20, _ := c.HeaderByNumber(ctx, 20)
	require.Equal(t, []l1.Event{l1.Reorg{Ancestor: *h7, OldHead: header(9, 0)}, l1.NewHead{Head: *h20}}, events)

	c = linear(t, 9)
	w = seeded(t, c, 9, 4)
	fork(t, c, 3, 20, 1)
	_, err = w.Poll(ctx)
	require.ErrorIs(t, err, l1.ErrReorgTooDeep)
}

func TestWatcherFinality(t *testing.T) {
	ctx := context.Background()
	c := linear(t, 9)
	w := seeded(t, c, 9, 4)

	events, err := w.PollFinalized(ctx)
	require.NoError(t, err)
	require.Empty(t, events)

	require.NoError(t, c.Finalize(5))
	events, err = w.PollFinalized(ctx)
	require.NoError(t, err)
	require.Equal(t, []l1.Event{l1.Finalized{Head: header(5, 0)}}, events)
	events, err = w.PollFinalized(ctx)
	require.NoError(t, err)
	require.Empty(t, events)

	require.NoError(t, c.Finalize(7))
	events, err = w.PollFinalized(ctx)
	require.NoError(t, err)
	require.Equal(t, []l1.Event{l1.Finalized{Head: header(7, 0)}}, events)

	require.NoError(t, c.Finalize(6))
	_, err = w.PollFinalized(ctx)
	require.ErrorIs(t, err, l1.ErrFinalityViolation)
}

func TestWatcherRun(t *testing.T) {
	c := linear(t, 3)
	w := seeded(t, c, 3, 4)
	require.NoError(t, c.Finalize(0))
	extend(t, c, 4, 0)

	ctx := t.Context()
	out := make(chan l1.Event, 8)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, time.Millisecond, 1, out) }()
	require.Equal(t, l1.NewHead{Head: header(4, 0)}, <-out)
	require.Equal(t, l1.Finalized{Head: header(0, 0)}, <-out)

	fork(t, c, 0, 5, 1)
	require.ErrorIs(t, <-done, l1.ErrReorgTooDeep)
}
