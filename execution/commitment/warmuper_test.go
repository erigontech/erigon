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

package commitment

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestWarmuperFactoryMustNotOutliveCloseAndWait(t *testing.T) {
	t.Parallel()
	factoryEntered := make(chan struct{})
	release := make(chan struct{})
	readBack := make(chan int, 1)
	var callerOwned int
	factory := func(ctx context.Context) (PatriciaContext, func()) {
		close(factoryEntered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		readBack <- callerOwned
		return nil, nil
	}
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: 1,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()
	<-factoryEntered

	done := make(chan struct{})
	go func() {
		w.CloseAndWait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("CloseAndWait hung")
	}

	close(release)
	callerOwned = 1
	<-readBack
}

func TestWarmuperCloseAndWaitWithBlockedCtxFactory(t *testing.T) {
	t.Parallel()
	cleaned := make(chan struct{})
	factory := func(ctx context.Context) (PatriciaContext, func()) {
		<-ctx.Done()
		return nil, func() { close(cleaned) }
	}
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: 1,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()

	done := make(chan struct{})
	go func() {
		w.CloseAndWait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("CloseAndWait hung on a ctxFactory blocked until cancellation")
	}

	select {
	case <-cleaned:
	default:
		t.Fatal("factory cleanup did not run before CloseAndWait returned")
	}
}

func TestWarmuperNilFactoryResultUnblocksProducers(t *testing.T) {
	t.Parallel()
	factory := func(ctx context.Context) (PatriciaContext, func()) {
		return nil, nil
	}
	const numWorkers = 2
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: numWorkers,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()

	done := make(chan struct{})
	go func() {
		defer close(done)
		key := make([]byte, 32)
		for i := range numWorkers * 64 * 3 {
			w.WarmKey(key, 0, uint64(i))
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WarmKey blocked: workers exited on a nil factory result without cancelling the group")
	}
}

// DrainPending must return once the buffer it owns is empty, whether or not
// Close ran first — never spin.
func TestDrainPendingAfterCloseReturnsPromptly(t *testing.T) {
	t.Parallel()
	factory := func(ctx context.Context) (PatriciaContext, func()) {
		<-ctx.Done()
		return nil, nil
	}
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: 1,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()

	key := make([]byte, 32)
	const pending = 5
	for i := range pending {
		w.WarmKey(key, 0, uint64(i))
	}
	for i := range pending {
		if got := w.outstanding[uint64(i)%arenaRingSize].Load(); got == 0 {
			t.Fatalf("gen %d not recorded as outstanding before Close", i)
		}
	}

	w.Close()

	done := make(chan struct{})
	go func() {
		w.DrainPending()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DrainPending spun after Close instead of returning")
	}

	for i := range pending {
		if got := w.outstanding[uint64(i)%arenaRingSize].Load(); got != 0 {
			t.Fatalf("gen %d outstanding = %d, want 0 after DrainPending", i, got)
		}
	}
}

// A WarmKey send that already passed the closed check must observe
// cancellation safely, never panic. The race window is a few instructions
// wide, so this hammers many probes against one Close over many rounds.
func TestWarmKeyCloseRaceDoesNotPanic(t *testing.T) {
	t.Parallel()
	const rounds = 150
	const numWorkers = 4
	const numProbes = 500

	for round := range rounds {
		factory := func(ctx context.Context) (PatriciaContext, func()) {
			<-ctx.Done()
			return nil, nil
		}
		w := NewWarmuper(context.Background(), WarmupConfig{
			Enabled:    true,
			CtxFactory: factory,
			NumWorkers: numWorkers,
			MaxDepth:   WarmupMaxDepth,
		})
		w.Start()

		key := make([]byte, 32)
		var wg sync.WaitGroup
		ready := make(chan struct{})
		panics := make(chan any, numProbes)
		for i := range numProbes {
			wg.Add(1)
			gen := uint64(i)
			go func() {
				defer wg.Done()
				<-ready
				defer func() {
					if r := recover(); r != nil {
						panics <- r
					}
				}()
				w.WarmKey(key, 0, gen)
			}()
		}
		close(ready) // release every probe at once so some land mid-WarmKey when Close runs
		w.Close()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: probe WarmKey calls did not return after Close", round)
		}

		close(panics)
		for r := range panics {
			t.Fatalf("round %d: WarmKey panicked racing Close: %v", round, r)
		}
	}
}

// Close must leave w.work open — closing it is what made a concurrent WarmKey send panic
// and DrainPending spin. A receive on a closed channel is immediately ready with ok=false.
func TestCloseLeavesWorkChannelOpen(t *testing.T) {
	t.Parallel()
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: func(ctx context.Context) (PatriciaContext, func()) { <-ctx.Done(); return nil, nil },
		NumWorkers: 2,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()
	w.Close()

	select {
	case _, ok := <-w.work:
		if !ok {
			t.Fatal("Close must not close w.work")
		}
	default:
	}
}

// countingBranchCtx records which read path the warmuper took. Branch models the
// owning read (it copies); branchBorrowed hands back the source slice itself.
type countingBranchCtx struct {
	src      []byte
	owned    atomic.Int64
	borrowed atomic.Int64
}

func (c *countingBranchCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	c.owned.Add(1)
	return bytes.Clone(c.src), 0, nil
}

func (c *countingBranchCtx) BranchBorrowed(prefix []byte) ([]byte, kv.Step, error) {
	c.borrowed.Add(1)
	return c.src, 0, nil
}

func (c *countingBranchCtx) PutBranch(prefix, data, prevData []byte) error { return nil }
func (c *countingBranchCtx) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (c *countingBranchCtx) Storage(plainKey []byte) (*Update, error)      { return nil, nil }

// TestWarmuperBorrowsBranchBytes pins the warmuper on the non-copying read. It
// parses each branch and descends before its next read on the same context, so it
// never needs to own the bytes -- and it issues several times more branch reads
// than the fold does, so an owning read there is the bulk of the copying.
func TestWarmuperBorrowsBranchBytes(t *testing.T) {
	t.Parallel()
	// touchMap, afterMap with nibble 0 set, then one cell with no fields.
	ctx := &countingBranchCtx{src: []byte{0x00, 0x01, 0x00, 0x01, 0x00}}
	w := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: func(context.Context) (PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,
		MaxDepth:   WarmupMaxDepth,
	})
	w.Start()
	w.WarmKey([]byte{0, 0, 0, 0}, 0, 0)
	require.NoError(t, w.WaitBufferFree(0))
	w.CloseAndWait()

	require.Positive(t, ctx.borrowed.Load(), "warmup read branches through the copying path")
	require.Zero(t, ctx.owned.Load(), "warmup still copies branch bytes it drops immediately")
}
