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
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

type branchReadRecorder struct {
	*MockState
	read map[string]bool
}

func (r *branchReadRecorder) Branch(prefix []byte) ([]byte, kv.Step, error) {
	data, step, err := r.MockState.Branch(prefix)
	if len(data) > 0 {
		r.read[string(prefix)] = true
	}
	return data, step, err
}

func TestWarmupKeyReadsEveryBranchOnThePath(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	ub := NewUpdateBuilder()
	for i := range 64 {
		addr := fmt.Sprintf("%040x", i+1)
		ub.Balance(addr, uint64(i+1))
		if i%8 == 0 {
			for s := range 300 {
				ub.Storage(addr, fmt.Sprintf("%064x", s+1), fmt.Sprintf("%02x", s%250+1))
			}
		}
	}
	plainKeys, updates := ub.Build()
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	_, err := hph.Process(t.Context(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	w := &Warmuper{maxDepth: WarmupMaxDepth}
	storageBranches, extensionHops := 0, 0
	for _, pk := range plainKeys {
		hk := KeyToHexNibbleHash(pk)
		want := map[string]bool{}
		var depths []int
		for prefix := range ms.cm {
			if nib := nibbles.CompactToHex([]byte(prefix)); len(nib) < len(hk) && bytes.HasPrefix(hk, nib) {
				want[prefix] = true
				depths = append(depths, len(nib))
				if len(nib) >= 64 {
					storageBranches++
				}
			}
		}
		slices.Sort(depths)
		for i := 1; i < len(depths); i++ {
			if depths[i] > depths[i-1]+1 && (depths[i-1] >= 64 || depths[i] < 64) {
				extensionHops++
			}
		}
		rec := &branchReadRecorder{MockState: ms, read: map[string]bool{}}
		w.warmupKey(rec, hk, 0)
		require.Equal(t, want, rec.read, "plain key %x", pk)
	}
	require.NotZero(t, storageBranches, "fixture must carry storage-plane branches")
	require.NotZero(t, extensionHops, "fixture must carry extension nodes between branches")
}
