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
	"context"
	"encoding/hex"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type warmupRecordContext struct {
	noopPatriciaContext
	record []byte
}

func (c *warmupRecordContext) Branch([]byte) ([]byte, kv.Step, error) {
	return c.record, 0, nil
}

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
		Key:        HexPatriciaWarmupKey,
		Step:       HexPatriciaWarmupStep,
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
		Key:        HexPatriciaWarmupKey,
		Step:       HexPatriciaWarmupStep,
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
		Key:        HexPatriciaWarmupKey,
		Step:       HexPatriciaWarmupStep,
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
		Key:        HexPatriciaWarmupKey,
		Step:       HexPatriciaWarmupStep,
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
			Key:        HexPatriciaWarmupKey,
			Step:       HexPatriciaWarmupStep,
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
		Key:        HexPatriciaWarmupKey,
		Step:       HexPatriciaWarmupStep,
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

func TestWarmupHPHKeyMatchesCompactEncoding(t *testing.T) {
	hashedKey := make([]byte, 64)
	for i := range hashedKey {
		hashedKey[i] = byte((i*7 + 3) & 0x0f)
	}

	for depth := range 65 {
		expected := nibbles.HexToCompact(hashedKey[:depth])
		got := HexPatriciaWarmupKey(hashedKey, depth, make([]byte, 0, maxCompactKeyLen))
		require.Equalf(t, expected, got, "depth=%d parity=%d", depth, depth&1)
	}
}

func TestWarmupHPHStepMatchesDescentDecisions(t *testing.T) {
	childRecord := func(nibble int, fieldBits byte, suffix ...byte) []byte {
		bitmap := uint16(1) << nibble
		record := []byte{0, 0, byte(bitmap >> 8), byte(bitmap), fieldBits}
		return append(record, suffix...)
	}

	withSibling := func(nibble int, fieldBits byte, suffix ...byte) []byte {
		bitmap := uint16(1) | uint16(1)<<nibble
		record := []byte{0, 0, byte(bitmap >> 8), byte(bitmap), byte(fieldHash), 0, fieldBits}
		return append(record, suffix...)
	}

	for _, tc := range []struct {
		name      string
		record    []byte
		hashedKey []byte
		depth     int
		next      int
		stop      bool
	}{
		{name: "child-present", record: withSibling(2, byte(fieldHash), 0), hashedKey: []byte{2}, depth: 0, next: 1},
		{name: "child-absent", record: childRecord(1, byte(fieldHash), 0), hashedKey: []byte{2}, depth: 0, stop: true},
		{name: "leaf-terminator", record: childRecord(2, byte(fieldAccountAddr)), hashedKey: []byte{2}, depth: 0, stop: true},
		{name: "extension-advance", record: childRecord(2, byte(fieldExtension), 3), hashedKey: []byte{2}, depth: 0, next: 3},
		{name: "truncated", record: []byte{0, 0, 0}, hashedKey: []byte{2}, depth: 0, stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, stop := HexPatriciaWarmupStep(tc.record, tc.hashedKey, tc.depth)
			require.Equal(t, tc.next, next)
			require.Equal(t, tc.stop, stop)
		})
	}
}

func TestWarmupHPHStepUsesExtensionLength(t *testing.T) {
	bitmap := uint16(1) << 2
	record := []byte{0, 0, byte(bitmap >> 8), byte(bitmap), byte(fieldExtension), 7}

	next, stop := HexPatriciaWarmupStep(record, []byte{0, 0, 0, 0, 2}, 4)
	require.False(t, stop)
	require.Equal(t, 11, next)
}

func TestWarmupKeyStopsOnBackwardsStep(t *testing.T) {
	record, err := hex.DecodeString("302f033cd3c1afb6afd59e92bbf69401b89a7f")
	require.NoError(t, err)
	w := &Warmuper{
		maxDepth: WarmupMaxDepth,
		key:      HexPatriciaWarmupKey,
		step:     HexPatriciaWarmupStep,
	}
	ctx := &warmupRecordContext{record: record}
	require.NotPanics(t, func() {
		w.warmupKey(ctx, []byte{6, 6, 3}, 2, make([]byte, warmupKeyScratchLen))
	})
}

func TestWarmuperStatsConcurrentWithStart(t *testing.T) {
	for range 100 {
		w := NewWarmuper(context.Background(), WarmupConfig{
			MaxDepth: WarmupMaxDepth,
			Key:      HexPatriciaWarmupKey,
			Step:     HexPatriciaWarmupStep,
		})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			w.Start()
		}()
		go func() {
			defer wg.Done()
			for range 1000 {
				w.Stats()
				runtime.Gosched()
			}
		}()
		wg.Wait()
	}
}
