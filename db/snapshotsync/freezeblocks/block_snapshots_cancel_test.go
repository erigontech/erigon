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

package freezeblocks

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests pin the BlockRetire cancellation contract that mode-B SetHead
// relies on: an in-flight retire goroutine must be cancellable from outside,
// because Provider.Unwind cannot proceed while retire is mid-flight producing
// snapshot/.idx files for blocks past the new unwind target.
//
// Backstory: iter 3 of the 5-iter mode-B soak (2026-06-14) wedged the
// recovery phase for 1803s. The unwind itself committed fine (head reached
// target), but RetireBlocks was mid-flight building
// v2.0-003007-003008-transactions.idx (started ~35 min before SetHead ran).
// SetHead waited only for SharedDomains quiescence — not for retire — so
// the unwind wiped block-data tables past toBlock while retire's recsplit
// builder was still reading them. Result: endless "Building recsplit.
// Collision happened." retries and inv_extras=3 orphan snapshot files past
// target. The fix is to give SetHead a cancellation lever on BlockRetire.

func TestBlockRetire_CancelInFlight_NoOpWhenIdle(t *testing.T) {
	br := &BlockRetire{}
	require.False(t, br.Working())

	// Idle CancelInFlight must return promptly (no goroutine to cancel).
	start := time.Now()
	err := br.CancelInFlight(50 * time.Millisecond)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 25*time.Millisecond,
		"CancelInFlight on idle BlockRetire should return immediately")
	require.False(t, br.Working())
}

func TestBlockRetire_CancelInFlight_StopsActiveRetire(t *testing.T) {
	br := &BlockRetire{}

	// Simulate an in-flight retire goroutine that respects ctx.Done.
	ctx, cancel := context.WithCancel(context.Background())
	br.setRetireCancel(cancel)
	br.working.Store(true)

	exited := make(chan struct{})
	go func() {
		<-ctx.Done()
		br.working.Store(false)
		close(exited)
	}()

	// CancelInFlight must trigger cancel + wait for working to drop.
	err := br.CancelInFlight(2 * time.Second)
	require.NoError(t, err)

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("simulated retire goroutine did not exit after CancelInFlight")
	}
	require.False(t, br.Working())
}

func TestBlockRetire_CancelInFlight_TimeoutIfStuck(t *testing.T) {
	br := &BlockRetire{}

	// Stuck goroutine — ignores ctx.Done.
	_, cancel := context.WithCancel(context.Background())
	br.setRetireCancel(cancel)
	br.working.Store(true)
	// Reset working at end so the test doesn't leak goroutine state.
	defer br.working.Store(false)

	err := br.CancelInFlight(50 * time.Millisecond)
	require.Error(t, err,
		"CancelInFlight should return a timeout error when retire goroutine ignores cancel")
}

func TestBlockRetire_CancelInFlight_RaceSafe(t *testing.T) {
	// Concurrent CancelInFlight calls should not panic and at most one
	// should observe Working()==true at the moment of cancel — the rest
	// should treat the BlockRetire as already idle.
	br := &BlockRetire{}

	ctx, cancel := context.WithCancel(context.Background())
	br.setRetireCancel(cancel)
	br.working.Store(true)

	var seenCancel atomic.Int32
	go func() {
		<-ctx.Done()
		seenCancel.Add(1)
		br.working.Store(false)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = br.CancelInFlight(time.Second)
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), seenCancel.Load(),
		"goroutine should observe exactly one cancel signal regardless of concurrent CancelInFlight callers")
	require.False(t, br.Working())
}

func TestBlockRetire_MaxScheduledBlockAccessor(t *testing.T) {
	br := &BlockRetire{}
	require.Equal(t, uint64(0), br.MaxScheduledBlock())
	br.maxScheduledBlock.Store(3_007_000)
	require.Equal(t, uint64(3_007_000), br.MaxScheduledBlock())
}
