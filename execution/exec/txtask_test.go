// Copyright 2024 The Erigon Authors
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

package exec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAwaitDrainExitsOnContextCancel reproduces the infinite-loop described in
// https://github.com/erigontech/erigon/issues/18252.
//
// Scenario: a map worker crashes after some results have already been moved from
// the resultCh into the heap (e.g. txnums 5..N), but the result the reduce is
// waiting for (txnum 0) was never produced.  The map goroutine calls
// out.Close(), which nils resultCh (because the channel is empty at that
// moment), and the errgroup cancels the shared context.
//
// Before the fix, AwaitDrain took the "resultCh == nil" fast-path and returned
// (false, nil) without ever inspecting ctx, so the reduce loop never terminated.
func TestAwaitDrainExitsOnContextCancel(t *testing.T) {
	q := NewResultsQueue(10, 10)

	// Simulate a worker that produced txnum=5 but the worker responsible for
	// txnum=0 already panicked without producing a result.
	bgCtx := context.Background()
	err := q.Add(bgCtx, &TxResult{Task: &TxTask{TxNum: 5}})
	require.NoError(t, err)

	// Let AwaitDrain move the item from the channel into the heap.
	_, err = q.AwaitDrain(bgCtx, 50*time.Millisecond)
	require.NoError(t, err)
	// resultCh is now empty (all items are in the heap).

	// Simulate the map goroutine calling out.Close() after crashing.
	// Because resultCh is empty, Close() closes and nils the channel immediately.
	q.Close()

	// Simulate the errgroup cancelling the context because the map goroutine
	// returned an error.
	cancelCtx, cancel := context.WithCancel(bgCtx)
	cancel()

	// The reduce loop must exit promptly; without the fix it spins forever.
	done := make(chan error, 1)
	go func() {
		for {
			closed, err := q.AwaitDrain(cancelCtx, 10*time.Millisecond)
			if err != nil {
				done <- err
				return
			}
			if closed {
				done <- nil
				return
			}
			// Simulate processResults making no progress (txnum 0 missing).
		}
	}()

	select {
	case err := <-done:
		require.Error(t, err, "reduce loop must exit with an error when ctx is cancelled")
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("AwaitDrain did not respect context cancellation – infinite loop detected (issue #18252)")
	}
}
