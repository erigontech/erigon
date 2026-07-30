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

package event

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const notifierTestTimeout = 10 * time.Second

func waitAsync(n *Notifier, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- n.Wait(ctx) }()
	return done
}

func requireReturns(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(notifierTestTimeout):
		t.Fatal("Wait did not return")
		return nil
	}
}

func TestNotifierWaitReturnsWhenSignaled(t *testing.T) {
	t.Parallel()
	n := NewNotifier()
	done := waitAsync(n, t.Context())
	n.SetAndBroadcast()
	require.NoError(t, requireReturns(t, done))
}

func TestNotifierWaitReturnsWhenAlreadySignaled(t *testing.T) {
	t.Parallel()
	n := NewNotifier()
	n.SetAndBroadcast()
	require.NoError(t, requireReturns(t, waitAsync(n, t.Context())))
}

func TestNotifierWaitReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	n := NewNotifier()
	ctx, cancel := context.WithCancel(t.Context())
	done := waitAsync(n, ctx)
	cancel()
	require.ErrorIs(t, requireReturns(t, done), context.Canceled)
}

func TestNotifierSetAndBroadcastReleasesEveryWaiter(t *testing.T) {
	t.Parallel()
	const waiters = 32
	n := NewNotifier()

	results := make([]<-chan error, waiters)
	for i := range results {
		results[i] = waitAsync(n, t.Context())
	}
	n.SetAndBroadcast()

	for _, done := range results {
		require.NoError(t, requireReturns(t, done))
	}
}

func TestNotifierResetClearsSignal(t *testing.T) {
	t.Parallel()
	n := NewNotifier()
	n.SetAndBroadcast()
	require.NoError(t, requireReturns(t, waitAsync(n, t.Context())))

	n.Reset()
	ctx, cancel := context.WithCancel(t.Context())
	done := waitAsync(n, ctx)
	cancel()
	require.ErrorIs(t, requireReturns(t, done), context.Canceled)
}

// A cancelled Wait must not leave its inner goroutine parked in cond.Wait().
func TestNotifierWaitDoesNotLeakGoroutines(t *testing.T) {
	n := NewNotifier()
	before := runtime.NumGoroutine()

	for range 64 {
		ctx, cancel := context.WithCancel(t.Context())
		done := waitAsync(n, ctx)
		cancel()
		require.ErrorIs(t, requireReturns(t, done), context.Canceled)
	}

	deadline := time.Now().Add(notifierTestTimeout)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before)
}

// Guards the lock discipline: hasEvent is a plain field, so dropping the mutex
// from any of Reset/SetAndBroadcast/Wait turns this into a reported data race.
func TestNotifierConcurrentAccess(t *testing.T) {
	n := NewNotifier()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for ctx.Err() == nil {
				//nolint:errcheck
				n.Wait(ctx)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				n.SetAndBroadcast()
				n.Reset()
			}
		})
	}
	wg.Wait()
}
