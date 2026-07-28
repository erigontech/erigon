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

package caplin

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var _ rpchelper.ChainConfigRestartable = (*CaplinService)(nil)

// fakeLaunch returns a LaunchFn that blocks until its ctx is cancelled
// and records each invocation. Used to simulate the Caplin goroutine
// without standing up the real one.
type fakeLaunch struct {
	starts   atomic.Int32
	released chan struct{}
}

func newFakeLaunch() *fakeLaunch {
	return &fakeLaunch{released: make(chan struct{}, 8)}
}

func (f *fakeLaunch) fn() LaunchFn {
	return func(ctx context.Context) error {
		f.starts.Add(1)
		select {
		case f.released <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
}

func TestCaplinService_StartSpawnsLaunch(t *testing.T) {
	fl := newFakeLaunch()
	s := NewCaplinService(context.Background(), datadir.Dirs{}, fl.fn(), nil, log.New())
	require.NoError(t, s.Start(context.Background()))

	select {
	case <-fl.released:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not invoke launch within 2s")
	}
	require.Equal(t, int32(1), fl.starts.Load())

	s.Close()
}

func TestCaplinService_DoubleStartIsIdempotent(t *testing.T) {
	s := NewCaplinService(context.Background(), datadir.Dirs{}, newFakeLaunch().fn(), nil, log.New())
	require.NoError(t, s.Start(context.Background()))
	require.NoError(t, s.Start(context.Background()),
		"second Start while running must be a no-op — matches the sentry/storage Restartable exemplars")
	s.Close()
}

func TestCaplinService_StopExitsCleanly(t *testing.T) {
	unexpected := atomic.Int32{}
	fl := newFakeLaunch()
	s := NewCaplinService(context.Background(), datadir.Dirs{}, fl.fn(), func() { unexpected.Add(1) }, log.New())
	require.NoError(t, s.Start(context.Background()))
	<-fl.released

	s.Close()
	require.Equal(t, int32(0), unexpected.Load(),
		"onUnexpectedExit must not fire when the exit was triggered by Stop")
}

// TestCaplinService_RestartTearsDownAndRelaunches pins the recovery
// path: Restart cancels the prior goroutine, waits for it to exit,
// then spawns a fresh one. The relaunched Caplin runs the normal
// /finalized checkpoint-sync — no anchor override.
func TestCaplinService_RestartTearsDownAndRelaunches(t *testing.T) {
	unexpected := atomic.Int32{}
	fl := newFakeLaunch()
	s := NewCaplinService(context.Background(), datadir.Dirs{}, fl.fn(), func() { unexpected.Add(1) }, log.New())
	require.NoError(t, s.Start(context.Background()))
	<-fl.released

	require.NoError(t, s.Restart())

	select {
	case <-fl.released:
	case <-time.After(5 * time.Second):
		t.Fatal("Restart did not invoke launch a second time within 5s")
	}
	require.Equal(t, int32(2), fl.starts.Load())
	require.Equal(t, int32(0), unexpected.Load(),
		"Restart-driven teardown must not fire onUnexpectedExit")

	s.Close()
}

func TestCaplinService_RestartBeforeStartErrors(t *testing.T) {
	s := NewCaplinService(context.Background(), datadir.Dirs{}, newFakeLaunch().fn(), nil, log.New())
	err := s.Restart()
	require.Error(t, err)
	require.Contains(t, err.Error(), "never started")
}

// TestCaplinService_UnexpectedExitFiresCallback: a launch closure that
// returns on its own (without ctx cancellation by Stop/Restart) must
// cascade to the onUnexpectedExit callback. This preserves the
// pre-refactor behaviour where Caplin death triggered backend shutdown.
func TestCaplinService_UnexpectedExitFiresCallback(t *testing.T) {
	unexpected := make(chan struct{}, 1)
	launch := func(ctx context.Context) error {
		return nil
	}
	s := NewCaplinService(context.Background(), datadir.Dirs{}, launch, func() {
		select {
		case unexpected <- struct{}{}:
		default:
		}
	}, log.New())
	require.NoError(t, s.Start(context.Background()))

	select {
	case <-unexpected:
	case <-time.After(2 * time.Second):
		t.Fatal("onUnexpectedExit must fire when Caplin exits without Stop/Restart")
	}
}

// TestCaplinService_StopStartRoundTrip is the ChainConfigRestartable
// exercise: Start spawns Caplin, Stop drains it cleanly (no unexpected-
// exit callback), and a subsequent Start relaunches with the current
// launch closure — including one swapped via SetLaunch, which the
// debug_setFork orchestrator uses to rebind Caplin to the new
// chain.Config.
func TestCaplinService_StopStartRoundTrip(t *testing.T) {
	unexpected := atomic.Int32{}
	fl := newFakeLaunch()
	s := NewCaplinService(context.Background(), datadir.Dirs{}, fl.fn(), func() { unexpected.Add(1) }, log.New())

	require.NoError(t, s.Start(context.Background()))
	<-fl.released
	require.Equal(t, int32(1), fl.starts.Load())

	require.NoError(t, s.Stop())
	require.Equal(t, int32(0), unexpected.Load(),
		"Stop-driven teardown must not fire onUnexpectedExit")

	// Second Start with a fresh launch closure — the orchestrator's
	// SetChainConfig+SetLaunch sequence in miniature. Assert the fresh
	// closure was invoked (not the original one).
	fl2 := newFakeLaunch()
	s.SetChainConfig(nil) // no-op stub; must not panic
	s.SetLaunch(fl2.fn())
	require.NoError(t, s.Start(context.Background()))
	<-fl2.released
	require.Equal(t, int32(1), fl2.starts.Load(), "the SetLaunch closure must be the one invoked on the second Start")
	require.Equal(t, int32(1), fl.starts.Load(), "the original closure must NOT run again")

	s.Close()
}
