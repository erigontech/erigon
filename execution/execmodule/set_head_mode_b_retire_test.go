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

package execmodule

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests pin the Mode-B retire-coordination contract. The wedge:
// SetHead's waitForQuiescence waited only for SharedDomains, not for
// in-flight BlockRetire. Iter 3 of the 5-iter mode-B soak (2026-06-14)
// hit a 1803s recovery timeout because retire (running 35min before
// SetHead) kept producing snapshot/.idx files for blocks past the new
// unwind target, while the unwind itself wiped block-data underneath it.
//
// Contract being established here:
//   - If retire's MaxScheduledBlock > targetBlock, SetHead cancels it
//     before Provider.Unwind starts. By definition: retire is producing
//     output for blocks the unwind is about to remove — wasted work.
//   - If retire's MaxScheduledBlock <= targetBlock, SetHead leaves it
//     alone (the work is preserved — those blocks survive the unwind).
//   - quiesceRetireIfPastTarget is a no-op when the retire handle is
//     nil (harness / standalone exec, no production wiring).

type fakeRetireCanceller struct {
	working           atomic.Bool
	maxScheduledBlock uint64
	cancelCalls       atomic.Int32
	cancelTimeout     time.Duration
	cancelErr         error

	mu         sync.Mutex
	onCancel   func()
	cancelDone chan struct{}
}

func (f *fakeRetireCanceller) Working() bool             { return f.working.Load() }
func (f *fakeRetireCanceller) MaxScheduledBlock() uint64 { return f.maxScheduledBlock }
func (f *fakeRetireCanceller) CancelInFlight(timeout time.Duration) error {
	f.cancelCalls.Add(1)
	f.mu.Lock()
	f.cancelTimeout = timeout
	cb := f.onCancel
	done := f.cancelDone
	f.mu.Unlock()
	if cb != nil {
		cb()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(timeout):
			return f.cancelErr
		}
	}
	return f.cancelErr
}

func TestQuiesceRetire_NilCancellerIsNoOp(t *testing.T) {
	t.Parallel()
	e := &ExecModule{}
	require.NoError(t, e.quiesceRetireIfPastTarget(2_984_451),
		"no retire handle wired (harness path) should be a clean no-op")
}

func TestQuiesceRetire_IdleRetireIsNoOp(t *testing.T) {
	t.Parallel()
	fr := &fakeRetireCanceller{maxScheduledBlock: 3_007_000}
	// Not Working — retire is idle.
	e := &ExecModule{blockRetire: fr}

	require.NoError(t, e.quiesceRetireIfPastTarget(2_984_451))
	require.Equal(t, int32(0), fr.cancelCalls.Load(),
		"idle retire (Working=false) must not be cancelled — nothing to cancel")
}

func TestQuiesceRetire_RetireBelowTarget_NotCancelled(t *testing.T) {
	t.Parallel()
	fr := &fakeRetireCanceller{maxScheduledBlock: 2_980_000}
	fr.working.Store(true)
	e := &ExecModule{blockRetire: fr}

	require.NoError(t, e.quiesceRetireIfPastTarget(2_984_451))
	require.Equal(t, int32(0), fr.cancelCalls.Load(),
		"retire whose range ends before targetBlock produces work the unwind preserves — must not be cancelled")
	require.True(t, fr.Working(),
		"retire goroutine must keep running when its range is below target")
}

func TestQuiesceRetire_RetirePastTarget_Cancelled(t *testing.T) {
	t.Parallel()
	fr := &fakeRetireCanceller{maxScheduledBlock: 3_007_000}
	fr.working.Store(true)
	// On cancel, the goroutine drains and clears Working.
	fr.cancelDone = make(chan struct{})
	fr.onCancel = func() {
		fr.working.Store(false)
		close(fr.cancelDone)
	}

	e := &ExecModule{blockRetire: fr}
	require.NoError(t, e.quiesceRetireIfPastTarget(2_984_451))
	require.Equal(t, int32(1), fr.cancelCalls.Load(),
		"retire producing files past targetBlock is wasted work — must be cancelled exactly once")
	require.False(t, fr.Working(),
		"after CancelInFlight returns, retire goroutine must be drained")
}

func TestQuiesceRetire_CancelTimeoutSurfacesError(t *testing.T) {
	t.Parallel()
	fr := &fakeRetireCanceller{
		maxScheduledBlock: 3_007_000,
		cancelErr:         errCancelTimeoutSentinel{},
	}
	fr.working.Store(true)
	// No onCancel / no cancelDone — the fake never drains. CancelInFlight
	// returns the synthetic timeout error. SetHead must surface it.
	e := &ExecModule{blockRetire: fr}

	err := e.quiesceRetireIfPastTarget(2_984_451)
	require.Error(t, err,
		"a stuck retire goroutine (CancelInFlight returns error) must surface to the SetHead caller — silent continuation is exactly the inv_extras=3 wedge")
}

type errCancelTimeoutSentinel struct{}

func (errCancelTimeoutSentinel) Error() string { return "retire goroutine did not drain (test)" }
