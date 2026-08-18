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
	"context"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/stretchr/testify/require"
)

// occupiedModule returns a module whose semaphore is already taken, so SetHead cannot get past the
// wait, with a wait short enough to run in a test.
func occupiedModule(t *testing.T) *ExecModule {
	t.Helper()
	module := &ExecModule{semaphore: semaphore.NewWeighted(1), setHeadAcquireTimeout: time.Millisecond}
	require.NoError(t, module.semaphore.Acquire(t.Context(), 1))
	return module
}

func TestSetHeadReportsBusyWhenItsOwnWaitRunsOut(t *testing.T) {
	err := occupiedModule(t).SetHead(t.Context(), 1)

	// Reaching the wait's own deadline is what says the module was occupied.
	require.ErrorIs(t, err, ErrBusy)
}

func TestSetHeadDoesNotReportBusyWhenTheCallerGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := occupiedModule(t).SetHead(ctx, 1)

	// Nothing is known about the module, so calling it busy would invite a retry with nothing to
	// wait for.
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrBusy)
}

func TestSetHeadDoesNotReportBusyForACallerCancelledWithThatCause(t *testing.T) {
	// A caller may cancel with any cause it likes, including this package's own sentinel. That says
	// nothing about the module, so the marker classified on has to be one no caller can supply.
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(ErrBusy)

	err := occupiedModule(t).SetHead(ctx, 1)

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrBusy)
}

func TestSetHeadDoesNotReportBusyForACallerWithAShorterDeadline(t *testing.T) {
	module := &ExecModule{semaphore: semaphore.NewWeighted(1), setHeadAcquireTimeout: time.Hour}
	require.NoError(t, module.semaphore.Acquire(t.Context(), 1))

	// The caller's own deadline expires long before this wait would, so what ran out is the
	// caller's patience, not the module's. Classifying on the error's shape rather than on which
	// deadline was reached reports the module as occupied on no evidence at all.
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()

	err := module.SetHead(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrBusy)
}
