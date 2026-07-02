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

package requests

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newDialError() *net.OpError {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func sequenceOp(attempts *atomic.Int64, errs ...error) func(context.Context) error {
	return func(context.Context) error {
		n := attempts.Add(1)
		if int(n) > len(errs) {
			return errs[len(errs)-1]
		}
		return errs[n-1]
	}
}

func TestRetryConnectsSuccess(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	err := retryConnects(t.Context(), sequenceOp(&attempts, nil))
	require.NoError(t, err)
	require.EqualValues(t, 1, attempts.Load())
}

func TestRetryConnectsNonRecoverableErrorNotRetried(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	var attempts atomic.Int64
	err := retryConnects(t.Context(), sequenceOp(&attempts, boom))
	require.ErrorIs(t, err, boom)
	require.EqualValues(t, 1, attempts.Load())
}

func TestRetryConnectsDialErrorRetriedUntilSuccess(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	start := time.Now()
	err := retryConnects(t.Context(), sequenceOp(&attempts, newDialError(), nil))
	require.NoError(t, err)
	require.EqualValues(t, 2, attempts.Load())
	require.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond)
}

func TestRetryConnectsTimeoutAfterDialErrorReturnsDialError(t *testing.T) {
	t.Parallel()
	dialErr := newDialError()
	var attempts atomic.Int64
	err := retryConnects(t.Context(), sequenceOp(&attempts, dialErr, context.DeadlineExceeded))
	require.ErrorIs(t, err, dialErr)
	require.EqualValues(t, 2, attempts.Load())
}

func TestRetryConnectsTimeoutRetriedUntilSuccess(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	err := retryConnects(t.Context(), sequenceOp(&attempts, context.DeadlineExceeded, nil))
	require.NoError(t, err)
	require.EqualValues(t, 2, attempts.Load())
}

func TestRetryConnectsParentDeadlineReturnsLastDialError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 1200*time.Millisecond)
	defer cancel()
	dialErr := newDialError()
	var attempts atomic.Int64
	err := retryConnects(ctx, sequenceOp(&attempts, dialErr))
	require.ErrorIs(t, err, dialErr)
}

func TestRetryConnectsParentDeadlineAfterOnlyTimeoutsReturnsNil(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 1200*time.Millisecond)
	defer cancel()
	var attempts atomic.Int64
	err := retryConnects(ctx, sequenceOp(&attempts, context.DeadlineExceeded))
	require.NoError(t, err)
}

func TestRetryConnectsParentCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	var attempts atomic.Int64
	err := retryConnects(ctx, sequenceOp(&attempts, newDialError()))
	require.ErrorIs(t, err, context.Canceled)
}
