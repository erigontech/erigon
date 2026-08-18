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

package execution_client

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
)

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, execmodule.ErrBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Equal(t, 3, calls)
}

func TestRetryAssembleBlockStopsOnRejection(t *testing.T) {
	rejected := errors.New("withdrawals before shanghai")
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 30, time.Minute, func(context.Context) (uint64, error) {
		calls++
		return 0, rejected
	})

	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockStopsOnAbandonedBusyAttempt(t *testing.T) {
	abandonedBusy := errors.Join(execmodule.ErrRequestAbandoned, execmodule.ErrBusy)
	calls := 0

	_, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, abandonedBusy
	})

	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, execmodule.ErrBusy)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 2, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, execmodule.ErrBusy
	})

	require.ErrorIs(t, err, execmodule.ErrBusy)
	require.Equal(t, 2, calls)
}

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		_, err := retryAssembleBlock(ctx, 30, time.Minute, func(context.Context) (uint64, error) {
			calls++
			cancel()
			return 0, execmodule.ErrBusy
		})

		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
		require.ErrorIs(t, err, execmodule.ErrBusy, "the contention that caused the wait must survive in the error")
		require.Equal(t, 1, calls)
	})
}

func TestRetryAssembleBlockDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Second, func(context.Context) (uint64, error) {
		calls++
		return 0, execmodule.ErrBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}

func TestRetryAssembleBlockKeepsCancellationOnTheFinalAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 1, time.Second, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, execmodule.ErrBusy
	})

	require.Equal(t, 1, calls)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, execmodule.ErrBusy)
}
