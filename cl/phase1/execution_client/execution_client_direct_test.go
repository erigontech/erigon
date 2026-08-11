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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
)

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, errors.New("busy")
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, errors.New("busy")
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("busy")
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Equal(t, 3, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}

func TestAwaitForkChoiceAdoptedPassesThroughSettledStatus(t *testing.T) {
	calls := 0
	status, err := awaitForkChoiceAdopted(t.Context(), execmodule.ExecutionStatusSuccess, 30, time.Hour,
		func(context.Context) (execmodule.ExecutionStatus, error) {
			calls++
			return execmodule.ExecutionStatusSuccess, nil
		})

	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	require.Zero(t, calls, "a settled status must not be re-sent")
}

func TestAwaitForkChoiceAdoptedWaitsForBusyToSettle(t *testing.T) {
	calls := 0
	status, err := awaitForkChoiceAdopted(t.Context(), execmodule.ExecutionStatusBusy, 30, time.Millisecond,
		func(context.Context) (execmodule.ExecutionStatus, error) {
			calls++
			if calls < 3 {
				return execmodule.ExecutionStatusBusy, nil
			}
			return execmodule.ExecutionStatusSuccess, nil
		})

	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	require.Equal(t, 3, calls)
}

func TestAwaitForkChoiceAdoptedGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	status, err := awaitForkChoiceAdopted(t.Context(), execmodule.ExecutionStatusBusy, 2, time.Millisecond,
		func(context.Context) (execmodule.ExecutionStatus, error) {
			calls++
			return execmodule.ExecutionStatusBusy, nil
		})

	// Still Busy, so the caller reports the head was never adopted rather than assembling on it.
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, status)
	require.Equal(t, 2, calls)
}

func TestAwaitForkChoiceAdoptedStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := awaitForkChoiceAdopted(ctx, execmodule.ExecutionStatusBusy, 30, time.Hour,
		func(context.Context) (execmodule.ExecutionStatus, error) {
			calls++
			return execmodule.ExecutionStatusSuccess, nil
		})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}
