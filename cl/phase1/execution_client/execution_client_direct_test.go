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
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, chainreader.ErrExecutionBusy
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
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockStopsOnPermanentError(t *testing.T) {
	rejected := errors.New("withdrawals before shanghai")
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, rejected
	})

	// A rejection answers the same way however often it is asked, so retrying it only burns the slot.
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, chainreader.ErrExecutionBusy
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

func TestForkChoiceStatusErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status execmodule.ExecutionStatus
		want   error
	}{
		{"busy is contention, not rejection", execmodule.ExecutionStatusBusy, ErrForkChoiceBusy},
		{"too far away", execmodule.ExecutionStatusTooFarAway, ErrForkChoiceNotAdopted},
		{"missing segment", execmodule.ExecutionStatusMissingSegment, ErrForkChoiceNotAdopted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, forkChoiceStatusError(tc.status), tc.want)
		})
	}
	require.NoError(t, forkChoiceStatusError(execmodule.ExecutionStatusSuccess))
}
