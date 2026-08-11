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

package errors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancellationMatchingError struct {
	cause error
}

func (e *cancellationMatchingError) Error() string {
	return "cancellation matching error: " + e.cause.Error()
}

func (e *cancellationMatchingError) Is(target error) bool {
	return target == context.Canceled
}

func (e *cancellationMatchingError) Unwrap() error {
	return e.cause
}

func TestNilIfCanceled(t *testing.T) {
	require.NoError(t, NilIfCanceled(nil))
	require.NoError(t, NilIfCanceled(context.Canceled))
	require.NoError(t, NilIfCanceled(fmt.Errorf("drain: %w", context.Canceled)))
	require.NoError(t, NilIfCanceled(errors.Join(context.Canceled, fmt.Errorf("drain: %w", context.Canceled))),
		"a join that is cancellation on every branch is still cancellation")

	boom := errors.New("boom")
	require.Same(t, boom, NilIfCanceled(boom))
	require.ErrorIs(t, NilIfCanceled(context.DeadlineExceeded), context.DeadlineExceeded)

	joined := errors.Join(context.Canceled, boom)
	require.ErrorIs(t, NilIfCanceled(joined), boom,
		"a real branch of a joined error must survive the filter")
	require.ErrorIs(t, NilIfCanceled(errors.Join(boom, context.Canceled)), boom)
	wrapped := fmt.Errorf("teardown: %w", joined)
	require.ErrorIs(t, NilIfCanceled(wrapped), boom,
		"a wrapped join with a real branch must survive the filter")
}

func TestIsOnlyCanceled(t *testing.T) {
	require.False(t, IsOnlyCanceled(nil))
	require.True(t, IsOnlyCanceled(context.Canceled))
	require.True(t, IsOnlyCanceled(fmt.Errorf("drain: %w", context.Canceled)))
	require.True(t, IsOnlyCanceled(errors.Join(context.Canceled, fmt.Errorf("drain: %w", context.Canceled))))

	boom := errors.New("boom")
	require.False(t, IsOnlyCanceled(boom))
	require.False(t, IsOnlyCanceled(context.DeadlineExceeded))
	require.False(t, IsOnlyCanceled(errors.Join(context.Canceled, boom)))
	require.False(t, IsOnlyCanceled(fmt.Errorf("teardown: %w", errors.Join(context.Canceled, boom))))
}

func TestIsOnlyCanceledPreservesCauseBehindNonLeafCancellationMatch(t *testing.T) {
	cause := errors.New("boom")
	err := &cancellationMatchingError{cause: cause}

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, cause)
	require.False(t, IsOnlyCanceled(err))
	require.Same(t, err, NilIfCanceled(err))
}

func TestIsOnly(t *testing.T) {
	targetA := errors.New("target A")
	targetB := errors.New("target B")
	boom := errors.New("boom")

	require.False(t, IsOnly(nil, targetA))
	require.False(t, IsOnly(targetA))
	require.True(t, IsOnly(fmt.Errorf("wrapped: %w", targetA), targetA))
	require.True(t, IsOnly(errors.Join(targetA, fmt.Errorf("wrapped: %w", targetB)), targetA, targetB))
	require.False(t, IsOnly(errors.Join(targetA, boom), targetA, targetB))
}
