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

package common

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
