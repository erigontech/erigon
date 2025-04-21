// Copyright 2024 The Erigon Authors
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	err := Sleep(ctx, 100*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)

	eg := errgroup.Group{}
	eg.Go(func() error {
		return Sleep(ctx, time.Minute)
	})
	cancel()
	err = eg.Wait()
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Minute)
}
