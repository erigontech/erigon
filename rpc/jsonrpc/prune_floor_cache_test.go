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

package jsonrpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPruneFloorCacheRefreshesAtNewHead(t *testing.T) {
	t.Parallel()

	var reads atomic.Uint64
	cache := pruneFloorCache{ttl: time.Hour}
	read := func() (uint64, error) { return reads.Add(1), nil }

	floor, err := cache.get(t.Context(), 10, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)
	floor, err = cache.get(t.Context(), 10, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)
	floor, err = cache.get(t.Context(), 11, read)
	require.NoError(t, err)
	require.Equal(t, uint64(2), floor)
	require.Equal(t, uint64(2), reads.Load())
}

func TestPruneFloorCacheRefreshesAtSameHeadAfterExpiry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)
	var reads atomic.Uint64
	cache := pruneFloorCache{
		ttl: time.Second,
		now: func() time.Time { return now },
	}
	read := func() (uint64, error) { return reads.Add(1), nil }

	floor, err := cache.get(t.Context(), 10, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)
	floor, err = cache.get(t.Context(), 10, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)
	require.Equal(t, uint64(1), reads.Load())

	now = now.Add(time.Second)
	floor, err = cache.get(t.Context(), 10, read)
	require.NoError(t, err)
	require.Equal(t, uint64(2), floor)
	require.Equal(t, uint64(2), reads.Load())
}

func TestPruneFloorCacheReadFinishesBeforeCallerReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan error, 1)
	cache := pruneFloorCache{ttl: time.Hour}

	go func() {
		_, err := cache.get(ctx, 1, func() (uint64, error) {
			close(started)
			<-release
			return 1, nil
		})
		returned <- err
	}()

	<-started
	cancel()
	select {
	case err := <-returned:
		close(release)
		require.Failf(t, "floor read outlived caller", "get returned %v while its read was still running", err)
	case <-time.After(250 * time.Millisecond):
		close(release)
	}
	require.ErrorIs(t, <-returned, context.Canceled)
}

func BenchmarkPruneFloorCacheHit(b *testing.B) {
	cache := pruneFloorCache{ttl: time.Hour}
	read := func() (uint64, error) { return 1, nil }
	_, err := cache.get(context.Background(), 1, read)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := cache.get(context.Background(), 1, read)
		if err != nil {
			b.Fatal(err)
		}
	}
}
