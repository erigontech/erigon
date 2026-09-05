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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type pruneFloorCacheResult struct {
	floor uint64
	err   error
}

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

func TestPruneFloorCacheSeparatesFileViewsAtSameHead(t *testing.T) {
	t.Parallel()

	var reads atomic.Uint64
	cache := pruneFloorCache{ttl: time.Hour}
	read := func() (uint64, error) { return reads.Add(1), nil }

	first := pruneFloorCacheKey{head: 10, snapshotGeneration: 1}
	floor, err := cache.getForKey(t.Context(), first, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)
	floor, err = cache.getForKey(t.Context(), first, read)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor)

	second := pruneFloorCacheKey{head: 10, snapshotGeneration: 2}
	floor, err = cache.getForKey(t.Context(), second, read)
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

func TestPruneFloorCacheCoalescesConcurrentReadsAtSameHead(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Uint64
	cache := pruneFloorCache{ttl: time.Hour}
	read := func() (uint64, error) {
		if reads.Add(1) == 1 {
			close(started)
		}
		<-release
		return 7, nil
	}

	results := make(chan pruneFloorCacheResult, 2)
	get := func() {
		floor, err := cache.get(t.Context(), 10, read)
		results <- pruneFloorCacheResult{floor: floor, err: err}
	}

	go get()
	<-started
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		get()
	}()
	<-secondStarted
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, uint64(1), reads.Load())
	close(release)

	for range 2 {
		got := <-results
		require.NoError(t, got.err)
		require.Equal(t, uint64(7), got.floor)
	}
	require.Equal(t, uint64(1), reads.Load())
}

func TestPruneFloorCacheLoadsDifferentHeadsConcurrently(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	cache := pruneFloorCache{ttl: time.Hour}
	firstResult := make(chan pruneFloorCacheResult, 1)
	go func() {
		floor, err := cache.get(t.Context(), 10, func() (uint64, error) {
			close(firstStarted)
			<-releaseFirst
			return 7, nil
		})
		firstResult <- pruneFloorCacheResult{floor: floor, err: err}
	}()
	<-firstStarted

	secondReadStarted := make(chan struct{})
	secondResult := make(chan pruneFloorCacheResult, 1)
	go func() {
		floor, err := cache.get(t.Context(), 11, func() (uint64, error) {
			close(secondReadStarted)
			return 8, nil
		})
		secondResult <- pruneFloorCacheResult{floor: floor, err: err}
	}()

	select {
	case <-secondReadStarted:
	case <-time.After(time.Second):
		close(releaseFirst)
		require.Fail(t, "different-head read was blocked by the in-flight read")
	}
	second := <-secondResult
	require.NoError(t, second.err)
	require.Equal(t, uint64(8), second.floor)

	close(releaseFirst)
	first := <-firstResult
	require.NoError(t, first.err)
	require.Equal(t, uint64(7), first.floor)
}

func TestPruneFloorCacheWaiterHonorsContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cache := pruneFloorCache{ttl: time.Hour}
	leaderResult := make(chan pruneFloorCacheResult, 1)
	go func() {
		floor, err := cache.get(t.Context(), 10, func() (uint64, error) {
			close(started)
			<-release
			return 7, nil
		})
		leaderResult <- pruneFloorCacheResult{floor: floor, err: err}
	}()
	<-started

	ctx, cancel := context.WithCancel(t.Context())
	waiterResult := make(chan error, 1)
	go func() {
		_, err := cache.get(ctx, 10, func() (uint64, error) {
			return 8, nil
		})
		waiterResult <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-waiterResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(release)
		require.Fail(t, "waiter did not honor its context")
	}
	close(release)
	require.NoError(t, (<-leaderResult).err)
}

func TestPruneFloorCacheWaiterRetriesAfterLeaderError(t *testing.T) {
	wantErr := errors.New("leader read failed")
	started := make(chan struct{})
	release := make(chan struct{})
	cache := pruneFloorCache{ttl: time.Hour}
	leaderResult := make(chan pruneFloorCacheResult, 1)
	go func() {
		floor, err := cache.get(t.Context(), 10, func() (uint64, error) {
			close(started)
			<-release
			return 0, wantErr
		})
		leaderResult <- pruneFloorCacheResult{floor: floor, err: err}
	}()
	<-started

	waiterResult := make(chan pruneFloorCacheResult, 1)
	go func() {
		floor, err := cache.get(t.Context(), 10, func() (uint64, error) {
			return 8, nil
		})
		waiterResult <- pruneFloorCacheResult{floor: floor, err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	require.ErrorIs(t, (<-leaderResult).err, wantErr)
	waiter := <-waiterResult
	require.NoError(t, waiter.err)
	require.Equal(t, uint64(8), waiter.floor)
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
