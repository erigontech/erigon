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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type pruneFloorCacheResult struct {
	floor uint64
	err   error
}

func (c *pruneFloorCache[T]) get(ctx context.Context, head uint64, read func() (T, error)) (T, error) {
	return c.getForKey(ctx, pruneFloorCacheKey{head: head}, read)
}

func TestPruneFloorCacheHitsDoNotWaitForWriter(t *testing.T) {
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
	keys := []pruneFloorCacheKey{{head: 1, dbViewID: 1}, {head: 1, dbViewID: 2}}
	for _, key := range keys {
		_, err := cache.getForKey(t.Context(), key, func() (uint64, error) { return key.dbViewID, nil })
		require.NoError(t, err)
	}

	results := make(chan pruneFloorCacheResult, len(keys))
	done := make(chan struct{})
	blocked := false
	func() {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		go func() {
			defer close(done)
			for _, key := range keys {
				floor, err := cache.getForKey(t.Context(), key, func() (uint64, error) {
					return 0, errors.New("unexpected cache miss")
				})
				results <- pruneFloorCacheResult{floor: floor, err: err}
			}
		}()
		for _, key := range keys {
			select {
			case got := <-results:
				require.NoError(t, got.err)
				require.Equal(t, key.dbViewID, got.floor)
			case <-time.After(time.Second):
				blocked = true
				return
			}
		}
	}()
	<-done
	require.False(t, blocked, "fresh cache hit waited for the write lock")
}

func TestPruneFloorCacheEvictsOldEntries(t *testing.T) {
	t.Parallel()

	cache := pruneFloorCache[uint64]{ttl: time.Hour}
	var reads uint64
	read := func() (uint64, error) {
		reads++
		return reads, nil
	}
	for head := uint64(0); head <= pruneFloorCacheSize; head++ {
		floor, err := cache.get(t.Context(), head, read)
		require.NoError(t, err)
		require.Equal(t, head+1, floor)
	}
	for head := uint64(1); head <= pruneFloorCacheSize; head++ {
		floor, err := cache.get(t.Context(), head, read)
		require.NoError(t, err)
		require.Equal(t, head+1, floor)
	}
	require.Equal(t, uint64(pruneFloorCacheSize+1), reads)
	floor, err := cache.get(t.Context(), 0, read)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFloorCacheSize+2), floor)
}

func TestPruneFloorCacheConcurrentEviction(t *testing.T) {
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
	var workers sync.WaitGroup
	for view := range uint64(8) {
		workers.Go(func() {
			for head := range uint64(2 * pruneFloorCacheSize) {
				key := pruneFloorCacheKey{head: head, dbViewID: view}
				want := head*8 + view
				floor, err := cache.getForKey(t.Context(), key, func() (uint64, error) { return want, nil })
				if err != nil || floor != want {
					t.Errorf("key %v: got floor %d, error %v; want %d", key, floor, err, want)
					return
				}
			}
		})
	}
	workers.Wait()

	entries := 0
	cache.values.Range(func(_, _ any) bool {
		entries++
		return true
	})
	require.Equal(t, pruneFloorCacheSize, entries)
}

func TestPruneFloorCacheRefreshesAtNewHead(t *testing.T) {
	t.Parallel()

	var reads atomic.Uint64
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
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
	cache := pruneFloorCache[uint64]{
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

func TestPruneFloorCacheFailedRefreshDoesNotRenewExpiredValue(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)
	cache := pruneFloorCache[uint64]{ttl: time.Second, now: func() time.Time { return now }}
	floor, err := cache.get(t.Context(), 10, func() (uint64, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, uint64(7), floor)

	now = now.Add(time.Second)
	wantErr := errors.New("refresh failed")
	_, err = cache.get(t.Context(), 10, func() (uint64, error) { return 0, wantErr })
	require.ErrorIs(t, err, wantErr)

	floor, err = cache.get(t.Context(), 10, func() (uint64, error) { return 8, nil })
	require.NoError(t, err)
	require.Equal(t, uint64(8), floor)
}

func TestPruneFloorCacheCoalescesConcurrentReadsAtSameHead(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Uint64
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
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
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
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
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
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
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
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

func TestPruneFloorCacheFailedLoadsDoNotSerializeRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := pruneFloorCache[uint64]{ttl: time.Hour}
		leaderErr := errors.New("leader read failed")
		waiterErr := errors.New("waiter read failed")
		release := make(chan struct{})
		leaderResult := make(chan error, 1)
		go func() {
			_, err := cache.get(t.Context(), 10, func() (uint64, error) {
				<-release
				return 0, leaderErr
			})
			leaderResult <- err
		}()
		synctest.Wait()

		const waiters = 4
		var reads atomic.Int64
		results := make(chan error, waiters)
		for range waiters {
			go func() {
				_, err := cache.get(t.Context(), 10, func() (uint64, error) {
					reads.Add(1)
					time.Sleep(time.Second)
					return 0, waiterErr
				})
				results <- err
			}()
		}
		synctest.Wait()
		close(release)
		synctest.Wait()

		started := reads.Load()
		require.ErrorIs(t, <-leaderResult, leaderErr)
		for range waiters {
			require.ErrorIs(t, <-results, waiterErr)
		}
		require.Equal(t, int64(waiters), started, "each waiter retries without waiting for another failing read")
		require.Equal(t, int64(waiters), reads.Load(), "each waiter reads only once")
	})
}

func TestPruneFloorCacheReadFinishesBeforeCallerReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan error, 1)
	cache := pruneFloorCache[uint64]{ttl: time.Hour}

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

func TestPruneFloorCacheRecoversAfterReadPanic(t *testing.T) {
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
	require.PanicsWithValue(t, "read failed", func() {
		_, _ = cache.get(t.Context(), 10, func() (uint64, error) {
			panic("read failed")
		})
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	floor, err := cache.get(ctx, 10, func() (uint64, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, uint64(7), floor)
}
