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

package services

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func canceledPendingQueueContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

func newTestPendingJobQueueWithOptions(ctx context.Context, options pendingJobQueueOptions) *pendingJobQueue[int, string] {
	return newPendingJobQueue(ctx, options,
		func(context.Context, int, string) (func(), bool) {
			return nil, false
		},
		func(int) {},
	)
}

func newTestPendingJobQueue(t *testing.T) *pendingJobQueue[int, string] {
	return newTestPendingJobQueueWithOptions(canceledPendingQueueContext(t), pendingJobQueueOptions{
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	})
}

func TestNewPendingJobQueueRejectsNilTryProcess(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			t.Context(),
			pendingJobQueueOptions{
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			nil,
			func(int) {},
		)
	})
}

func TestNewPendingJobQueueRejectsNilOnExpired(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			t.Context(),
			pendingJobQueueOptions{
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			func(context.Context, int, string) (func(), bool) {
				return nil, false
			},
			nil,
		)
	})
}

func TestNewPendingJobQueueRejectsNonPositiveCapacity(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity int32
	}{
		{name: "zero", capacity: 0},
		{name: "negative", capacity: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.PanicsWithValue(t, "pending job queue capacity must be positive", func() {
				newTestPendingJobQueueWithOptions(t.Context(), pendingJobQueueOptions{
					capacity:      test.capacity,
					expiry:        time.Minute,
					checkInterval: time.Millisecond,
				})
			})
		})
	}
}

func TestNewPendingJobQueueRejectsNonPositiveCheckInterval(t *testing.T) {
	for _, test := range []struct {
		name          string
		checkInterval time.Duration
	}{
		{name: "zero", checkInterval: 0},
		{name: "negative", checkInterval: -time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.PanicsWithValue(t, "pending job queue check interval must be positive", func() {
				newTestPendingJobQueueWithOptions(t.Context(), pendingJobQueueOptions{
					capacity:      1,
					expiry:        time.Minute,
					checkInterval: test.checkInterval,
				})
			})
		})
	}
}

func TestNewPendingJobQueueStartsProcessingLoop(t *testing.T) {
	processed := make(chan string, 1)
	queue := newPendingJobQueue(
		t.Context(),
		pendingJobQueueOptions{
			capacity:      1,
			expiry:        time.Minute,
			checkInterval: time.Millisecond,
		},
		func(_ context.Context, _ int, msg string) (func(), bool) {
			processed <- msg
			return nil, true
		},
		func(int) {},
	)

	require.NoError(t, queue.enqueue("message", func() (int, error) {
		return 1, nil
	}))

	select {
	case msg := <-processed:
		require.Equal(t, "message", msg)
	case <-time.After(time.Second):
		t.Fatal("pending job queue did not process the job")
	}
	require.Eventually(t, func() bool {
		return queue.count.Load() == 0
	}, time.Second, time.Millisecond)
}

func TestPendingJobQueueEnqueueSkipsKeyBuildAtCapacity(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	queue.count.Store(queue.capacity)

	keyBuilt := false
	err := queue.enqueue("message", func() (int, error) {
		keyBuilt = true
		return 1, nil
	})

	require.NoError(t, err)
	require.False(t, keyBuilt)
	require.Equal(t, queue.capacity, queue.count.Load())
}

func TestPendingJobQueueEnqueueReleasesReservationOnKeyBuildPanic(t *testing.T) {
	queue := newTestPendingJobQueue(t)

	require.Panics(t, func() {
		_ = queue.enqueue("message", func() (int, error) {
			panic("key build failed")
		})
	})
	require.Zero(t, queue.count.Load())
}

func TestPendingJobQueueAfterRemoveCanEnqueueSameKey(t *testing.T) {
	var queue *pendingJobQueue[int, string]
	afterRemoveCalled := false

	queue = newPendingJobQueue(canceledPendingQueueContext(t), pendingJobQueueOptions{
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	},
		func(_ context.Context, key int, _ string) (func(), bool) {
			return func() {
				afterRemoveCalled = true
				_, exists := queue.jobs.Load(key)
				require.False(t, exists)
				require.NoError(t, queue.enqueue("replacement", func() (int, error) {
					return key, nil
				}))
			}, true
		},
		func(int) {},
	)

	require.NoError(t, queue.enqueue("original", func() (int, error) {
		return 1, nil
	}))

	queue.processPending(t.Context())

	require.True(t, afterRemoveCalled)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "replacement", stored.(*pendingJob[string]).msg)
}
