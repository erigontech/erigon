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
	"sync"
	"sync/atomic"
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
		func(context.Context, int, string) pendingJobDecision {
			return pendingJobKeep
		},
		nil,
		func(int) {},
	)
}

func newTestPendingJobQueue(t *testing.T) *pendingJobQueue[int, string] {
	return newTestPendingJobQueueWithOptions(canceledPendingQueueContext(t), pendingJobQueueOptions{
		name:          t.Name(),
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
				name:          t.Name(),
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			nil,
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
				name:          t.Name(),
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			func(context.Context, int, string) pendingJobDecision {
				return pendingJobKeep
			},
			nil,
			nil,
		)
	})
}

func TestNewPendingJobQueueRejectsEmptyName(t *testing.T) {
	require.PanicsWithValue(t, "pending job queue name must not be empty", func() {
		newTestPendingJobQueueWithOptions(t.Context(), pendingJobQueueOptions{
			capacity:      1,
			expiry:        time.Minute,
			checkInterval: time.Millisecond,
		})
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
			name:          t.Name(),
			capacity:      1,
			expiry:        time.Minute,
			checkInterval: time.Millisecond,
		},
		func(_ context.Context, _ int, msg string) pendingJobDecision {
			processed <- msg
			return pendingJobRemove
		},
		nil,
		func(int) {},
	)

	result, err := queue.enqueueLazy("message", func() (int, error) {
		return 1, nil
	})
	require.NoError(t, err)
	require.Equal(t, pendingJobEnqueued, result)

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

func TestPendingJobQueueStopWaitsForInFlightProcessing(t *testing.T) {
	processing := make(chan context.Context)
	processingReleased := make(chan struct{})
	var releaseOnce sync.Once
	releaseProcessing := func() {
		releaseOnce.Do(func() { close(processingReleased) })
	}

	queue := newPendingJobQueue(
		t.Context(),
		pendingJobQueueOptions{
			name:          t.Name(),
			capacity:      1,
			expiry:        time.Minute,
			checkInterval: time.Millisecond,
		},
		func(ctx context.Context, _ int, _ string) pendingJobDecision {
			processing <- ctx
			<-processingReleased
			return pendingJobRemove
		},
		nil,
		func(int) {},
	)
	defer func() {
		releaseProcessing()
		queue.stopAndWait()
	}()
	require.Equal(t, pendingJobEnqueued, queue.enqueueKey(1, "message"))

	var processingCtx context.Context
	select {
	case processingCtx = <-processing:
	case <-time.After(time.Second):
		t.Fatal("pending job queue did not start processing")
	}

	stopped := make(chan struct{})
	go func() {
		queue.stopAndWait()
		close(stopped)
	}()
	select {
	case <-processingCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("pending job queue was not cancelled")
	}
	select {
	case <-stopped:
		t.Fatal("pending job queue reported completion while processing was still active")
	default:
	}

	releaseProcessing()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("pending job queue did not stop")
	}
}

func TestPendingJobQueueEnqueueKeyDeduplicates(t *testing.T) {
	queue := newTestPendingJobQueue(t)

	firstResult := queue.enqueueKey(1, "original")
	duplicateResult := queue.enqueueKey(1, "duplicate")

	require.Equal(t, pendingJobEnqueued, firstResult)
	require.Equal(t, pendingJobDuplicate, duplicateResult)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "original", stored.(*pendingJob[string]).msg)
}

func TestPendingJobQueueEnqueueSkipsKeyBuildAtCapacity(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	queue.count.Store(queue.capacity)

	keyBuilt := false
	result, err := queue.enqueueLazy("message", func() (int, error) {
		keyBuilt = true
		return 1, nil
	})

	require.NoError(t, err)
	require.Equal(t, pendingJobQueueFull, result)
	require.False(t, keyBuilt)
	require.Equal(t, queue.capacity, queue.count.Load())
}

func TestPendingJobQueueEnqueueReleasesReservationOnKeyBuildPanic(t *testing.T) {
	queue := newTestPendingJobQueue(t)

	require.Panics(t, func() {
		_, _ = queue.enqueueLazy("message", func() (int, error) {
			panic("key build failed")
		})
	})
	require.Zero(t, queue.count.Load())
}

func TestPendingJobQueueCountsFullRejection(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	queue.count.Store(queue.capacity)
	before := queue.fullCounter.GetValueUint64()

	result := queue.enqueueKey(1, "message")

	require.Equal(t, pendingJobQueueFull, result)
	require.Equal(t, before+1, queue.fullCounter.GetValueUint64())
}

func TestPendingJobQueueConcurrentEnqueueResults(t *testing.T) {
	const capacity = int32(5)
	queue := newTestPendingJobQueueWithOptions(canceledPendingQueueContext(t), pendingJobQueueOptions{
		name:          t.Name(),
		capacity:      capacity,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	})
	var enqueued atomic.Int32
	var full atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup

	for key := range 100 {
		wg.Go(func() {
			switch queue.enqueueKey(key, "message") {
			case pendingJobEnqueued:
				enqueued.Add(1)
			case pendingJobQueueFull:
				full.Add(1)
			default:
				unexpected.Add(1)
			}
		})
	}
	wg.Wait()

	require.Equal(t, capacity, enqueued.Load())
	require.Equal(t, int32(100)-capacity, full.Load())
	require.Zero(t, unexpected.Load())
	require.Equal(t, capacity, queue.count.Load())
}

func TestPendingJobQueueAfterRemoveCanEnqueueSameKey(t *testing.T) {
	var queue *pendingJobQueue[int, string]
	afterRemoveCalled := false

	queue = newPendingJobQueue(canceledPendingQueueContext(t), pendingJobQueueOptions{
		name:          t.Name(),
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	},
		func(context.Context, int, string) pendingJobDecision {
			return pendingJobRemoveThenProcess
		},
		func(_ context.Context, key int, _ string) {
			afterRemoveCalled = true
			_, exists := queue.jobs.Load(key)
			require.False(t, exists)
			_ = queue.enqueueKey(key, "replacement")
		},
		func(int) {},
	)

	_ = queue.enqueueKey(1, "original")

	queue.processPending(t.Context())

	require.True(t, afterRemoveCalled)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "replacement", stored.(*pendingJob[string]).msg)
}
