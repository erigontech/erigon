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
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type serviceLogCapture struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (c *serviceLogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffer.Write(p)
}

func (c *serviceLogCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffer.String()
}

func (c *serviceLogCapture) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buffer.Bytes())
}

func captureServiceLogs(t *testing.T) *serviceLogCapture {
	t.Helper()
	var output serviceLogCapture
	logger := log.Root()
	previousHandler := logger.GetHandler()
	logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
	t.Cleanup(func() { logger.SetHandler(previousHandler) })
	return &output
}

func canceledPendingQueueContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

func storePendingJob[K comparable, M any](
	t *testing.T,
	queue *pendingJobQueue[K, M],
	key K,
	msg M,
	creationTime time.Time,
) *pendingJob[M] {
	t.Helper()
	job := &pendingJob[M]{
		msg:          msg,
		creationTime: creationTime,
	}
	_, loaded := queue.jobs.LoadOrStore(key, job)
	require.False(t, loaded)
	queue.count.Add(1)
	return job
}

func enqueueTestPendingJob[K comparable, M any](queue *pendingJobQueue[K, M], key K, msg M) error {
	return queue.enqueueLazy(msg, func() (K, error) { return key, nil })
}

func newTestPendingJobQueueWithOptions(ctx context.Context, options pendingJobQueueOptions) *pendingJobQueue[int, string] {
	return newPendingJobQueue(ctx, options,
		func(context.Context, int, string) pendingJobDecision {
			return pendingJobKeep
		},
		nil,
		func(int, string) {},
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
			func(int, string) {},
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

func TestNewPendingJobQueueRejectsNonPositiveExpiry(t *testing.T) {
	for _, test := range []struct {
		name   string
		expiry time.Duration
	}{
		{name: "zero", expiry: 0},
		{name: "negative", expiry: -time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.PanicsWithValue(t, "pending job queue expiry must be positive", func() {
				newTestPendingJobQueueWithOptions(t.Context(), pendingJobQueueOptions{
					name:          t.Name(),
					capacity:      1,
					expiry:        test.expiry,
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

func TestPendingJobQueueLoopRetriesKeptJob(t *testing.T) {
	processed := make(chan string, 1)
	var attempts atomic.Int32
	queue := newPendingJobQueue(
		t.Context(),
		pendingJobQueueOptions{
			name:          t.Name(),
			capacity:      1,
			expiry:        time.Minute,
			checkInterval: time.Millisecond,
		},
		func(_ context.Context, _ int, msg string) pendingJobDecision {
			// Keep the first attempt so only a later polling tick can process
			// and remove the job.
			if attempts.Add(1) == 1 {
				return pendingJobKeep
			}
			processed <- msg
			return pendingJobRemove
		},
		nil,
		func(int, string) {},
	)

	err := queue.enqueueLazy("message", func() (int, error) {
		return 1, nil
	})
	require.NoError(t, err)

	select {
	case msg := <-processed:
		require.Equal(t, "message", msg)
	case <-time.After(time.Second):
		t.Fatal("pending job queue did not retry the kept job")
	}
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.Eventually(t, func() bool {
		return queue.count.Load() == 0
	}, time.Second, time.Millisecond)
}

func TestPendingJobQueueDoesNotProcessAfterContextCancellation(t *testing.T) {
	processed := 0
	for range 1_000 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		queue := &pendingJobQueue[int, string]{
			pendingJobQueueOptions: pendingJobQueueOptions{
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Nanosecond,
			},
			tryProcess: func(context.Context, int, string) pendingJobDecision {
				processed++
				return pendingJobRemove
			},
			onExpired: func(int, string) {},
			wakeLoop:  make(chan struct{}, 1),
		}
		storePendingJob(t, queue, 1, "message", time.Now())

		queue.loop(ctx)
	}

	require.Zero(t, processed)
}

func TestPendingJobQueueCancellationStopsCurrentScan(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	processed := 0
	queue := &pendingJobQueue[int, string]{
		pendingJobQueueOptions: pendingJobQueueOptions{
			capacity: 2,
			expiry:   time.Minute,
		},
		tryProcess: func(context.Context, int, string) pendingJobDecision {
			processed++
			cancel()
			return pendingJobKeep
		},
		onExpired: func(int, string) {},
	}
	storePendingJob(t, queue, 1, "first", time.Now())
	storePendingJob(t, queue, 2, "second", time.Now())

	queue.processPending(ctx)

	require.Equal(t, 1, processed)
}

func TestPendingJobQueueStopWaitsForInFlightProcessing(t *testing.T) {
	processing := make(chan context.Context, 1)
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
		func(int, string) {},
	)
	defer func() {
		releaseProcessing()
		queue.stopAndWait()
	}()
	require.NoError(t, enqueueTestPendingJob(queue, 1, "message"))

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

func TestPendingJobQueueEnqueueDeduplicates(t *testing.T) {
	queue := newTestPendingJobQueueWithOptions(canceledPendingQueueContext(t), pendingJobQueueOptions{
		name:          t.Name(),
		capacity:      2,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	})

	require.NoError(t, enqueueTestPendingJob(queue, 1, "original"))
	require.NoError(t, enqueueTestPendingJob(queue, 1, "duplicate"))
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "original", stored.(*pendingJob[string]).msg)
}

func TestPendingJobQueueExpiryRemovesBeforeCallback(t *testing.T) {
	var queue *pendingJobQueue[int, string]
	callbackSawStoredJob := false
	var enqueueErr error
	queue = newPendingJobQueue(canceledPendingQueueContext(t), pendingJobQueueOptions{
		name:          t.Name(),
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	},
		func(context.Context, int, string) pendingJobDecision {
			return pendingJobKeep
		},
		nil,
		func(key int, _ string) {
			_, callbackSawStoredJob = queue.jobs.Load(key)
			if callbackSawStoredJob {
				return
			}
			enqueueErr = enqueueTestPendingJob(queue, key, "replacement")
		},
	)
	storePendingJob(t, queue, 1, "expired", time.Now().Add(-2*time.Minute))

	queue.processPending(t.Context())

	require.False(t, callbackSawStoredJob)
	require.NoError(t, enqueueErr)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "replacement", stored.(*pendingJob[string]).msg)
}

func TestPendingJobQueueEnqueueSkipsKeyBuildAtCapacity(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	queue.count.Store(queue.capacity)

	keyBuilt := false
	err := queue.enqueueLazy("message", func() (int, error) {
		keyBuilt = true
		return 1, nil
	})

	require.ErrorIs(t, err, errPendingJobQueueFull)
	require.False(t, keyBuilt)
	require.Equal(t, queue.capacity, queue.count.Load())
}

func TestPendingJobQueueEnqueueReleasesReservationOnKeyBuildPanic(t *testing.T) {
	queue := newTestPendingJobQueue(t)

	require.Panics(t, func() {
		_ = queue.enqueueLazy("message", func() (int, error) {
			panic("key build failed")
		})
	})
	require.Zero(t, queue.count.Load())
}

func TestPendingJobQueueEnqueueReleasesReservationOnKeyBuildError(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	wantErr := errors.New("key build failed")

	err := queue.enqueueLazy("message", func() (int, error) {
		return 0, wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Zero(t, queue.count.Load())
}

func TestPendingJobQueueCountsFullRejection(t *testing.T) {
	queue := newTestPendingJobQueue(t)
	queue.count.Store(queue.capacity)
	before := queue.fullCounter.GetValueUint64()

	err := enqueueTestPendingJob(queue, 1, "message")

	require.ErrorIs(t, err, errPendingJobQueueFull)
	require.Equal(t, before+1, queue.fullCounter.GetValueUint64())
}

func TestPendingJobQueueConcurrentEnqueueAdmission(t *testing.T) {
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
			switch err := enqueueTestPendingJob(queue, key, "message"); {
			case err == nil:
				enqueued.Add(1)
			case errors.Is(err, errPendingJobQueueFull):
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

func TestPendingJobQueueConcurrentEnqueueRemoveKeepsCountBounded(t *testing.T) {
	const (
		workers  = 32
		attempts = 10_000
	)
	queue := newTestPendingJobQueue(t)
	held := storePendingJob(t, queue, 0, "held", time.Now())

	start := make(chan struct{})
	continueAfterRemove := make(chan struct{})
	var overCapacity atomic.Bool

	var firstAttempts sync.WaitGroup
	firstAttempts.Add(workers)
	var successfulEnqueues atomic.Int32
	var unexpectedErrors atomic.Int32
	var workersWG sync.WaitGroup
	for worker := range workers {
		workersWG.Go(func() {
			<-start
			if !errors.Is(enqueueTestPendingJob(queue, worker+1, "message"), errPendingJobQueueFull) {
				unexpectedErrors.Add(1)
			}
			firstAttempts.Done()
			<-continueAfterRemove

			for attempt := range attempts {
				key := workers + 1 + worker*attempts + attempt
				switch err := enqueueTestPendingJob(queue, key, "message"); {
				case errors.Is(err, errPendingJobQueueFull):
				case err == nil:
					successfulEnqueues.Add(1)
					runtime.Gosched()
					if queue.count.Load() > queue.capacity {
						overCapacity.Store(true)
					}
					job, loaded := queue.jobs.Load(key)
					if !loaded || !queue.remove(key, job.(*pendingJob[string])) {
						unexpectedErrors.Add(1)
					}
				default:
					unexpectedErrors.Add(1)
				}
			}
		})
	}
	close(start)
	firstAttempts.Wait()
	removed := queue.remove(0, held)
	close(continueAfterRemove)
	workersWG.Wait()

	require.True(t, removed)
	require.False(t, overCapacity.Load())
	require.NotZero(t, successfulEnqueues.Load())
	require.Zero(t, unexpectedErrors.Load())
	require.Zero(t, queue.count.Load())
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
			_ = enqueueTestPendingJob(queue, key, "replacement")
		},
		func(int, string) {},
	)

	_ = enqueueTestPendingJob(queue, 1, "original")

	queue.processPending(t.Context())

	require.True(t, afterRemoveCalled)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "replacement", stored.(*pendingJob[string]).msg)
}
