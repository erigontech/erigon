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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type pendingJob[M any] struct {
	mu           sync.Mutex
	msg          M
	creationTime time.Time
}

type pendingJobQueueOptions struct {
	name          string
	capacity      int32
	expiry        time.Duration
	checkInterval time.Duration
}

type pendingJobEnqueueResult uint8

const (
	pendingJobEnqueueError pendingJobEnqueueResult = iota
	pendingJobEnqueued
	pendingJobDuplicate
	pendingJobQueueFull
)

type pendingJobDecision uint8

const (
	pendingJobKeep pendingJobDecision = iota
	pendingJobRemove
	pendingJobRemoveThenProcess
)

var pendingJobQueueRejectedCounter = metrics.GetOrCreateCounterVec(
	"caplin_pending_job_queue_rejected_total",
	[]string{"queue"},
	"Total pending queue admission attempts rejected at capacity",
)

var errPendingJobQueueFull = errors.New("pending job queue full")

func pendingJobAdmissionError(result pendingJobEnqueueResult, err error) error {
	if err != nil {
		return err
	}
	if result == pendingJobQueueFull {
		return errPendingJobQueueFull
	}
	return nil
}

// pendingJobQueue retries dependency-blocked jobs on the single processing loop
// started by newPendingJobQueue. Jobs remain until the service callback requests
// removal or they expire.
type pendingJobQueue[K comparable, M any] struct {
	pendingJobQueueOptions
	// tryProcess callbacks run sequentially, but may overlap mergeDuplicate.
	// Mutable messages must synchronize their own shared state. mergeDuplicate
	// and onExpired hold the entry lock and must not enqueue the same key;
	// onExpired runs immediately before removal. processAfterRemove runs only
	// after successful removal, so it may re-enqueue.
	tryProcess         func(ctx context.Context, key K, msg M) pendingJobDecision
	processAfterRemove func(ctx context.Context, key K, msg M)
	onExpired          func(key K, msg M)
	mergeDuplicate     func(existing, incoming M)

	jobs sync.Map // K -> *pendingJob[M]
	// count includes stored jobs and reservations held by in-flight enqueues. It
	// can exceed the number of visible jobs, but it never exceeds capacity.
	count    atomic.Int32
	wakeLoop chan struct{}

	cancelLoop context.CancelFunc
	loopWG     sync.WaitGroup

	fullCounter metrics.Counter
}

func newPendingJobQueue[K comparable, M any](
	ctx context.Context,
	options pendingJobQueueOptions,
	tryProcess func(ctx context.Context, key K, msg M) pendingJobDecision,
	processAfterRemove func(ctx context.Context, key K, msg M),
	onExpired func(key K, msg M),
	mergeDuplicate func(existing, incoming M),
) *pendingJobQueue[K, M] {
	if options.capacity <= 0 {
		panic("pending job queue capacity must be positive")
	}
	if options.expiry <= 0 {
		panic("pending job queue expiry must be positive")
	}
	if options.checkInterval <= 0 {
		panic("pending job queue check interval must be positive")
	}
	if options.name == "" {
		panic("pending job queue name must not be empty")
	}
	if tryProcess == nil {
		panic("pending job queue requires tryProcess")
	}
	if onExpired == nil {
		panic("pending job queue requires onExpired")
	}
	loopCtx, cancelLoop := context.WithCancel(ctx)
	q := &pendingJobQueue[K, M]{
		pendingJobQueueOptions: options,
		tryProcess:             tryProcess,
		processAfterRemove:     processAfterRemove,
		onExpired:              onExpired,
		mergeDuplicate:         mergeDuplicate,
		wakeLoop:               make(chan struct{}, 1),
		cancelLoop:             cancelLoop,
		fullCounter:            pendingJobQueueRejectedCounter.WithLabelValues(options.name),
	}
	q.loopWG.Go(func() {
		q.loop(loopCtx)
	})
	return q
}

func (q *pendingJobQueue[K, M]) stopAndWait() {
	q.cancelLoop()
	q.loopWG.Wait()
}

// enqueueKey checks for a duplicate before reserving capacity. Callers that
// already know the key can therefore merge state even when the queue is full.
func (q *pendingJobQueue[K, M]) enqueueKey(key K, msg M) pendingJobEnqueueResult {
	for {
		value, ok := q.jobs.Load(key)
		if !ok {
			break
		}
		if q.mergeStoredDuplicate(key, value.(*pendingJob[M]), msg) {
			return pendingJobDuplicate
		}
	}
	if !q.reserve() {
		return pendingJobQueueFull
	}
	return q.storeReserved(key, msg)
}

// enqueueLazy reserves capacity before building the key so a full queue skips
// potentially expensive work. A duplicate arriving at capacity is therefore
// reported as full because detecting it would require building the key. Storage
// keeps or releases the reservation; deferred cleanup handles key-construction
// errors and panics.
func (q *pendingJobQueue[K, M]) enqueueLazy(msg M, buildKey func() (K, error)) (pendingJobEnqueueResult, error) {
	if !q.reserve() {
		return pendingJobQueueFull, nil
	}

	reservationOwned := true
	defer func() {
		if reservationOwned {
			q.count.Add(-1)
		}
	}()

	key, err := buildKey()
	if err != nil {
		return pendingJobEnqueueError, err
	}
	result := q.storeReserved(key, msg)
	reservationOwned = false
	return result, nil
}

func (q *pendingJobQueue[K, M]) reserve() bool {
	for {
		count := q.count.Load()
		if count >= q.capacity {
			q.fullCounter.Inc()
			return false
		}
		if q.count.CompareAndSwap(count, count+1) {
			return true
		}
	}
}

// storeReserved transfers the caller's reservation to a new job, or releases
// it if the key is already present.
func (q *pendingJobQueue[K, M]) storeReserved(key K, msg M) pendingJobEnqueueResult {
	candidate := &pendingJob[M]{
		msg:          msg,
		creationTime: time.Now(),
	}
	for {
		value, loaded := q.jobs.LoadOrStore(key, candidate)
		if !loaded {
			break
		}
		if !q.mergeStoredDuplicate(key, value.(*pendingJob[M]), msg) {
			continue
		}
		q.count.Add(-1)
		return pendingJobDuplicate
	}
	// Wake notifications may coalesce; count determines whether work remains.
	select {
	case q.wakeLoop <- struct{}{}:
	default:
	}
	return pendingJobEnqueued
}

// mergeStoredDuplicate confirms that existing is still current before merging.
// The entry lock serializes the merge with expiry and removal; callbacks remain
// responsible for synchronizing mutable message state with tryProcess.
func (q *pendingJobQueue[K, M]) mergeStoredDuplicate(key K, existing *pendingJob[M], incoming M) bool {
	existing.mu.Lock()
	defer existing.mu.Unlock()
	current, ok := q.jobs.Load(key)
	if !ok || current != existing {
		return false
	}
	if q.mergeDuplicate != nil {
		q.mergeDuplicate(existing.msg, incoming)
	}
	return true
}

func (q *pendingJobQueue[K, M]) remove(key K, job *pendingJob[M]) bool {
	if !q.jobs.CompareAndDelete(key, job) {
		return false
	}
	q.count.Add(-1)
	return true
}

// loop is the background goroutine that retries pending jobs.
func (q *pendingJobQueue[K, M]) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		for q.count.Load() == 0 {
			select {
			case <-ctx.Done():
				return
			case <-q.wakeLoop:
			}
		}

		ticker := time.NewTicker(q.checkInterval)
		for q.count.Load() > 0 {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				// A tick can win the select after cancellation is ready. Recheck
				// before invoking service code on an already-canceled queue.
				if ctx.Err() != nil {
					ticker.Stop()
					return
				}
				q.processPending(ctx)
			}
		}
		ticker.Stop()
	}
}

// processPending runs callbacks sequentially. Once it observes cancellation, it
// stops before the next job; an already-started callback may finish. It must not
// be called concurrently.
func (q *pendingJobQueue[K, M]) processPending(ctx context.Context) {
	q.jobs.Range(func(key, value any) bool {
		if ctx.Err() != nil {
			return false
		}
		k := key.(K)
		job := value.(*pendingJob[M])
		if time.Since(job.creationTime) > q.expiry {
			job.mu.Lock()
			current, stored := q.jobs.Load(k)
			if stored && current == job {
				q.onExpired(k, job.msg)
				q.remove(k, job)
			}
			job.mu.Unlock()
			return true
		}

		decision := q.tryProcess(ctx, k, job.msg)
		switch decision {
		case pendingJobKeep:
			return true
		case pendingJobRemove:
		case pendingJobRemoveThenProcess:
			if q.processAfterRemove == nil {
				panic("pending job queue requires processAfterRemove")
			}
		default:
			panic("invalid pending job decision")
		}

		job.mu.Lock()
		removed := q.remove(k, job)
		job.mu.Unlock()
		if removed && decision == pendingJobRemoveThenProcess {
			q.processAfterRemove(ctx, k, job.msg)
		}
		return true
	})
}
