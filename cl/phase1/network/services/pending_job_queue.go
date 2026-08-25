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
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

type pendingJob[M any] struct {
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
	"Total pending jobs rejected because their queue was full",
)

// pendingJobQueue retries dependency-blocked jobs on the single processing loop
// started by newPendingJobQueue. Jobs remain until the service callback requests
// removal or they expire.
type pendingJobQueue[K comparable, M any] struct {
	pendingJobQueueOptions
	// tryProcess may run repeatedly for a job, and identity-checked removal may
	// fail if the entry was replaced concurrently. Its mutations must be safe to
	// repeat. processAfterRemove may safely re-enqueue the key because it runs
	// only after successful removal.
	tryProcess         func(ctx context.Context, key K, msg M) pendingJobDecision
	processAfterRemove func(ctx context.Context, key K, msg M)
	onExpired          func(key K)

	jobs     sync.Map // K -> *pendingJob[M]
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
	onExpired func(key K),
) *pendingJobQueue[K, M] {
	if options.capacity <= 0 {
		panic("pending job queue capacity must be positive")
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

func (q *pendingJobQueue[K, M]) enqueueKey(key K, msg M) pendingJobEnqueueResult {
	if _, ok := q.jobs.Load(key); ok {
		return pendingJobDuplicate
	}
	if !q.reserve() {
		return pendingJobQueueFull
	}
	return q.storeReserved(key, msg)
}

// enqueueLazy reserves capacity before building the key so a full queue skips
// potentially expensive work. Storage keeps or releases the reservation;
// deferred cleanup handles key-construction errors and panics.
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
	if q.count.Add(1) > q.capacity {
		q.count.Add(-1)
		q.fullCounter.Inc()
		return false
	}
	return true
}

func (q *pendingJobQueue[K, M]) storeReserved(key K, msg M) pendingJobEnqueueResult {
	if _, loaded := q.jobs.LoadOrStore(key, &pendingJob[M]{
		msg:          msg,
		creationTime: time.Now(),
	}); loaded {
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
				q.processPending(ctx)
			}
		}
		ticker.Stop()
	}
}

// processPending must not run concurrently.
func (q *pendingJobQueue[K, M]) processPending(ctx context.Context) {
	q.jobs.Range(func(key, value any) bool {
		k := key.(K)
		job := value.(*pendingJob[M])

		if time.Since(job.creationTime) > q.expiry {
			if q.remove(k, job) {
				q.onExpired(k)
			}
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
		if !q.remove(k, job) {
			return true
		}
		if decision == pendingJobRemoveThenProcess {
			q.processAfterRemove(ctx, k, job.msg)
		}
		return true
	})
}
