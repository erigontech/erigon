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
)

type pendingJob[M any] struct {
	msg          M
	creationTime time.Time
}

type pendingJobQueueOptions struct {
	capacity      int32
	expiry        time.Duration
	checkInterval time.Duration
}

type pendingJobDecision uint8

const (
	pendingJobKeep pendingJobDecision = iota
	pendingJobRemove
	pendingJobRemoveThenProcess
)

// pendingJobQueue retries dependency-blocked jobs on the single processing loop
// started by newPendingJobQueue. Jobs remain until the service callback requests
// removal or they expire.
type pendingJobQueue[K comparable, M any] struct {
	pendingJobQueueOptions
	// Mutations in tryProcess must remain safe if the job is retried or removal
	// loses the identity check. processAfterRemove runs only after successful
	// removal, so it may safely re-enqueue the same key.
	tryProcess         func(ctx context.Context, key K, msg M) pendingJobDecision
	processAfterRemove func(ctx context.Context, key K, msg M)
	onExpired          func(key K)

	jobs  sync.Map // K -> *pendingJob[M]
	count atomic.Int32
	cond  *sync.Cond
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
	if tryProcess == nil {
		panic("pending job queue requires tryProcess")
	}
	if onExpired == nil {
		panic("pending job queue requires onExpired")
	}
	q := &pendingJobQueue[K, M]{
		pendingJobQueueOptions: options,
		tryProcess:             tryProcess,
		processAfterRemove:     processAfterRemove,
		onExpired:              onExpired,
		cond:                   sync.NewCond(&sync.Mutex{}),
	}
	go q.loop(ctx)
	return q
}

func (q *pendingJobQueue[K, M]) enqueueKey(key K, msg M) {
	if !q.reserve() {
		return
	}
	q.storeReserved(key, msg)
}

// enqueueLazy reserves capacity before building the key so a full queue skips
// potentially expensive work. The enqueue attempt owns its reservation until a
// job is stored, so deferred cleanup must cover both errors and panics.
func (q *pendingJobQueue[K, M]) enqueueLazy(msg M, buildKey func() (K, error)) error {
	if !q.reserve() {
		return nil
	}

	reservationOwned := true
	defer func() {
		if reservationOwned {
			q.count.Add(-1)
		}
	}()

	key, err := buildKey()
	if err != nil {
		return err
	}
	q.storeReserved(key, msg)
	reservationOwned = false
	return nil
}

func (q *pendingJobQueue[K, M]) reserve() bool {
	if q.count.Add(1) > q.capacity {
		q.count.Add(-1)
		return false
	}
	return true
}

func (q *pendingJobQueue[K, M]) storeReserved(key K, msg M) {
	if _, loaded := q.jobs.LoadOrStore(key, &pendingJob[M]{
		msg:          msg,
		creationTime: time.Now(),
	}); loaded {
		q.count.Add(-1)
	} else {
		q.cond.L.Lock()
		q.cond.Signal()
		q.cond.L.Unlock()
	}
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
	// Wake any blocked Wait() on context cancellation to prevent deadlock.
	go func() {
		<-ctx.Done()
		q.cond.L.Lock()
		q.cond.Broadcast()
		q.cond.L.Unlock()
	}()

	for {
		q.cond.L.Lock()
		for q.count.Load() == 0 {
			select {
			case <-ctx.Done():
				q.cond.L.Unlock()
				return
			default:
			}
			q.cond.Wait()
		}
		q.cond.L.Unlock()

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
