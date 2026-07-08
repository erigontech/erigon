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

// pendingJobQueue holds gossip messages whose processing dependencies have not
// arrived yet and retries them periodically until processed or expired.
type pendingJobQueue[K comparable, M any] struct {
	capacity int32
	expiry   time.Duration
	tick     time.Duration
	// tryProcess decides a job's fate: done=false keeps it queued for the next
	// tick; done=true removes it. A non-nil process func runs after the removal,
	// so that a concurrent re-enqueue under the same key is not lost.
	tryProcess func(ctx context.Context, key K, msg M) (process func(), done bool)
	onExpired  func(key K)

	jobs  sync.Map // K -> *pendingJob[M]
	count atomic.Int32
	cond  *sync.Cond
}

func newPendingJobQueue[K comparable, M any](
	capacity int32,
	expiry time.Duration,
	tick time.Duration,
	tryProcess func(ctx context.Context, key K, msg M) (func(), bool),
	onExpired func(key K),
) *pendingJobQueue[K, M] {
	return &pendingJobQueue[K, M]{
		capacity:   capacity,
		expiry:     expiry,
		tick:       tick,
		tryProcess: tryProcess,
		onExpired:  onExpired,
		cond:       sync.NewCond(&sync.Mutex{}),
	}
}

// enqueue adds a job unless the queue is at capacity or the key is already queued.
func (q *pendingJobQueue[K, M]) enqueue(key K, msg M) {
	if q.count.Add(1) > q.capacity {
		q.count.Add(-1)
		return
	}

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

func (q *pendingJobQueue[K, M]) remove(key K) {
	q.jobs.Delete(key)
	q.count.Add(-1)
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
		// Wait until there are pending jobs
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

		// Poll until all pending jobs are processed
		ticker := time.NewTicker(q.tick)
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

func (q *pendingJobQueue[K, M]) processPending(ctx context.Context) {
	q.jobs.Range(func(key, value any) bool {
		k := key.(K)
		job := value.(*pendingJob[M])

		if time.Since(job.creationTime) > q.expiry {
			q.remove(k)
			q.onExpired(k)
			return true
		}

		process, done := q.tryProcess(ctx, k, job.msg)
		if !done {
			return true
		}
		q.remove(k)
		if process != nil {
			process()
		}
		return true
	})
}
