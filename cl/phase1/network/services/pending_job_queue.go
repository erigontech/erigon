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

// pendingJobQueue retries dependency-blocked jobs until the service callback
// requests their removal or they expire.
type pendingJobQueue[K comparable, M any] struct {
	capacity int32
	expiry   time.Duration
	tick     time.Duration
	// tryProcess returns whether to remove the current job and an optional
	// callback that runs only after identity-checked removal. This lets the
	// callback enqueue the same key without the new job being deleted.
	tryProcess func(ctx context.Context, key K, msg M) (afterRemove func(), remove bool)
	onExpired  func(key K)

	jobs  sync.Map // K -> *pendingJob[M]
	count atomic.Int32
	cond  *sync.Cond
}

func newPendingJobQueue[K comparable, M any](
	options pendingJobQueueOptions,
	tryProcess func(ctx context.Context, key K, msg M) (afterRemove func(), remove bool),
	onExpired func(key K),
) *pendingJobQueue[K, M] {
	if tryProcess == nil {
		panic("pending job queue requires tryProcess")
	}
	if onExpired == nil {
		panic("pending job queue requires onExpired")
	}
	return &pendingJobQueue[K, M]{
		capacity:   options.capacity,
		expiry:     options.expiry,
		tick:       options.checkInterval,
		tryProcess: tryProcess,
		onExpired:  onExpired,
		cond:       sync.NewCond(&sync.Mutex{}),
	}
}

// enqueue reserves capacity before building the key, so a full queue skips key
// construction. A full queue or a duplicate key is a silent no-op; a
// key-building error releases the reservation.
func (q *pendingJobQueue[K, M]) enqueue(msg M, buildKey func() (K, error)) error {
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
			if q.remove(k, job) {
				q.onExpired(k)
			}
			return true
		}

		afterRemove, remove := q.tryProcess(ctx, k, job.msg)
		if !remove {
			return true
		}
		if !q.remove(k, job) {
			return true
		}
		if afterRemove != nil {
			afterRemove()
		}
		return true
	})
}
