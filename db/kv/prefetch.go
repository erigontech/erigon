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

package kv

import (
	"context"
	"errors"
	"sync"
)

type Prefetcher struct {
	ctx       context.Context
	cancel    context.CancelFunc
	jobs      chan prefetchJob
	workers   sync.WaitGroup
	mu        sync.RWMutex
	closeOnce sync.Once
}

type prefetchJob struct {
	ctx   context.Context
	pairs [][2][]byte
	batch *PrefetchBatch
}

type PrefetchBatch struct {
	pending sync.WaitGroup
	once    sync.Once
	err     error
}

func NewPrefetcher(db RoDB, table string, workers uint64) *Prefetcher {
	return newPrefetcher(workers, func(ctx context.Context, pairs [][2][]byte) error {
		tx, err := db.BeginRo(WithNonBlockingAcquire(ctx))
		if err != nil {
			if errors.Is(err, ErrReadTxLimitExceeded) {
				return nil
			}
			return err
		}
		defer tx.Rollback()
		c, err := tx.CursorDupSort(table)
		if err != nil {
			return err
		}
		defer c.Close()
		for _, pair := range pairs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, _, err := c.SeekExact(pair[0]); err != nil {
				return err
			}
			if _, err := c.SeekBothRange(pair[0], pair[1]); err != nil {
				return err
			}
		}
		return nil
	})
}

func newPrefetcher(workers uint64, fetch func(context.Context, [][2][]byte) error) *Prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Prefetcher{ctx: ctx, cancel: cancel, jobs: make(chan prefetchJob, workers)}
	for range cap(p.jobs) {
		p.workers.Go(func() {
			for job := range p.jobs {
				jobCtx, cancel := context.WithCancel(job.ctx)
				stop := context.AfterFunc(ctx, cancel)
				err := ctx.Err()
				if err == nil {
					err = jobCtx.Err()
				}
				if err == nil {
					err = fetch(jobCtx, job.pairs)
				}
				stop()
				cancel()
				job.batch.finish(err)
			}
		})
	}
	return p
}

// The pairs and their bytes must remain unchanged until the batch's Wait returns.
func (p *Prefetcher) Prefetch(ctx context.Context, pairs [][2][]byte) *PrefetchBatch {
	p.mu.RLock()
	defer p.mu.RUnlock()
	b := &PrefetchBatch{}
	if err := p.ctx.Err(); err != nil {
		b.err = err
		return b
	}
	if err := ctx.Err(); err != nil {
		b.err = err
		return b
	}
	parts := min(cap(p.jobs), len(pairs))
	b.pending.Add(parts)
	for part := range parts {
		select {
		case p.jobs <- prefetchJob{ctx: ctx, pairs: pairs[len(pairs)*part/parts : len(pairs)*(part+1)/parts], batch: b}:
		case <-p.ctx.Done():
			b.finish(p.ctx.Err())
		case <-ctx.Done():
			b.finish(ctx.Err())
		}
	}
	return b
}

func (b *PrefetchBatch) finish(err error) {
	if err != nil {
		b.once.Do(func() { b.err = err })
	}
	b.pending.Done()
}

func (b *PrefetchBatch) Wait() error {
	b.pending.Wait()
	return b.err
}

func (p *Prefetcher) Close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.mu.Lock()
		close(p.jobs)
		p.mu.Unlock()
		p.workers.Wait()
	})
}
