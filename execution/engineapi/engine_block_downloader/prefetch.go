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

package engine_block_downloader

import (
	"context"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

type batchFeed interface {
	Next(ctx context.Context) (p2p.BlockBatchResult, error)
}

type prefetchedBatch struct {
	batch  p2p.BlockBatchResult
	err    error
	weight int64
}

// batchPrefetcher keeps pulling batches from the feed while the consumer executes
// the previous chunk. budget is a soft cap on the encoded size of queued batches:
// one more batch waits for room, and a batch larger than budget still goes through.
type batchPrefetcher struct {
	ch     chan prefetchedBatch
	budget *semaphore.Weighted
}

func newBatchPrefetcher(ctx context.Context, feed batchFeed, budget int64) *batchPrefetcher {
	budget = max(budget, 1)
	p := &batchPrefetcher{
		ch:     make(chan prefetchedBatch, 64),
		budget: semaphore.NewWeighted(budget),
	}
	go p.run(ctx, feed, budget)
	return p
}

func (p *batchPrefetcher) run(ctx context.Context, feed batchFeed, budget int64) {
	defer close(p.ch)
	for {
		batch, err := feed.Next(ctx)
		weight := min(max(batchSize(batch.Blocks), 1), budget)
		if p.budget.Acquire(ctx, weight) != nil {
			return
		}
		select {
		case p.ch <- prefetchedBatch{batch: batch, err: err, weight: weight}:
		case <-ctx.Done():
			return
		}
		if err != nil || len(batch.Blocks) == 0 {
			return
		}
	}
}

func batchSize(blocks []*types.Block) int64 {
	var size int64
	for _, b := range blocks {
		size += int64(b.Size())
		if raw, err := b.BlockAccessListSidecar().Bytes(); err == nil {
			size += int64(len(raw))
		}
	}
	return size
}

func (p *batchPrefetcher) Next(ctx context.Context) (p2p.BlockBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return p2p.BlockBatchResult{}, err
	}
	select {
	case <-ctx.Done():
		return p2p.BlockBatchResult{}, ctx.Err()
	case item, ok := <-p.ch:
		if !ok {
			return p2p.BlockBatchResult{}, ctx.Err()
		}
		p.budget.Release(item.weight)
		return item.batch, item.err
	}
}
