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
	"time"

	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

// A resource cap, not a protocol bound: votes that arrive before their block usually name one or a
// few recent blocks.
const maxBlockArrivalRoots = 32

// blockArrivalQueue keeps replays of gossip messages that name a block fork
// choice has not imported yet and runs them once it has.
type blockArrivalQueue struct {
	forkchoiceStore forkchoice.ForkChoiceStorageReader
	maxReplays      int
	ttl             time.Duration

	mu      sync.Mutex
	byRoot  map[common.Hash]*pendingReplays
	replays int
}

type pendingReplays struct {
	queuedAt time.Time
	replays  []func(context.Context)
}

func newBlockArrivalQueue(forkchoiceStore forkchoice.ForkChoiceStorageReader, maxReplays int, ttl time.Duration) *blockArrivalQueue {
	return &blockArrivalQueue{
		forkchoiceStore: forkchoiceStore,
		maxReplays:      maxReplays,
		ttl:             ttl,
		byRoot:          make(map[common.Hash]*pendingReplays),
	}
}

// add drops the replay when the queue is full.
func (q *blockArrivalQueue) add(root common.Hash, replay func(context.Context)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.replays >= q.maxReplays {
		return
	}
	pending, ok := q.byRoot[root]
	if !ok {
		if len(q.byRoot) >= maxBlockArrivalRoots {
			return
		}
		pending = &pendingReplays{queuedAt: time.Now()}
		q.byRoot[root] = pending
	}
	pending.replays = append(pending.replays, replay)
	q.replays++
}

func (q *blockArrivalQueue) run(ctx context.Context) {
	ticker := time.NewTicker(attestationJobsIntervalTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			q.replayArrived(ctx, now)
		}
	}
}

// replayArrived drops roots queued for longer than the ttl and runs the
// replays of roots whose block fork choice now has.
func (q *blockArrivalQueue) replayArrived(ctx context.Context, now time.Time) {
	q.mu.Lock()
	roots := make([]common.Hash, 0, len(q.byRoot))
	for root, pending := range q.byRoot {
		if now.Sub(pending.queuedAt) > q.ttl {
			q.removeLocked(root)
			continue
		}
		roots = append(roots, root)
	}
	q.mu.Unlock()

	// mu is not held while calling fork choice, so add stays safe under any caller lock.
	var ready []func(context.Context)
	for _, root := range roots {
		if _, ok := q.forkchoiceStore.GetHeader(root); ok {
			q.mu.Lock()
			ready = append(ready, q.removeLocked(root)...)
			q.mu.Unlock()
		}
	}

	// Each replay waits for its signature batch, so run enough at once to fill a batch.
	sem := make(chan struct{}, batchSignatureVerificationThreshold)
	var wg sync.WaitGroup
	for _, replay := range ready {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			replay(ctx)
		})
	}
	wg.Wait()
}

func (q *blockArrivalQueue) removeLocked(root common.Hash) []func(context.Context) {
	pending := q.byRoot[root]
	delete(q.byRoot, root)
	q.replays -= len(pending.replays)
	return pending.replays
}
