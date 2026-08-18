// Copyright 2024 The Erigon Authors
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

package builder

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
)

// BlockBuilderFunc builds a payload. Its context ends when the payload is discarded, so anything
// that can block - opening a read view, waiting on a transaction provider - has to honour it.
type BlockBuilderFunc func(ctx context.Context, param *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error)

// BlockBuilder wraps a goroutine that builds Proof-of-Stake payloads (PoS "mining").
//
// It answers to two different requests. Interrupting asks for the block it has so far, which is how
// a payload is collected. Discarding says the payload is not wanted at all, and cancels the work.
type BlockBuilder struct {
	interrupt atomic.Bool
	discarded atomic.Bool
	discard   context.CancelFunc
	mu        sync.Mutex
	done      chan struct{}
	result    *types.BlockWithReceipts
	err       error
}

// NewBlockBuilder starts a build. maxBuildTime is the budget after which the builder stops itself
// and keeps the block it has; stopGrace is how long a stopped build may take to finish up - the
// transaction in flight, the packing tail, payload finalization - before it is taken as stuck and
// discarded.
func NewBlockBuilder(ctx context.Context, build BlockBuilderFunc, param *Parameters, maxBuildTime, stopGrace time.Duration) *BlockBuilder {
	buildCtx, discard := context.WithCancel(ctx)
	builder := &BlockBuilder{done: make(chan struct{}), discard: discard}

	go func() {
		var result *types.BlockWithReceipts
		var err error

		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("block builder panic: %+v, trace: %s", rec, dbg.Stack())
				log.Warn("Block builder panicked", "err", err)
				result = nil
			}

			builder.mu.Lock()
			builder.result = result
			builder.err = err
			builder.mu.Unlock()
			close(builder.done)
			discard()
		}()

		log.Info("Building block...")
		t := time.Now()
		result, err = build(buildCtx, param, &builder.interrupt)
		if err != nil {
			if buildCtx.Err() != nil {
				log.Debug("Block builder discarded", "err", err)
			} else {
				log.Warn("Failed to build a block", "err", err)
			}
		} else {
			block := result.Block
			log.Info("Built block", "hash", block.Hash(), "height", block.NumberU64(), "txs", len(block.Transactions()), "executionRequests", len(result.Requests), "gasUsedPct", 100*float64(block.GasUsed())/float64(block.GasLimit()), "time", time.Since(t))
		}
	}()

	go func() {
		timer := time.NewTimer(maxBuildTime)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-builder.done:
			return
		}
		// Ask for the block it has, which is what the budget was for. A build that has not answered
		// by the end of the grace is treated as unresponsive and discarded.
		log.Warn("Stopping block builder due to max build time exceeded")
		graceCtx, cancelGrace := context.WithTimeout(ctx, stopGrace)
		defer cancelGrace()
		if _, err := builder.Stop(graceCtx); err != nil {
			select {
			case <-builder.done:
				// The build ended within the grace with an error of its own, logged where it happened.
			default:
				builder.Discard()
				log.Warn("Discarded unresponsive block builder due to max build time exceeded")
			}
			return
		}
		log.Debug("Stopped block builder due to max build time exceeded")
	}()

	return builder
}

func (b *BlockBuilder) Stop(ctx context.Context) (*types.BlockWithReceipts, error) {
	b.interrupt.Store(true)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return b.result, b.err
}

// Discard marks the build unusable and cancels its context. It does not wait for the work to unwind.
func (b *BlockBuilder) Discard() {
	b.interrupt.Store(true)
	b.discarded.Store(true)
	b.discard()
}

// Discarded reports the payload was abandoned. The work may still be winding down, but nothing will
// come of it, so a discarded builder reads as gone at once.
func (b *BlockBuilder) Discarded() bool {
	return b.discarded.Load()
}

// Failed reports whether the builder has finished and ended in an error, which a caller looking to
// reuse it has to read as absent because that error is latched.
func (b *BlockBuilder) Failed() bool {
	select {
	case <-b.done:
	default:
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err != nil
}

func (b *BlockBuilder) Block() *types.Block {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.result == nil {
		return nil
	}
	return b.result.Block
}
