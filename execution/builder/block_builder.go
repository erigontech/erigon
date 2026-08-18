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

// buildStopGrace bounds how long a builder may ignore a stop request. Once acknowledged, its
// packing tail and payload finalization are allowed to finish.
const buildStopGrace = 500 * time.Millisecond

// BlockBuilderFunc builds a payload. Its context ends when the payload is discarded, so anything
// that can block - opening a read view, waiting on a transaction provider - has to honour it.
// acknowledgeStop marks that the interrupt was observed or transaction packing has already ended.
type BlockBuilderFunc func(ctx context.Context, param *Parameters, interrupt *atomic.Bool, acknowledgeStop func()) (*types.BlockWithReceipts, error)

// BlockBuilder wraps a goroutine that builds Proof-of-Stake payloads (PoS "mining").
//
// It answers to two different requests. Interrupting asks for the block it has so far, which is how
// a payload is collected. Discarding says the payload is not wanted at all, and cancels the work.
type BlockBuilder struct {
	interrupt        atomic.Bool
	discard          context.CancelFunc
	mu               sync.Mutex
	done             chan struct{}
	result           *types.BlockWithReceipts
	err              error
	stopAcknowledged bool
}

func NewBlockBuilder(ctx context.Context, build BlockBuilderFunc, param *Parameters, maxBuildTime time.Duration) *BlockBuilder {
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
		result, err = build(buildCtx, param, &builder.interrupt, builder.acknowledgeStop)
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
		// Ask for the block it has, which is what the budget was for, but do not wait on it
		// indefinitely: a build parked somewhere that never reads the flag would hold its read view
		// until the builder count forced it out, which on a quiet node is a very long time.
		log.Warn("Stopping block builder due to max build time exceeded")
		graceCtx, cancelGrace := context.WithTimeout(ctx, buildStopGrace)
		defer cancelGrace()
		if _, err := builder.Stop(graceCtx); err != nil {
			if builder.discardIfUnresponsive() {
				log.Debug("Discarded block builder due to max build time exceeded")
			} else {
				log.Debug("Block builder acknowledged stop after max build time exceeded")
			}
			return
		}
		log.Debug("Stopped block builder due to max build time exceeded")
	}()

	return builder
}

func (b *BlockBuilder) acknowledgeStop() {
	b.mu.Lock()
	b.stopAcknowledged = true
	b.mu.Unlock()
}

func (b *BlockBuilder) discardIfUnresponsive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopAcknowledged {
		return false
	}
	b.discard()
	return true
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

// Discard abandons the build and releases what it holds. A read view or a transaction provider
// blocked on the builder's context returns at once instead of waiting out its own deadline, which
// is the difference between an evicted builder freeing its resources now and freeing them a slot
// from now.
func (b *BlockBuilder) Discard() {
	b.interrupt.Store(true)
	b.discard()
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
