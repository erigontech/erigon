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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// BlockBuilderFunc builds a payload. Its context ends when the payload is discarded, so anything
// that can block - opening a read view, waiting on a transaction provider - has to honour it.
type BlockBuilderFunc func(ctx context.Context, param *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error)

type InclusionListFunc func(ctx context.Context) (types.Transactions, error)

// ErrDiscarded reports that the payload was abandoned before the build completed.
var ErrDiscarded = errors.New("block builder discarded")

// rebuildInterval is how long each rebuild of a BlockBuilder with a fallback payload keeps
// collecting transactions before it seals its payload.
const rebuildInterval = time.Second

type stoppedKey struct{}

// stoppedOf returns the channel closed when the running build is asked to wrap up, nil for a build
// that runs without one.
func stoppedOf(ctx context.Context) <-chan struct{} {
	stopped, _ := ctx.Value(stoppedKey{}).(chan struct{})
	return stopped
}

// BuildEmpty builds the payload without transactions.
func BuildEmpty(ctx context.Context, build BlockBuilderFunc, param *Parameters) (*types.BlockWithReceipts, error) {
	emptyParam := param.Copy()
	emptyParam.CustomTxnProvider = noTxnProvider{}
	var interrupt atomic.Bool
	interrupt.Store(true)
	return build(ctx, emptyParam, &interrupt)
}

type noTxnProvider struct{}

func (noTxnProvider) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

// BlockValue computes the expected value received by the fee recipient in wei.
func BlockValue(br *types.BlockWithReceipts) *uint256.Int {
	baseFee := br.Block.HeaderNoCopy().BaseFee
	value := uint256.NewInt(0)
	txs := br.Block.Transactions()
	var gas, txValue uint256.Int
	for i := range txs {
		gas.SetUint64(br.Receipts[i].GasUsed)
		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)
		txValue.Mul(&gas, &effectiveTip)
		value.Add(value, &txValue)
	}
	return value
}

const (
	blockBuilderRunning uint32 = iota
	blockBuilderCompleted
	blockBuilderDiscarded
)

// BlockBuilder wraps a goroutine that builds Proof-of-Stake payloads (PoS "mining").
//
// Started with a fallback payload, it rebuilds the payload every rebuildInterval and keeps the most
// valuable one sealed, so Resolve hands it out at once. Started without one, it builds once and
// Resolve waits for that build.
//
// It answers to two different requests. Stopping asks for the payload it has, which is how a
// payload is collected. Discarding says the payload is not wanted at all, and cancels the work.
type BlockBuilder struct {
	interrupt atomic.Bool
	stopped   chan struct{}
	stopOnce  sync.Once
	state     atomic.Uint32
	discard   context.CancelFunc
	rebuild   bool
	mu        sync.Mutex
	done      chan struct{}
	result    *types.BlockWithReceipts
	err       error
}

// NewBlockBuilder starts a build. fallback, when set, is the payload handed out until a rebuild
// seals a better one. maxBuildTime is the budget after which the builder stops itself and keeps the
// payload it has; stopGrace is how long a stopped build may take to finish up - the transaction in
// flight, the packing tail, payload finalization - before it is taken as stuck and discarded.
func NewBlockBuilder(ctx context.Context, build BlockBuilderFunc, param *Parameters, fallback *types.BlockWithReceipts, maxBuildTime, stopGrace time.Duration) *BlockBuilder {
	buildCtx, discard := context.WithCancel(ctx)
	builder := &BlockBuilder{
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		discard: discard,
		rebuild: fallback != nil,
		result:  fallback,
	}

	go builder.run(buildCtx, build, param)

	go func() {
		timer := time.NewTimer(maxBuildTime)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-builder.done:
			return
		}
		// Ask for the payload it has, which is what the budget was for. A build that has not
		// answered by the end of the grace is treated as unresponsive and discarded.
		log.Warn("Stopping block builder due to max build time exceeded")
		graceCtx, cancelGrace := context.WithTimeout(ctx, stopGrace)
		defer cancelGrace()
		_, err := builder.Stop(graceCtx)
		if err == nil {
			log.Debug("Stopped block builder due to max build time exceeded")
			return
		}
		if errors.Is(err, ErrDiscarded) || builder.finished() {
			return
		}
		builder.Discard()
		log.Warn("Discarded unresponsive block builder due to max build time exceeded")
	}()

	return builder
}

func (b *BlockBuilder) run(ctx context.Context, build BlockBuilderFunc, param *Parameters) {
	defer func() {
		b.state.CompareAndSwap(blockBuilderRunning, blockBuilderCompleted)
		close(b.done)
		b.discard()
	}()

	if !b.rebuild {
		result, err := buildPayload(context.WithValue(ctx, stoppedKey{}, b.stopped), build, param, &b.interrupt)
		b.mu.Lock()
		b.result, b.err = result, err
		b.state.CompareAndSwap(blockBuilderRunning, blockBuilderCompleted)
		b.mu.Unlock()
		return
	}

	for first := true; ; first = false {
		roundCtx, interrupt, roundStopped := b.startRound(ctx, first)
		result, err := buildPayload(roundCtx, build, param, interrupt)
		if err != nil {
			return
		}
		b.offer(result)
		if !first {
			<-roundStopped
		}
		select {
		case <-b.stopped:
			return
		case <-ctx.Done():
			return
		default:
		}
	}
}

// startRound prepares one rebuild, which wraps up once rebuildInterval passes or the builder is
// stopped. The first round wraps up as soon as it has drained the transaction provider.
func (b *BlockBuilder) startRound(ctx context.Context, first bool) (context.Context, *atomic.Bool, <-chan struct{}) {
	interrupt := new(atomic.Bool)
	interrupt.Store(first)
	stopped := make(chan struct{})
	go func() {
		timer := time.NewTimer(rebuildInterval)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-b.stopped:
		case <-ctx.Done():
		}
		interrupt.Store(true)
		close(stopped)
	}()
	return context.WithValue(ctx, stoppedKey{}, stopped), interrupt, stopped
}

func (b *BlockBuilder) offer(result *types.BlockWithReceipts) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.result == nil || BlockValue(result).Cmp(BlockValue(b.result)) >= 0 {
		b.result = result
	}
}

func buildPayload(ctx context.Context, build BlockBuilderFunc, param *Parameters, interrupt *atomic.Bool) (result *types.BlockWithReceipts, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("block builder panic: %+v, trace: %s", rec, dbg.Stack())
			log.Warn("Block builder panicked", "err", err)
			result = nil
		}
	}()

	log.Info("Building block...")
	t := time.Now()
	result, err = build(ctx, param, interrupt)
	if err != nil {
		if ctx.Err() != nil {
			log.Debug("Block builder discarded", "err", err)
		} else {
			log.Warn("Failed to build a block", "err", err)
		}
		return nil, err
	}
	block := result.Block
	log.Info("Built block", "hash", block.Hash(), "height", block.NumberU64(), "txs", len(block.Transactions()), "executionRequests", len(result.Requests), "gasUsedPct", 100*float64(block.GasUsed())/float64(block.GasLimit()), "time", time.Since(t))
	return result, nil
}

func (b *BlockBuilder) stop() {
	b.interrupt.Store(true)
	b.stopOnce.Do(func() { close(b.stopped) })
}

// Resolve stops the builder and returns the payload it holds. A builder with a fallback payload
// answers at once with the most valuable payload sealed so far; one without waits for its build.
func (b *BlockBuilder) Resolve(ctx context.Context) (*types.BlockWithReceipts, error) {
	if b.rebuild {
		b.stop()
		return b.readResult()
	}
	return b.Stop(ctx)
}

// Stop stops the builder and waits for its build to finish, returning the final payload.
func (b *BlockBuilder) Stop(ctx context.Context) (*types.BlockWithReceipts, error) {
	b.stop()
	if b.Discarded() {
		return b.readResult()
	}

	select {
	case <-b.done:
	case <-ctx.Done():
		// Completion may become ready while select chooses, so recheck before abandoning the result.
		select {
		case <-b.done:
		default:
			return nil, ctx.Err()
		}
	}
	return b.readResult()
}

func (b *BlockBuilder) readResult() (*types.BlockWithReceipts, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.Discarded() && (b.result == nil || b.err != nil) {
		return nil, ErrDiscarded
	}
	return b.result, b.err
}

func (b *BlockBuilder) finished() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

// Discard cancels the build without waiting for it to return.
func (b *BlockBuilder) Discard() {
	b.interrupt.Store(true)
	b.state.CompareAndSwap(blockBuilderRunning, blockBuilderDiscarded)
	b.discard()
}

// Discarded reports whether discard won before the build completed.
func (b *BlockBuilder) Discarded() bool {
	return b.state.Load() == blockBuilderDiscarded
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
	result, err := b.readResult()
	if err != nil || result == nil {
		return nil
	}
	return result.Block
}
