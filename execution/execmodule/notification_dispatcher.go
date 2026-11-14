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

package execmodule

import (
	"cmp"
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/protocol/misc"
	sharedreceipts "github.com/erigontech/erigon/execution/receipts"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// Accumulation re-exports notifications.Accumulation for use within execmodule.
type Accumulation = notifications.Accumulation

// NewAccumulation creates a new Accumulation.
func NewAccumulation() *Accumulation {
	return notifications.NewAccumulation()
}

// Dispatcher sends state-change notifications to subscribers.
// Shared between the DevP2P StageLoop path (via Hook) and the Engine API path
// (via PipelineExecutor).
//
// Key design: reads block metadata from a kv.Tx which can be either the SD's
// blockOverlay (before commit) or a committed DB tx. The state version is
// supplied separately because only the durable state flush owns that value.
type Dispatcher struct {
	chainConfig         *chain.Config
	events              *shards.Events
	stateChangeConsumer notifications.StateChangeConsumer
	loadReceipts        ReceiptsLoader
	logger              log.Logger
}

// ReceiptsLoader regenerates a committed block's receipts for a block missing from RecentReceipts.
type ReceiptsLoader func(ctx context.Context, tx kv.TemporalTx, blockNum uint64) (notifications.BlockReceipts, error)

// NewReceiptsLoader regenerates a block's receipts through getter (the receipt cache first, a replay
// otherwise) and fills the fields a replay or the cache may leave empty.
func NewReceiptsLoader(cfg *chain.Config, blockReader dbservices.FullBlockReader, getter eth.ReceiptsGetter) ReceiptsLoader {
	return func(ctx context.Context, tx kv.TemporalTx, blockNum uint64) (notifications.BlockReceipts, error) {
		hash, ok, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return notifications.BlockReceipts{}, err
		}
		if !ok {
			return notifications.BlockReceipts{}, fmt.Errorf("no canonical block %d", blockNum)
		}
		block, _, err := blockReader.BlockWithSenders(ctx, tx, hash, blockNum)
		if err != nil {
			return notifications.BlockReceipts{}, err
		}
		if block == nil {
			return notifications.BlockReceipts{}, fmt.Errorf("block %d not found", blockNum)
		}
		receipts, err := getter.GetReceipts(ctx, cfg, tx, block, eth.ReceiptsOpts{})
		if err != nil {
			return notifications.BlockReceipts{}, err
		}
		sharedreceipts.DeriveFields(receipts, hash)
		return notifications.BlockReceipts{Receipts: receipts, Txs: block.Transactions(), Header: block.Header()}, nil
	}
}

func NewDispatcher(
	chainConfig *chain.Config,
	events *shards.Events,
	stateChangeConsumer notifications.StateChangeConsumer,
	loadReceipts ReceiptsLoader,
	logger log.Logger,
) *Dispatcher {
	return &Dispatcher{
		chainConfig:         chainConfig,
		events:              events,
		stateChangeConsumer: stateChangeConsumer,
		loadReceipts:        loadReceipts,
		logger:              logger,
	}
}

// Dispatch sends all pending notifications. The tx parameter is the data source
// for headers and forkchoice markers — it can be the SD's blockOverlay
// (MemoryMutation) for pre-commit dispatch, or a committed DB tx.
//
// Parameters:
//   - ctx: context for cancellation
//   - tx: data source (overlay or committed tx)
//   - stateVersion: durable version represented by the accumulated state changes
//   - accumulator: state change accumulator (may be nil)
//   - recentReceipts: receipt/log cache (may be nil)
//   - finishProgressBefore: Finish stage progress before the sync run
//   - finishProgressAfter: Finish stage progress after the sync run
//   - prevUnwindPoint: unwind point of this run, from the FCU or pipeline (may be nil)
//   - committedTx: committed state holding every block of the run, for blocks missing from recentReceipts
//     (nil when the blocks are not committed yet)
func (d *Dispatcher) Dispatch(
	ctx context.Context,
	tx kv.Tx,
	stateVersion uint64,
	accumulator *notifications.Accumulator,
	recentReceipts *notifications.RecentReceipts,
	finishProgressBefore uint64,
	finishProgressAfter uint64,
	prevUnwindPoint *uint64,
	committedTx kv.TemporalTx,
) error {
	if accumulator != nil {
		accumulator.SetStateID(stateVersion)
	}

	if d.events != nil {
		base := finishProgressBefore
		if prevUnwindPoint != nil && *prevUnwindPoint < finishProgressBefore {
			base = *prevUnwindPoint
		}
		var notifyFrom uint64
		switch {
		case finishProgressAfter == 0:
			// Genesis (block 0): notify from block 0.
			notifyFrom = 0
		default:
			// finishProgressAfter can trail finishProgressBefore, and a wrapped
			// span would clamp to 1024 and notify over a range that never ran.
			span, underflow := math.SafeSub(finishProgressAfter, base)
			if underflow {
				span = 0
			}
			notifyFrom = finishProgressAfter - min(span, notifications.MaxNotifiedBlocks) + 1
		}
		notifyTo := finishProgressAfter + 1 //[from, to)

		if err := stagedsync.NotifyNewHeaders(ctx, notifyFrom, notifyTo, d.events, tx, d.logger); err != nil {
			return err
		}
		if recentReceipts != nil {
			d.cacheMissingReceipts(ctx, committedTx, recentReceipts, notifyFrom, notifyTo)
			recentReceipts.NotifyReceipts(d.events, notifyFrom, notifyTo)
			recentReceipts.NotifyLogs(d.events, notifyFrom, notifyTo)
			if !d.events.HasReceiptOrLogSubscribers() {
				recentReceipts.DropUpTo(rawdb.ReadForkchoiceFinalizedNum(tx)) // a reorg cannot reach these, and a later subscriber needs removed logs only for blocks above them
			}
		}
	}

	currentHeader := rawdb.ReadCurrentHeader(tx)
	if accumulator != nil && currentHeader != nil {
		if changes := accumulator.Changes(); len(changes) == 0 || changes[len(changes)-1].BlockHeight < currentHeader.Number.Uint64() {
			accumulator.StartChange(currentHeader, nil, false)
		}

		pendingBaseFee := misc.CalcBaseFee(d.chainConfig, currentHeader)
		pendingBlobFee := d.chainConfig.GetMinBlobGasPrice()
		if currentHeader.ExcessBlobGas != nil {
			nextBlockTime := currentHeader.Time + d.chainConfig.SecondsPerSlot()
			excessBlobGas := misc.CalcExcessBlobGas(d.chainConfig, currentHeader, nextBlockTime)
			f, err := misc.GetBlobGasPrice(d.chainConfig, excessBlobGas, nextBlockTime)
			if err != nil {
				return err
			}
			pendingBlobFee = f.Uint64()
		}

		finalizedBlock := common.Deref(rawdb.ReadHeaderNumber(tx, rawdb.ReadForkchoiceFinalized(tx)))

		accumulator.SendAndReset(ctx, d.stateChangeConsumer, pendingBaseFee.Uint64(), pendingBlobFee, currentHeader.GasLimit, finalizedBlock)
	}
	return nil
}

// cacheMissingReceipts regenerates from committedTx the receipts of the blocks in [from,to) the cache lacks.
func (d *Dispatcher) cacheMissingReceipts(ctx context.Context, committedTx kv.TemporalTx, recentReceipts *notifications.RecentReceipts, from, to uint64) {
	if committedTx == nil || d.loadReceipts == nil || !d.events.HasReceiptOrLogSubscribers() {
		return
	}
	var failed []uint64
	var firstErr error
	for _, blockNum := range recentReceipts.Missing(from, to) {
		b, err := d.loadReceipts(ctx, committedTx, blockNum)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failed, firstErr = append(failed, blockNum), cmp.Or(firstErr, err)
			continue
		}
		recentReceipts.Add(b.Receipts, b.Txs, b.Header)
	}
	if len(failed) > 0 {
		d.logger.Warn("[dispatcher] blocks notified without receipts", "count", len(failed), "first", failed[0], "err", firstErr)
	}
}

// PublishOverlay sends the SharedDomains to in-process subscribers via Events.
// The SD holds both block overlay (table data) and domain state.
func (d *Dispatcher) PublishOverlay(sd *execctx.SharedDomains) {
	if d.events != nil {
		d.logger.Debug("[dispatcher] PublishOverlay: publishing", "sdNil", sd == nil)
		d.events.PublishOverlay(sd)
	}
}
