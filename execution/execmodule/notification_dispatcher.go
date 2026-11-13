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
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
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
// Block metadata comes from the SD's block overlay or a committed database transaction.
// The domain flush advances the state version, so callers supply it separately.
// Before commit, notifications carry the projected version of that flush.
type Dispatcher struct {
	chainConfig         *chain.Config
	headerReader        dbservices.HeaderReader
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
	headerReader dbservices.HeaderReader,
	events *shards.Events,
	stateChangeConsumer notifications.StateChangeConsumer,
	loadReceipts ReceiptsLoader,
	logger log.Logger,
) *Dispatcher {
	return &Dispatcher{
		chainConfig:         chainConfig,
		headerReader:        headerReader,
		events:              events,
		stateChangeConsumer: stateChangeConsumer,
		loadReceipts:        loadReceipts,
		logger:              logger,
	}
}

// StateChangesSnapshot builds a subscription reset from one committed execution view.
func (d *Dispatcher) StateChangesSnapshot(ctx context.Context, db kv.TemporalRoDB) (*remoteproto.StateChangeBatch, error) {
	var result *remoteproto.StateChangeBatch
	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		height, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		version, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return err
		}
		accumulator := notifications.NewAccumulator()
		accumulator.MarkIncomplete()
		batch, err := d.prepareStateChanges(ctx, tx, tx, version, height, accumulator)
		if err != nil {
			return err
		}
		if batch == nil {
			return fmt.Errorf("executed header %d unavailable for state snapshot", height)
		}
		result = batch.ToProtoBatch()
		return nil
	})
	return result, err
}

// Dispatch sends all pending notifications. The tx parameter is the data source
// for headers and forkchoice markers — it can be the SD's blockOverlay
// (MemoryMutation) for pre-commit dispatch, or a committed DB tx. The state getter
// must include all account writes at the announced head, including uncommitted ones.
//
// Parameters:
//   - ctx: context for cancellation
//   - tx: data source (overlay or committed tx)
//   - state: final state at the announced head
//   - stateVersion: announced state version, which may not yet be committed
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
	state kv.TemporalGetter,
	stateVersion uint64,
	accumulator *notifications.Accumulator,
	recentReceipts *notifications.RecentReceipts,
	finishProgressBefore uint64,
	finishProgressAfter uint64,
	prevUnwindPoint *uint64,
	committedTx kv.TemporalTx,
) error {
	batch, err := d.prepareStateChanges(ctx, tx, state, stateVersion, finishProgressAfter, accumulator)
	if err != nil {
		return err
	}
	var recordedChanges []notifications.StateChange
	if accumulator != nil {
		recordedChanges = accumulator.Changes()
	}

	if d.events != nil {
		base := finishProgressBefore
		if prevUnwindPoint != nil && *prevUnwindPoint < finishProgressBefore {
			base = *prevUnwindPoint
		}
		// Undelivered commits may predate this run. Only recorded changes extend
		// the header range; synthetic head entries do not announce a new block.
		for _, change := range recordedChanges {
			before := change.BlockHeight
			if change.Direction == notifications.DirectionForward {
				before = change.FirstBlockHeight
				if before > 0 {
					before--
				}
			}
			base = min(base, before)
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

	if batch != nil {
		if d.stateChangeConsumer != nil {
			d.stateChangeConsumer.SendStateChanges(ctx, batch.ToProtoBatch())
		}
		accumulator.Reset()
	}
	return nil
}

// Preparation can fail on account reads. Complete it before publishing any events,
// and keep recorded changes intact so a failed dispatch can be retried.
func (d *Dispatcher) prepareStateChanges(ctx context.Context, tx kv.Tx, state kv.TemporalGetter, stateVersion, finishProgress uint64, accumulator *notifications.Accumulator) (*notifications.BlockBatchNotification, error) {
	if accumulator == nil {
		return nil, nil
	}
	// Head markers can lead execution. The announced head must match the supplied state.
	header, err := d.headerReader.HeaderByNumber(ctx, tx, finishProgress)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	batch := accumulator.Batch(header)
	batch.StateVersionID = stateVersion
	batch.PendingBlockBaseFee = misc.CalcBaseFee(d.chainConfig, header).Uint64()
	batch.PendingBlobFee = d.chainConfig.GetMinBlobGasPrice()
	if header.ExcessBlobGas != nil {
		nextBlockTime := header.Time + d.chainConfig.SecondsPerSlot()
		excessBlobGas := misc.CalcExcessBlobGas(d.chainConfig, header, nextBlockTime)
		fee, err := misc.GetBlobGasPrice(d.chainConfig, excessBlobGas, nextBlockTime)
		if err != nil {
			return nil, err
		}
		batch.PendingBlobFee = fee.Uint64()
	}
	batch.BlockGasLimit = header.GasLimit
	batch.FinalizedBlock = common.Deref(rawdb.ReadHeaderNumber(tx, rawdb.ReadForkchoiceFinalized(tx)))
	if err := batch.RefreshAccounts(state); err != nil {
		return nil, err
	}
	return batch, nil
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
