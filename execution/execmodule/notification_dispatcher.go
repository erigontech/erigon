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
	"bytes"
	"cmp"
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execobserver"
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
			return status.Errorf(codes.Unavailable, "executed header %d unavailable for state snapshot", height)
		}
		result = batch.ToProtoBatch()
		return nil
	})
	return result, err
}

// Dispatch sends all pending notifications for the announced head.
//
// Parameters:
//   - ctx: context for cancellation of reads and notification delivery.
//   - blockTx: block metadata at the announced head, from a block overlay or a committed transaction.
//   - stateReader: account state at the same head, including any unflushed domain writes.
//     A block overlay alone is insufficient: its GetLatest reads only the backing transaction.
//   - stateVersion: state version at the announced head. Before commit, use the projected
//     version that the pending domain flush will persist.
//   - accumulator: pending state changes, including retained changes from earlier runs;
//     nil disables state-change notifications.
//   - recentReceipts: receipt and log cache; nil disables receipt and log notifications.
//   - finishProgressBefore: Finish stage height before this run, used as the initial
//     base for the header notification range.
//   - finishProgressAfter: Finish stage height after this run, identifying the announced head.
//     It may be lower than finishProgressBefore after an unwind.
//   - prevUnwindPoint: unwind target for this run, or nil if none; can lower the notification range's base.
//   - committedTx: committed transaction covering the notified block range, used to load
//     missing receipts. Nil skips that recovery and is required while those blocks are uncommitted.
func (d *Dispatcher) Dispatch(
	ctx context.Context,
	blockTx kv.Tx,
	stateReader kv.TemporalGetter,
	stateVersion uint64,
	accumulator *notifications.Accumulator,
	recentReceipts *notifications.RecentReceipts,
	finishProgressBefore uint64,
	finishProgressAfter uint64,
	prevUnwindPoint *uint64,
	committedTx kv.TemporalTx,
) error {
	if accumulator != nil && rawdb.ReadCurrentHeader(blockTx) != nil {
		replaced, cleared := accumulator.SetCanonicalTxs(func(height uint64) ([][]byte, bool) {
			h, err := rawdb.ReadCanonicalHash(blockTx, height)
			if err != nil || h == (common.Hash{}) {
				return nil, false
			}
			body, err := rawdb.ReadBodyWithTransactions(blockTx, h, height)
			if err != nil || body == nil {
				return nil, false
			}
			out := make([][]byte, 0, len(body.Transactions))
			for _, txn := range body.Transactions {
				var buf bytes.Buffer
				if err := txn.MarshalBinary(&buf); err != nil {
					return nil, false
				}
				out = append(out, buf.Bytes())
			}
			return out, true
		})
		// NEVER CLEAR SILENTLY. A cleared height tells the txpool nothing was mined there, and it
		// never hears about that height again — which is how mined-removal fell to zero unnoticed.
		if cleared > 0 {
			d.logger.Warn("[accumulator] heights announcing no mined transactions",
				"replaced", replaced, "cleared", cleared)
		}
	}
	batch, err := d.prepareStateChanges(ctx, blockTx, stateReader, stateVersion, finishProgressAfter, accumulator)
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

		if err := stagedsync.NotifyNewHeaders(ctx, notifyFrom, notifyTo, d.events, blockTx, d.logger); err != nil {
			return err
		}
		if recentReceipts != nil {
			d.cacheMissingReceipts(ctx, committedTx, recentReceipts, notifyFrom, notifyTo)
			recentReceipts.NotifyReceipts(d.events, notifyFrom, notifyTo)
			recentReceipts.NotifyLogs(d.events, notifyFrom, notifyTo)
			if !d.events.HasReceiptOrLogSubscribers() {
				recentReceipts.DropUpTo(rawdb.ReadForkchoiceFinalizedNum(blockTx)) // a reorg cannot reach these, and a later subscriber needs removed logs only for blocks above them
			}
		}
	}

	// Fork-choice-update boundary: signal custom indexers (execobserver) to flush
	// their staged writes atomically up to the committed head, so an index never
	// reflects a block the chain didn't commit. Gated so the default build is inert.
	if execobserver.HasObservers() && d.chainConfig != nil && d.chainConfig.ChainID != nil {
		isUnwind := prevUnwindPoint != nil && *prevUnwindPoint != 0 && (*prevUnwindPoint) < finishProgressBefore
		execobserver.NotifyCommit(execobserver.CommitEvent{
			ChainID:  d.chainConfig.ChainID.Uint64(),
			Head:     finishProgressAfter,
			IsUnwind: isUnwind,
		})
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
func (d *Dispatcher) prepareStateChanges(ctx context.Context, blockTx kv.Tx, stateReader kv.TemporalGetter, stateVersion, finishProgress uint64, accumulator *notifications.Accumulator) (*notifications.BlockBatchNotification, error) {
	if accumulator == nil {
		return nil, nil
	}
	// Head markers can lead execution. The announced head must match the supplied state.
	header, err := d.headerReader.HeaderByNumber(ctx, blockTx, finishProgress)
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
	batch.FinalizedBlock = common.Deref(rawdb.ReadHeaderNumber(blockTx, rawdb.ReadForkchoiceFinalized(blockTx)))
	if err := batch.RefreshAccounts(stateReader); err != nil {
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
