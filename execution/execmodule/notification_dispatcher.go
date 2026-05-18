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
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/node/shards"
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
// Key design: reads from a kv.Tx which can be either the SD's blockOverlay
// (before commit) or a committed DB tx (legacy path). This decouples
// notification dispatch from commit ordering.
type Dispatcher struct {
	chainConfig         *chain.Config
	events              *shards.Events
	stateChangeConsumer notifications.StateChangeConsumer
	logger              log.Logger
}

func NewDispatcher(
	chainConfig *chain.Config,
	events *shards.Events,
	stateChangeConsumer notifications.StateChangeConsumer,
	logger log.Logger,
) *Dispatcher {
	return &Dispatcher{
		chainConfig:         chainConfig,
		events:              events,
		stateChangeConsumer: stateChangeConsumer,
		logger:              logger,
	}
}

// Dispatch sends all pending notifications. The tx parameter is the data source
// for headers, state version, and forkchoice markers — it can be the SD's
// blockOverlay (MemoryMutation) for pre-commit dispatch, or a committed DB tx.
//
// Parameters:
//   - ctx: context for cancellation
//   - tx: data source (overlay or committed tx)
//   - accumulator: state change accumulator (may be nil)
//   - recentReceipts: receipt/log cache (may be nil)
//   - finishProgressBefore: Finish stage progress before the sync run
//   - finishProgressAfter: Finish stage progress after the sync run
//   - prevUnwindPoint: previous unwind point from the pipeline (may be nil)
func (d *Dispatcher) Dispatch(
	ctx context.Context,
	tx kv.Tx,
	accumulator *notifications.Accumulator,
	recentReceipts *notifications.RecentReceipts,
	finishProgressBefore uint64,
	finishProgressAfter uint64,
	prevUnwindPoint *uint64,
) error {
	// Update the accumulator with the current plain state version so downstream
	// consumers (e.g. state cache) know state has moved on.
	if accumulator != nil {
		plainStateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return err
		}
		accumulator.SetStateID(plainStateVersion)
	}

	if d.events != nil {
		var notifyFrom uint64
		var isUnwind bool
		if prevUnwindPoint != nil && *prevUnwindPoint != 0 && (*prevUnwindPoint) < finishProgressBefore {
			notifyFrom = *prevUnwindPoint + 1 // +1: unwind already reverted *prevUnwindPoint; notify starting from the block after
			isUnwind = true
		} else if finishProgressAfter == 0 {
			// Genesis (block 0): notify from block 0.
			notifyFrom = 0
		} else {
			heightSpan := min(finishProgressAfter-finishProgressBefore, 1024)
			notifyFrom = finishProgressAfter - heightSpan
			notifyFrom++
		}
		notifyTo := finishProgressAfter + 1 //[from, to)

		if err := stagedsync.NotifyNewHeaders(ctx, notifyFrom, notifyTo, d.events, tx, d.logger); err != nil {
			return err
		}
		if recentReceipts != nil {
			recentReceipts.NotifyReceipts(d.events, notifyFrom, notifyTo, isUnwind)
			recentReceipts.NotifyLogs(d.events, notifyFrom, notifyTo, isUnwind)
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

		var finalizedBlock uint64
		if fb := rawdb.ReadHeaderNumber(tx, rawdb.ReadForkchoiceFinalized(tx)); fb != nil {
			finalizedBlock = *fb
		}

		accumulator.SendAndReset(ctx, d.stateChangeConsumer, pendingBaseFee.Uint64(), pendingBlobFee, currentHeader.GasLimit, finalizedBlock)
	}
	return nil
}

// PublishOverlay sends the SharedDomains to in-process subscribers via Events.
// The SD holds both block overlay (table data) and domain state.
func (d *Dispatcher) PublishOverlay(sd *execctx.SharedDomains) {
	if d.events != nil {
		d.logger.Debug("[dispatcher] PublishOverlay: publishing", "sdNil", sd == nil)
		d.events.PublishOverlay(sd)
	}
}
