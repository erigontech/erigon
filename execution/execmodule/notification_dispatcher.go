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
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execobserver"
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

	currentHeader := rawdb.ReadCurrentHeader(tx)
	if accumulator != nil && currentHeader != nil {
		// FCU flush: the txpool must learn the FCU'd block's txs so OnNewBlock removes mined txs and advances
		// canonical nonces (L1 keystone). Read the FCU'd block (finishProgressAfter — the block just committed,
		// which sits BEHIND any ahead change the accumulator may hold) by its CANONICAL hash from the overlay;
		// that is where the real sealed body lives. An ahead change's own block is not canonical yet, so reading
		// by its hash returns an empty body — which is why the change was sent empty.
		//
		// ⚠ THE CANONICAL BODY IS AUTHORITATIVE, NOT A FALLBACK. This used to populate the change only when it
		// was EMPTY, and a change for this height usually is NOT: pre-execution started it with the txs it
		// executed, and what executed is not what sealed — a stopped round commits only the prefix it applied.
		// The pre-executed set then went out as FORWARD and the txpool removed all of it as mined. See
		// SetCanonicalChangeTxs for the measurement; it cost a soak the whole pool.
		// ⚠ THE SET IS FIXED AT THE SEAL; ONLY THE CHOICE IS MADE HERE. A block's transactions are a
		// property of that block and never change once it seals.
		// What this moment decides is which block at each height was CHOSEN, which is a property of the
		// chain. Everything else is a consequence of having conflated the two:
		//
		//   - pre-execution's guess was the default, and a stopped round commits only the prefix it
		//     applied, so the guess routinely held transactions the block did not seal with;
		//   - the content was then re-derived here by reading the body back out of the database, which
		//     is a re-derivation of something already known and can come back EMPTY while the body is
		//     still flushing - and when it did, the guess stood and the txpool deleted all of it;
		//   - and only ONE height was reconciled, which is what you write having never seen an FCU
		//     advance more than one block. Under load it advances several, and every other height in
		//     the batch kept announcing a guess as mined.
		//
		// Now: no body is read, no height is special, and a block that was not chosen says nothing
		// mined. A height whose canonical hash cannot be read is treated as not chosen, because
		// removing too little self-corrects on the next block and removing too much strands a sender.
		replaced, cleared := accumulator.SetCanonicalTxs(func(height uint64) ([][]byte, bool) {
			h, err := rawdb.ReadCanonicalHash(tx, height)
			if err != nil || h == (common.Hash{}) {
				return nil, false
			}
			body, err := rawdb.ReadBodyWithTransactions(tx, h, height)
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
		// Genuinely empty flush (no change for the current head) — keep the empty backfill so consumers still
		// see the head advance. Standard flow: exec already populated the change, so this is a no-op.
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

// PublishOverlay sends the SharedDomains to in-process subscribers via Events.
// The SD holds both block overlay (table data) and domain state.
func (d *Dispatcher) PublishOverlay(sd *execctx.SharedDomains) {
	if d.events != nil {
		d.logger.Debug("[dispatcher] PublishOverlay: publishing", "sdNil", sd == nil)
		d.events.PublishOverlay(sd)
	}
}
