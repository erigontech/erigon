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
	"errors"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	// blockTimelineSlots is how many slots behind the current one a block is still tracked. A block of the next slot
	// is tracked too, as gossip accepts it early within the clock disparity.
	blockTimelineSlots       = 2
	maxPendingBlockTimelines = 64
	blockTimelineBuffer      = 64
)

// blockTimelines logs one debug line per block of a recent slot that the block service imports, with the time since
// the slot start at which the block arrived on gossip and went through its import attempts. Locally published blocks
// are imported by the beacon API and have no line. A separate goroutine writes the lines and a full buffer drops them,
// so a slow log file never delays an import.
type blockTimelines struct {
	clock  eth_clock.EthereumClock
	logger log.Logger

	mu sync.Mutex
	// Keyed by proposer and slot, which the gossip path has without hashing the block.
	pending map[proposerIndexAndSlot]*blockTimeline
	lines   chan blockTimeline
}

type blockTimeline struct {
	slot, number                             uint64
	root                                     common.Hash
	blobs                                    int
	seen, firstImport, importStart, imported time.Time
	attempts                                 int
	waitedForColumns                         bool
}

func newBlockTimelines(clock eth_clock.EthereumClock, logger log.Logger) *blockTimelines {
	return &blockTimelines{
		clock:   clock,
		logger:  logger,
		pending: make(map[proposerIndexAndSlot]*blockTimeline),
		lines:   make(chan blockTimeline, blockTimelineBuffer),
	}
}

func (t *blockTimelines) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case line := <-t.lines:
			t.write(line)
		}
	}
}

func (t *blockTimelines) write(line blockTimeline) {
	slotStart := t.clock.GetSlotTime(line.slot)
	t.logger.Info("[Caplin] Block import timeline",
		"slot", line.slot,
		"root", line.root,
		"number", line.number,
		"blobs", line.blobs,
		"seenMs", msSince(slotStart, line.seen),
		"firstImportMs", msSince(slotStart, line.firstImport),
		"importStartMs", msSince(slotStart, line.importStart),
		"importedMs", msSince(slotStart, line.imported),
		"attempts", line.attempts,
		"waitedForColumns", line.waitedForColumns,
	)
}

func msSince(slotStart, t time.Time) any {
	if t.IsZero() {
		return "-"
	}
	return t.Sub(slotStart).Milliseconds()
}

// seen records when the block arrived on gossip. Callers record it only after the block signature is verified, as
// the entry is shared by the blocks of one proposer and slot.
func (t *blockTimelines) seen(block *cltypes.SignedBeaconBlock, now time.Time) {
	t.update(block, func(line *blockTimeline) {
		if line.seen.IsZero() {
			line.seen = now
		}
	})
}

// importAttempt records one fork choice import of the block and queues its line once an attempt succeeds.
func (t *blockTimelines) importAttempt(block *cltypes.SignedBeaconBlock, root common.Hash, start, end time.Time, err error) {
	t.update(block, func(line *blockTimeline) {
		line.attempts++
		if line.firstImport.IsZero() {
			line.firstImport = start
		}
		if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			line.waitedForColumns = true
		}
		if err != nil {
			return
		}
		line.root, line.importStart, line.imported = root, start, end
		if body := block.Block.Body; body != nil {
			if body.ExecutionPayload != nil {
				line.number = body.ExecutionPayload.BlockNumber
			}
			if commitments := body.GetBlobKzgCommitments(); commitments != nil {
				line.blobs = commitments.Len()
			}
		}
		delete(t.pending, blockGossipKey(block))
		select {
		case t.lines <- *line:
		default:
		}
	})
}

func (t *blockTimelines) update(block *cltypes.SignedBeaconBlock, apply func(*blockTimeline)) {
	if t == nil || !t.logger.Enabled(context.Background(), log.LvlInfo) {
		return
	}
	currentSlot := t.clock.GetCurrentSlot()
	if !timelineSlotTracked(block.Block.Slot, currentSlot) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := blockGossipKey(block)
	line, ok := t.pending[key]
	if !ok {
		for pendingKey, pending := range t.pending {
			if !timelineSlotTracked(pending.slot, currentSlot) {
				delete(t.pending, pendingKey)
			}
		}
		if len(t.pending) >= maxPendingBlockTimelines {
			return
		}
		line = &blockTimeline{slot: block.Block.Slot}
		t.pending[key] = line
	}
	apply(line)
}

func timelineSlotTracked(slot, currentSlot uint64) bool {
	if slot > currentSlot {
		return slot-currentSlot <= 1
	}
	return currentSlot-slot <= blockTimelineSlots
}
