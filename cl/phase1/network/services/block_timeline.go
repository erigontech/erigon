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

	"github.com/erigontech/erigon/cl/clparams"
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
// the slot start at which the block arrived on gossip and went through its import attempts. A separate goroutine
// writes the lines and a full buffer drops them, so a slow log file never delays an import.
type blockTimelines struct {
	clock  eth_clock.EthereumClock
	logger log.Logger

	mu sync.Mutex
	// Keyed by the block signature, which identifies a signed block without hashing it.
	pending map[common.Bytes96]*blockTimeline
	lines   chan blockTimeline
}

type blockTimeline struct {
	slot                                     uint64
	number                                   any // EL block number, or "-" for a block without an execution payload
	root                                     common.Hash
	blobs                                    int
	seen, firstImport, importStart, imported time.Time
	attempts, inFlight                       int
	waitedForColumns                         bool
	// logged keeps the entry of a logged block until its slot leaves the window, so a later import of the same
	// block by another path does not log it again.
	logged bool
}

func newBlockTimelines(clock eth_clock.EthereumClock, logger log.Logger) *blockTimelines {
	return &blockTimelines{
		clock:   clock,
		logger:  logger,
		pending: make(map[common.Bytes96]*blockTimeline),
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
	t.logger.Debug("[Caplin] Block import timeline",
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

// seen records when the block arrived on gossip. Callers record it only after the signature check, as another block
// can carry a copy of the signature.
func (t *blockTimelines) seen(block *cltypes.SignedBeaconBlock, now time.Time) {
	t.update(block, newEntryIfRoom, func(line *blockTimeline) {
		keepEarliest(&line.seen, now)
	})
}

// importStarted records the start of one fork choice import of the block. Imports by different paths can overlap, so
// an attempt counts when it starts, not when it ends.
func (t *blockTimelines) importStarted(block *cltypes.SignedBeaconBlock, start time.Time) {
	t.update(block, newEntryEvictingArrival, func(line *blockTimeline) {
		line.attempts++
		line.inFlight++
		keepEarliest(&line.firstImport, start)
	})
}

// importEnded records the result of an import that started at start, and queues the line of the block once fork
// choice holds it.
func (t *blockTimelines) importEnded(block *cltypes.SignedBeaconBlock, root common.Hash, start, end time.Time, err error) {
	t.update(block, existingEntry, func(line *blockTimeline) {
		line.inFlight--
		if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			line.waitedForColumns = true
		}
		if err != nil {
			return
		}
		line.importStart, line.imported = start, end
		t.queue(line, block, root)
	})
}

// importedElsewhere queues the line of a tracked block that another path put into fork choice, with no import times.
// An import in flight may already have put the block there, and it queues the line with its times itself.
func (t *blockTimelines) importedElsewhere(block *cltypes.SignedBeaconBlock, root common.Hash) {
	t.update(block, existingEntry, func(line *blockTimeline) {
		if line.inFlight == 0 {
			t.queue(line, block, root)
		}
	})
}

func (t *blockTimelines) queue(line *blockTimeline, block *cltypes.SignedBeaconBlock, root common.Hash) {
	line.root = root
	line.number = "-"
	if payload := block.Block.Body.ExecutionPayload; payload != nil && block.Version() >= clparams.BellatrixVersion {
		line.number = payload.BlockNumber
	}
	if commitments := block.GetBlobKzgCommitments(); commitments != nil {
		line.blobs = commitments.Len()
	}
	line.logged = true
	select {
	case t.lines <- *line:
	default:
	}
}

func keepEarliest(current *time.Time, t time.Time) {
	if current.IsZero() || t.Before(*current) {
		*current = t
	}
}

// entryCreation says whether update adds an entry for a block that has none.
type entryCreation uint8

const (
	existingEntry entryCreation = iota
	newEntryIfRoom
	// newEntryEvictingArrival replaces an entry that has only an arrival when the map is full. Arrivals can be
	// recorded before the proposer check and imports only after it, so blocks of other signers cannot keep the
	// imported block out.
	newEntryEvictingArrival
)

func (t *blockTimelines) update(block *cltypes.SignedBeaconBlock, create entryCreation, apply func(*blockTimeline)) {
	if t == nil || !t.logger.Enabled(context.Background(), log.LvlDebug) {
		return
	}
	currentSlot := t.clock.GetCurrentSlot()
	if !timelineSlotTracked(block.Block.Slot, currentSlot) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := block.Signature
	line, ok := t.pending[key]
	if !ok {
		if create == existingEntry {
			return
		}
		for pendingKey, pending := range t.pending {
			if !timelineSlotTracked(pending.slot, currentSlot) {
				delete(t.pending, pendingKey)
			}
		}
		if len(t.pending) >= maxPendingBlockTimelines && (create != newEntryEvictingArrival || !t.evictArrivalLocked()) {
			return
		}
		line = &blockTimeline{slot: block.Block.Slot}
		t.pending[key] = line
	}
	if !line.logged {
		apply(line)
	}
}

func (t *blockTimelines) evictArrivalLocked() bool {
	for key, line := range t.pending {
		if line.attempts == 0 && !line.logged {
			delete(t.pending, key)
			return true
		}
	}
	return false
}

func timelineSlotTracked(slot, currentSlot uint64) bool {
	if slot > currentSlot {
		return slot-currentSlot <= 1
	}
	return currentSlot-slot <= blockTimelineSlots
}
