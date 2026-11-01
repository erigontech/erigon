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

package sequencer

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
)

// Event is a trigger of the schedule.
type Event interface{ isEvent() }

// SyncSlot asks for the rest of the slot whose Sync block is at Height with
// timestamp Time, anchored to L1 block L1Head; the slot's batch targets the
// L1 block after it.
type SyncSlot struct {
	Height, Time, L1Head uint64
}

// LiveTick asks for one Live block while the head is below the Live blocks'
// end of the slot whose Sync block is at SyncHeight.
type LiveTick struct {
	SyncHeight uint64
}

func (SyncSlot) isEvent() {}
func (LiveTick) isEvent() {}

// Schedule turns L1 heads into Sync-slot triggers. Each head anchors the
// slot whose Sync block has the timestamp of the next L1 block, and arms its
// trigger at the head's timestamp plus ProofWindowOpen; a newer head
// supersedes an armed trigger.
type Schedule struct {
	timing Timing
	// genesis is the timestamp of the L2 genesis block.
	genesis uint64
	pending *SyncSlot
	at      time.Time
	// height is the Sync height of the latest anchor, valid once anchored.
	height   uint64
	anchored bool
}

// NewSchedule returns the schedule of a chain whose genesis has timestamp
// genesis.
func NewSchedule(t Timing, genesis uint64) *Schedule {
	return &Schedule{timing: t, genesis: genesis}
}

// Head anchors a slot to h and returns the trigger it supersedes, if any.
func (s *Schedule) Head(h l1.Header) (superseded *SyncSlot) {
	ts := h.Time + uint64(s.timing.L1/time.Second)
	slot := &SyncSlot{Height: (ts - min(ts, s.genesis)) / uint64(s.timing.L2/time.Second), Time: ts, L1Head: h.Number}
	superseded, s.pending = s.pending, slot
	s.at = time.Unix(int64(h.Time), 0).Add(s.timing.ProofWindowOpen())
	s.height, s.anchored = slot.Height, true
	return superseded
}

// Next returns the instant the armed trigger fires at.
func (s *Schedule) Next() (time.Time, bool) { return s.at, s.pending != nil }

// Fire disarms the trigger and returns it.
func (s *Schedule) Fire() SyncSlot {
	slot := *s.pending
	s.pending = nil
	return slot
}

// Tick returns the Live tick of the latest anchor; there is none before the
// first, so ticks cannot outrun it.
func (s *Schedule) Tick() (LiveTick, bool) { return LiveTick{SyncHeight: s.height}, s.anchored }

// Run feeds the L1 heads of heads to s and sends its triggers to out until
// ctx ends or heads closes: each Sync slot when its instant passes, and a
// Live tick every L2 interval once anchored. A Live tick is dropped while
// out is full; the next one nudges again.
func (s *Schedule) Run(ctx context.Context, heads <-chan l1.Header, out chan<- Event, logger log.Logger) {
	ticker := time.NewTicker(s.timing.L2)
	defer ticker.Stop()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		timer.Stop()
		var fire <-chan time.Time
		if at, ok := s.Next(); ok {
			timer.Reset(time.Until(at))
			fire = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case h, ok := <-heads:
			if !ok {
				logger.Error("[eez] L1 head source closed, sequencer schedule stops", "event", "eez.slot.l1_anchored.source_closed")
				return
			}
			if stale := s.Head(h); stale != nil {
				logger.Warn("[eez] L1 head before the armed Sync slot fired, superseding it", "event", "eez.slot.l1_anchored.trigger_superseded",
					"staleHeight", stale.Height, "height", s.pending.Height, "l1Head", h.Number)
			}
		case <-fire:
			slot := s.Fire()
			logger.Debug("[eez] Sync slot trigger", "event", "eez.slot.l1_anchored.trigger", "l1Head", slot.L1Head, "height", slot.Height, "time", slot.Time)
			select {
			case out <- slot:
			case <-ctx.Done():
				return
			}
		case <-ticker.C:
			tick, ok := s.Tick()
			if !ok {
				continue
			}
			select {
			case out <- tick:
			default:
				logger.Debug("[eez] sequencer busy, Live tick skipped", "event", "eez.slot.l1_anchored.live_tick_skipped", "syncHeight", tick.SyncHeight)
			}
		}
	}
}
