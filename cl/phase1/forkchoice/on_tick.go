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

package forkchoice

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

// OnTick executes on_tick for a Unix time in milliseconds.
func (f *ForkChoiceStore) OnTick(timeMs uint64) {
	if timeMs <= f.timeMs.Load() {
		return
	}
	tickSlot := f.slotAtTimeMs(timeMs)
	for currentSlot := f.Slot(); currentSlot < tickSlot; currentSlot = f.Slot() {
		nextSlotTimeMs := utils.SecondsToMilliseconds(f.genesisTime + (currentSlot+1)*f.beaconCfg.SecondsPerSlot)
		f.onTickPerSlot(nextSlotTimeMs)
	}
	f.onTickPerSlot(timeMs)
}

// onTickPerSlot implements the spec's on_tick_per_slot (consensus-specs/fork_choice).
func (f *ForkChoiceStore) onTickPerSlot(timeMs uint64) {
	previousTimeMs := f.timeMs.Load()
	for {
		if timeMs <= previousTimeMs {
			return
		}
		if f.timeMs.CompareAndSwap(previousTimeMs, timeMs) {
			break
		}
		previousTimeMs = f.timeMs.Load()
	}
	currentSlot := f.slotAtTimeMs(timeMs)
	if currentSlot <= f.slotAtTimeMs(previousTimeMs) {
		return
	}
	f.mu.Lock()
	defer f.drainQueuedWork()
	defer f.mu.Unlock()
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending
	f.proposerBoostRoot.Store(common.Hash{})
	if f.computeSlotsSinceEpochStart(currentSlot) == 0 {
		// Only promote unrealized checkpoints when the node is synced (processing
		// blocks at the chain tip).  During forward sync, the highest-seen slot lags
		// far behind the wall clock.  Promoting unrealized finalization at that point
		// can jump the realized finalized checkpoint ahead of the blocks the fork
		// graph actually contains, causing Ancestor() to fail with
		// ErrNotFinalizedDescendant for subsequent blocks.
		if f.highestSeen.Load()+2*f.beaconCfg.SlotsPerEpoch >= currentSlot {
			f.updateCheckpoints(f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint), f.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint))
		}
	}
}
