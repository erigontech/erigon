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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// OnTick executes on_tick operation for forkchoice.
func (f *ForkChoiceStore) OnTick(time uint64) {
	tickSlot := (time - f.genesisTime) / f.beaconCfg.SecondsPerSlot
	for f.Slot() < tickSlot {
		previousTime := f.genesisTime + (f.Slot()+1)*f.beaconCfg.SecondsPerSlot
		f.onTickPerSlot(previousTime)
	}
	f.onTickPerSlot(time)
}

// onTickPerSlot handles ticks
func (f *ForkChoiceStore) onTickPerSlot(time uint64) {
	previousSlot := f.Slot()
	f.time.Store(time)
	currentSlot := f.Slot()
	if currentSlot <= previousSlot {
		return
	}
	f.mu.Lock()
	f.headHash = common.Hash{}
	f.mu.Unlock()
	// If this is a new slot, reset store.proposer_boost_root
	f.proposerBoostRoot.Store(common.Hash{})
	if f.computeSlotsSinceEpochStart(currentSlot) == 0 {
		f.updateCheckpoints(f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint), f.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint))
	}
}
