package forkchoice

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
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
	f.headHash = libcommon.Hash{}
	f.mu.Unlock()
	// If this is a new slot, reset store.proposer_boost_root
	f.proposerBoostRoot.Store(libcommon.Hash{})
	if f.computeSlotsSinceEpochStart(currentSlot) == 0 {
		f.updateCheckpoints(f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint).Copy(), f.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint).Copy())
	}
}
