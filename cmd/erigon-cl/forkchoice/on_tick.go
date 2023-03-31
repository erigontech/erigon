package forkchoice

import libcommon "github.com/ledgerwatch/erigon-lib/common"

// OnTick executes on_tick operation for forkchoice.
func (f *ForkChoiceStore) OnTick(time uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tickSlot := (time - f.forkGraph.GenesisTime()) / f.forkGraph.LastState().BeaconConfig().SecondsPerSlot
	for f.Slot() < tickSlot {
		previousTime := f.forkGraph.GenesisTime() + (f.Slot()+1)*f.forkGraph.Config().SecondsPerSlot
		f.onTickPerSlot(previousTime)
	}
	f.onTickPerSlot(time)
}

// onTickPerSlot handles ticks
func (f *ForkChoiceStore) onTickPerSlot(time uint64) {
	previousSlot := f.Slot()
	f.time = time
	currentSlot := f.Slot()
	if currentSlot <= previousSlot {
		return
	}
	// If this is a new slot, reset store.proposer_boost_root
	f.proposerBoostRoot = libcommon.Hash{}
	if f.computeSlotsSinceEpochStart(currentSlot) == 0 {
		f.updateCheckpoints(f.unrealizedJustifiedCheckpoint.Copy(), f.unrealizedFinalizedCheckpoint.Copy())
	}
}
