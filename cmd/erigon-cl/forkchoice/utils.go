package forkchoice

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// Slot calculates the current slot number using the time and genesis slot.
func (f *ForkChoiceStore) Slot() uint64 {
	return f.forkGraph.Config().GenesisSlot + ((f.time - f.forkGraph.GenesisTime()) / f.forkGraph.Config().SecondsPerSlot)
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateCheckpoints(justifiedCheckpoint, finalizedCheckpoint *cltypes.Checkpoint) {
	if justifiedCheckpoint.Epoch > f.justifiedCheckpoint.Epoch {
		f.justifiedCheckpoint = justifiedCheckpoint
	}
	if finalizedCheckpoint.Epoch > f.finalizedCheckpoint.Epoch {
		f.finalizedCheckpoint = finalizedCheckpoint
	}
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateUnrealizedCheckpoints(justifiedCheckpoint, finalizedCheckpoint *cltypes.Checkpoint) {
	if justifiedCheckpoint.Epoch > f.unrealizedJustifiedCheckpoint.Epoch {
		f.unrealizedJustifiedCheckpoint = justifiedCheckpoint
	}
	if finalizedCheckpoint.Epoch > f.unrealizedFinalizedCheckpoint.Epoch {
		f.unrealizedFinalizedCheckpoint = finalizedCheckpoint
	}
}

// computeEpochAtSlot calculates the epoch at a given slot number.
func (f *ForkChoiceStore) computeEpochAtSlot(slot uint64) uint64 {
	return slot / f.forkGraph.Config().SlotsPerEpoch
}

// computeStartSlotAtEpoch calculates the starting slot of a given epoch.
func (f *ForkChoiceStore) computeStartSlotAtEpoch(epoch uint64) uint64 {
	return epoch * f.forkGraph.Config().SlotsPerEpoch
}

// computeSlotsSinceEpochStart calculates the number of slots since the start of the epoch of a given slot.
func (f *ForkChoiceStore) computeSlotsSinceEpochStart(slot uint64) uint64 {
	return slot - f.computeStartSlotAtEpoch(f.computeEpochAtSlot(slot))
}

// Ancestor returns the ancestor to the given root.
func (f *ForkChoiceStore) Ancestor(root libcommon.Hash, slot uint64) libcommon.Hash {
	block, has := f.forkGraph.GetBlock(root)
	if !has {
		return libcommon.Hash{}
	}
	for block.Block.Slot > slot {
		block, has = f.forkGraph.GetBlock(block.Block.ParentRoot)
		if !has {
			return libcommon.Hash{}
		}
	}
	return root
}
