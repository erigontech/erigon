package forkchoice

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/transition"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/log/v3"
)

// Slot calculates the current slot number using the time and genesis slot.
func (f *ForkChoiceStore) Slot() uint64 {
	return f.beaconCfg.GenesisSlot + ((f.time.Load() - f.genesisTime) / f.beaconCfg.SecondsPerSlot)
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateCheckpoints(justifiedCheckpoint, finalizedCheckpoint solid.Checkpoint) {
	if justifiedCheckpoint.Epoch() > f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch() {
		f.justifiedCheckpoint.Store(justifiedCheckpoint)
	}
	if finalizedCheckpoint.Epoch() > f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch() {
		f.emitters.Publish("finalized_checkpoint", finalizedCheckpoint)
		f.onNewFinalized(finalizedCheckpoint)
		f.finalizedCheckpoint.Store(finalizedCheckpoint)
	}
}

func (f *ForkChoiceStore) onNewFinalized(newFinalized solid.Checkpoint) {
	f.checkpointStates.Range(func(key, value any) bool {
		checkpoint := key.(checkpointComparable)

		if solid.Checkpoint(checkpoint).Epoch() < newFinalized.Epoch() {
			f.checkpointStates.Delete(key)
		}
		return true
	})

	// get rid of children
	f.childrens.Range(func(k, v any) bool {
		if v.(childrens).parentSlot <= newFinalized.Epoch()*f.beaconCfg.SlotsPerEpoch {
			f.childrens.Delete(k)
			delete(f.headSet, k.(libcommon.Hash))
		}
		return true
	})

	f.forkGraph.Prune(newFinalized.Epoch() * f.beaconCfg.SlotsPerEpoch)
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateUnrealizedCheckpoints(justifiedCheckpoint, finalizedCheckpoint solid.Checkpoint) {
	if justifiedCheckpoint.Epoch() > f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint).Epoch() {
		f.unrealizedJustifiedCheckpoint.Store(justifiedCheckpoint)
	}
	if finalizedCheckpoint.Epoch() > f.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint).Epoch() {
		f.unrealizedFinalizedCheckpoint.Store(finalizedCheckpoint)
	}
}

// computeEpochAtSlot calculates the epoch at a given slot number.
func (f *ForkChoiceStore) computeEpochAtSlot(slot uint64) uint64 {
	return slot / f.beaconCfg.SlotsPerEpoch
}

func (f *ForkChoiceStore) computeSyncPeriod(epoch uint64) uint64 {
	return epoch / f.beaconCfg.EpochsPerSyncCommitteePeriod
}

// computeStartSlotAtEpoch calculates the starting slot of a given epoch.
func (f *ForkChoiceStore) computeStartSlotAtEpoch(epoch uint64) uint64 {
	return epoch * f.beaconCfg.SlotsPerEpoch
}

// computeSlotsSinceEpochStart calculates the number of slots since the start of the epoch of a given slot.
func (f *ForkChoiceStore) computeSlotsSinceEpochStart(slot uint64) uint64 {
	return slot - f.computeStartSlotAtEpoch(f.computeEpochAtSlot(slot))
}

// Ancestor returns the ancestor to the given root.
func (f *ForkChoiceStore) Ancestor(root libcommon.Hash, slot uint64) libcommon.Hash {
	header, has := f.forkGraph.GetHeader(root)
	if !has {
		return libcommon.Hash{}
	}
	for header.Slot > slot {
		root = header.ParentRoot
		header, has = f.forkGraph.GetHeader(header.ParentRoot)
		if !has {
			return libcommon.Hash{}
		}
	}
	return root
}

// getCheckpointState computes and caches checkpoint states.
func (f *ForkChoiceStore) getCheckpointState(checkpoint solid.Checkpoint) (*checkpointState, error) {
	// check if it can be found in cache.
	if state, ok := f.checkpointStates.Load(checkpointComparable(checkpoint)); ok {
		return state.(*checkpointState), nil
	}

	// If it is not in cache compute it and then put in cache.
	baseState, err := f.forkGraph.GetState(checkpoint.BlockRoot(), true)
	if err != nil {
		return nil, err
	}
	if baseState == nil {
		return nil, fmt.Errorf("getCheckpointState: baseState not found in graph")
	}
	// By default use the no change encoding to signal that there is no future epoch here.
	if baseState.Slot() < f.computeStartSlotAtEpoch(checkpoint.Epoch()) {
		log.Debug("Long checkpoint detected")
		// If we require to change it then process the future epoch
		if err := transition.DefaultMachine.ProcessSlots(baseState, f.computeStartSlotAtEpoch(checkpoint.Epoch())); err != nil {
			return nil, err
		}
	}
	mixes := baseState.RandaoMixes()
	// TODO: make this copy smarter when validators is a smarter struct
	validators := make([]solid.Validator, baseState.ValidatorLength())
	baseState.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		validators[idx] = v
		return true
	})
	checkpointState := newCheckpointState(f.beaconCfg, f.anchorPublicKeys, validators,
		mixes, baseState.GenesisValidatorsRoot(), baseState.Fork(), baseState.GetTotalActiveBalance(), state.Epoch(baseState.BeaconState))
	// Cache in memory what we are left with.
	f.checkpointStates.Store(checkpointComparable(checkpoint), checkpointState)
	return checkpointState, nil
}
