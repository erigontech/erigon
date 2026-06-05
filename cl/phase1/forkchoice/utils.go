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
	"errors"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/transition"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Slot calculates the current slot number using the time and genesis slot.
func (f *ForkChoiceStore) Slot() uint64 {
	return f.beaconCfg.GenesisSlot + ((f.time.Load() - f.genesisTime) / f.beaconCfg.SecondsPerSlot)
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateCheckpoints(justifiedCheckpoint, finalizedCheckpoint solid.Checkpoint) {
	if justifiedCheckpoint.Epoch > f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.justifiedCheckpoint.Store(justifiedCheckpoint)
	}
	if finalizedCheckpoint.Epoch > f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.onNewFinalized(finalizedCheckpoint)
		f.finalizedCheckpoint.Store(finalizedCheckpoint)

		// prepare and send the finalized checkpoint event
		blockRoot := finalizedCheckpoint.Root
		blockHeader, ok := f.forkGraph.GetHeader(blockRoot)
		if !ok {
			log.Warn("Finalized block header not found", "blockRoot", blockRoot)
			return
		}
		f.emitters.State().SendFinalizedCheckpoint(&beaconevents.FinalizedCheckpointData{
			Block:               finalizedCheckpoint.Root,
			Epoch:               finalizedCheckpoint.Epoch,
			State:               blockHeader.Root,
			ExecutionOptimistic: false,
		})
	}
}

func (f *ForkChoiceStore) onNewFinalized(newFinalized solid.Checkpoint) {
	f.checkpointStates.Range(func(key, value any) bool {
		checkpoint := key.(solid.Checkpoint)

		if checkpoint.Epoch < newFinalized.Epoch {
			f.checkpointStates.Delete(key)
		}
		return true
	})

	// get rid of children
	finalizedSlot := newFinalized.Epoch * f.beaconCfg.SlotsPerEpoch
	f.childrens.Range(func(k, v any) bool {
		if v.(childrens).parentSlot <= finalizedSlot {
			f.childrens.Delete(k)
			delete(f.headSet, k.(common.Hash))
		}
		return true
	})

	// Clean up per-block unrealized justifications/finalizations for finalized blocks.
	f.unrealizedJustifications.Range(func(k, v any) bool {
		blockRoot := k.(common.Hash)
		header, has := f.forkGraph.GetHeader(blockRoot)
		if !has || header.Slot <= finalizedSlot {
			f.unrealizedJustifications.Delete(k)
		}
		return true
	})
	f.unrealizedFinalizations.Range(func(k, v any) bool {
		blockRoot := k.(common.Hash)
		header, has := f.forkGraph.GetHeader(blockRoot)
		if !has || header.Slot <= finalizedSlot {
			f.unrealizedFinalizations.Delete(k)
		}
		return true
	})
	// Clean up block timeliness entries for finalized blocks.
	f.blockTimeliness.Range(func(k, v any) bool {
		blockRoot := k.(common.Hash)
		header, has := f.forkGraph.GetHeader(blockRoot)
		if !has || header.Slot <= finalizedSlot {
			f.blockTimeliness.Delete(k)
		}
		return true
	})
	// Clean up GLOAS-specific payload votes for finalized blocks.
	// Note: envelope files are cleaned up in forkGraph.Prune().
	if newFinalized.Epoch >= f.beaconCfg.GloasForkEpoch {
		f.payloadTimelinessVote.Range(func(k, v any) bool {
			root := k.(common.Hash)
			if header, has := f.forkGraph.GetHeader(root); !has || header.Slot <= finalizedSlot {
				f.payloadTimelinessVote.Delete(k)
			}
			return true
		})
		f.payloadDataAvailabilityVote.Range(func(k, v any) bool {
			// Key is stored as common.Hash
			root := k.(common.Hash)
			if header, has := f.forkGraph.GetHeader(root); !has || header.Slot <= finalizedSlot {
				f.payloadDataAvailabilityVote.Delete(k)
			}
			return true
		})
	}

	// Guard against uint64 underflow during the first 3 epochs after genesis.
	if newFinalized.Epoch > 3 {
		slotToPrune := ((newFinalized.Epoch - 3) * f.beaconCfg.SlotsPerEpoch) - 1
		f.forkGraph.Prune(slotToPrune)
	}
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateUnrealizedCheckpoints(justifiedCheckpoint, finalizedCheckpoint solid.Checkpoint) {
	if justifiedCheckpoint.Epoch > f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.unrealizedJustifiedCheckpoint.Store(justifiedCheckpoint)
	}
	if finalizedCheckpoint.Epoch > f.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.unrealizedFinalizedCheckpoint.Store(finalizedCheckpoint)
	}
}

// computeEpochAtSlot calculates the epoch at a given slot number.
func (f *ForkChoiceStore) computeEpochAtSlot(slot uint64) uint64 {
	return slot / f.beaconCfg.SlotsPerEpoch
}

// computeStartSlotAtEpoch calculates the starting slot of a given epoch.
func (f *ForkChoiceStore) computeStartSlotAtEpoch(epoch uint64) uint64 {
	return epoch * f.beaconCfg.SlotsPerEpoch
}

// computeSlotsSinceEpochStart calculates the number of slots since the start of the epoch of a given slot.
func (f *ForkChoiceStore) computeSlotsSinceEpochStart(slot uint64) uint64 {
	return slot - f.computeStartSlotAtEpoch(f.computeEpochAtSlot(slot))
}

func (f *ForkChoiceStore) getAncestor(node ForkChoiceNode, slot uint64) ForkChoiceNode {
	header, has := f.forkGraph.GetHeader(node.Root)
	if !has {
		return ForkChoiceNode{Root: common.Hash{}, PayloadStatus: cltypes.PayloadStatusPending}
	}

	if header.Slot <= slot {
		return node
	}

	for {
		block, hasBlock := f.forkGraph.GetBlock(node.Root)
		if !hasBlock || block == nil {
			return ForkChoiceNode{Root: common.Hash{}, PayloadStatus: cltypes.PayloadStatusPending}
		}
		parent := ForkChoiceNode{
			Root:          block.Block.ParentRoot,
			PayloadStatus: f.getParentPayloadStatus(block.Block),
		}
		parentHeader, hasParent := f.forkGraph.GetHeader(parent.Root)
		if !hasParent {
			return ForkChoiceNode{Root: common.Hash{}, PayloadStatus: cltypes.PayloadStatusPending}
		}
		if parentHeader.Slot <= slot {
			return parent
		}
		node = parent
	}
}

// Ancestor returns the ancestor to the given root.
// [Modified in Gloas:EIP7732] Returns ForkChoiceNode with payload status.
func (f *ForkChoiceStore) Ancestor(root common.Hash, slot uint64) ForkChoiceNode {
	return f.getAncestor(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusPending,
	}, slot)
}

// getCheckpointState computes and caches checkpoint states.
func (f *ForkChoiceStore) getCheckpointState(checkpoint solid.Checkpoint) (*checkpointState, error) {
	// check if it can be found in cache.
	if state, ok := f.checkpointStates.Load(checkpoint); ok {
		return state.(*checkpointState), nil
	}

	// If it is not in cache compute it and then put in cache.
	if f.forkGraph == nil {
		return nil, errors.New("getCheckpointState: forkGraph not initialized")
	}
	baseState, err := f.forkGraph.GetState(checkpoint.Root, true)
	if err != nil {
		return nil, err
	}
	if baseState == nil {
		return nil, errors.New("getCheckpointState: baseState not found in graph")
	}
	// By default use the no change encoding to signal that there is no future epoch here.
	if baseState.Slot() < f.computeStartSlotAtEpoch(checkpoint.Epoch) {
		log.Debug("Long checkpoint detected")
		baseState, err = baseState.Copy()
		if err != nil {
			return nil, err
		}
		// If we require to change it then process the future epoch
		if err := transition.DefaultMachine.ProcessSlots(baseState, f.computeStartSlotAtEpoch(checkpoint.Epoch)); err != nil {
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
	f.publicKeysRegistry.AddState(checkpoint, baseState)
	checkpointState := newCheckpointState(f.beaconCfg, f.publicKeysRegistry, validators,
		mixes, baseState.GenesisValidatorsRoot(), baseState.Fork(), baseState.GetTotalActiveBalance(), state.Epoch(baseState.BeaconState), checkpoint)
	// Cache in memory what we are left with.
	f.checkpointStates.Store(checkpoint, checkpointState)
	return checkpointState, nil
}
