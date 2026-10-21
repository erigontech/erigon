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
	"fmt"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Slot calculates the current slot number using the time and genesis slot.
func (f *ForkChoiceStore) Slot() uint64 {
	return f.slotAtTimeMs(f.timeMs.Load())
}

func (f *ForkChoiceStore) slotAtTimeMs(timeMs uint64) uint64 {
	return f.beaconCfg.GenesisSlot + ((timeMs - utils.SecondsToMilliseconds(f.genesisTime)) / (f.beaconCfg.SecondsPerSlot * 1000))
}

// queueEmit defers an event send until after f.mu is released: Feed.Send blocks
// until every subscriber accepts the event, so a stalled subscriber must not be
// able to wedge the store. Callers hold f.mu.
func (f *ForkChoiceStore) queueEmit(emit func()) {
	f.queuedEmits = append(f.queuedEmits, emit)
}

func (f *ForkChoiceStore) queuePrune(slot uint64) {
	f.queuedPrunes = append(f.queuedPrunes, slot)
}

func (f *ForkChoiceStore) queueOperationPrune(checkpoint solid.Checkpoint) {
	f.queuedOperationPrunes = append(f.queuedOperationPrunes, checkpoint)
}

// drainQueuedWork runs queued event sends and prunes. Call after releasing f.mu.
func (f *ForkChoiceStore) drainQueuedWork() {
	f.mu.Lock()
	emits := f.queuedEmits
	prunes := f.queuedPrunes
	operationPrunes := f.queuedOperationPrunes
	f.queuedEmits = nil
	f.queuedPrunes = nil
	f.queuedOperationPrunes = nil
	f.mu.Unlock()
	if len(operationPrunes) > 0 && f.operationsPool.HasPrunableOperations() {
		f.scheduleOperationPrune(operationPrunes[len(operationPrunes)-1])
	}
	for _, emit := range emits {
		emit()
	}
	for _, pruneSlot := range prunes {
		if err := f.forkGraph.Prune(pruneSlot); err != nil {
			log.Warn("Failed to prune fork graph", "pruneSlot", pruneSlot, "err", err)
			continue
		}
		f.pruneExecutionPayloadFirstSeen(pruneSlot)
	}
}

func (f *ForkChoiceStore) scheduleOperationPrune(checkpoint solid.Checkpoint) {
	f.operationPruneMu.Lock()
	if checkpoint.Epoch <= f.operationPruneTarget.Epoch {
		f.operationPruneMu.Unlock()
		return
	}
	f.operationPruneTarget = checkpoint
	f.operationPrunePending = true
	if f.operationPruneRunning {
		f.operationPruneMu.Unlock()
		return
	}
	f.operationPruneRunning = true
	f.operationPruneMu.Unlock()
	go f.runOperationPruner()
}

func (f *ForkChoiceStore) runOperationPruner() {
	for {
		f.operationPruneMu.Lock()
		if !f.operationPrunePending {
			f.operationPruneRunning = false
			f.operationPruneMu.Unlock()
			return
		}
		checkpoint := f.operationPruneTarget
		f.operationPrunePending = false
		f.operationPruneMu.Unlock()

		if !f.operationsPool.HasPrunableOperations() {
			continue
		}
		finalizedState, err := f.GetStateAtBlockRoot(checkpoint.Root)
		if err != nil || finalizedState == nil {
			log.Warn("Failed to load finalized state for operation pruning", "root", checkpoint.Root, "err", err)
			continue
		}
		f.operationsPool.PruneFinalized(finalizedState, checkpoint.Epoch)
	}
}

// updateCheckpoints updates the justified and finalized checkpoints if new checkpoints have higher epochs.
func (f *ForkChoiceStore) updateCheckpoints(justifiedCheckpoint, finalizedCheckpoint solid.Checkpoint) {
	if justifiedCheckpoint.Epoch > f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.checkpointStatesMu.Lock()
		f.justifiedCheckpoint.Store(justifiedCheckpoint)
		f.checkpointStatesMu.Unlock()
	}
	if finalizedCheckpoint.Epoch > f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch {
		f.finalizedCheckpoint.Store(finalizedCheckpoint)
		f.queueOperationPrune(finalizedCheckpoint)
		f.onNewFinalized(finalizedCheckpoint)

		blockRoot := finalizedCheckpoint.Root
		blockHeader, ok := f.forkGraph.GetHeader(blockRoot)
		if !ok {
			log.Warn("Finalized block header not found", "blockRoot", blockRoot)
			return
		}
		data := &beaconevents.FinalizedCheckpointData{
			Block:               finalizedCheckpoint.Root,
			Epoch:               finalizedCheckpoint.Epoch,
			State:               blockHeader.Root,
			ExecutionOptimistic: false,
		}
		f.queueEmit(func() { f.emitters.State().SendFinalizedCheckpoint(data) })
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
	if f.gloasWeightTree != nil {
		f.gloasWeightTree.pruneFinalized(finalizedSlot)
	}
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
		f.queuePrune(slotToPrune)
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
	if cached, ok := f.checkpointStates.Load(checkpoint); ok {
		return cached.(*checkpointState), nil
	}
	if f.forkGraph == nil {
		return nil, errors.New("getCheckpointState: forkGraph not initialized")
	}
	return f.checkpointStateOnce(checkpoint, "current", func() (*checkpointState, error) {
		var cached *checkpointState
		var baseState *state.CachingBeaconState
		var current bool
		var err error
		func() {
			f.mu.RLock()
			defer f.mu.RUnlock()
			if cs, ok := f.checkpointStates.Load(checkpoint); ok {
				cached = cs.(*checkpointState)
				return
			}
			baseState, current, err = f.copyCurrentStateLocked(checkpoint.Root)
		}()
		if cached != nil || err != nil {
			return cached, err
		}
		if !current {
			return f.replayCheckpointStateOnce(checkpoint)
		}
		return f.buildCheckpointState(checkpoint, baseState)
	})
}

func (f *ForkChoiceStore) getCheckpointStateLocked(checkpoint solid.Checkpoint) (*checkpointState, error) {
	if cached, ok := f.checkpointStates.Load(checkpoint); ok {
		return cached.(*checkpointState), nil
	}
	if f.forkGraph == nil {
		return nil, errors.New("getCheckpointState: forkGraph not initialized")
	}
	baseState, current, err := f.copyCurrentStateLocked(checkpoint.Root)
	if err != nil {
		return nil, err
	}
	if current {
		return f.buildCheckpointState(checkpoint, baseState)
	}
	return f.replayCheckpointStateOnce(checkpoint)
}

// replayCheckpointStateOnce never takes f.mu, so callers that hold it can join its flight.
func (f *ForkChoiceStore) replayCheckpointStateOnce(checkpoint solid.Checkpoint) (*checkpointState, error) {
	return f.checkpointStateOnce(checkpoint, "replay", func() (*checkpointState, error) {
		baseState, err := f.forkGraph.ReplayState(checkpoint.Root)
		if err != nil {
			return nil, err
		}
		return f.buildCheckpointState(checkpoint, baseState)
	})
}

func (f *ForkChoiceStore) checkpointStateOnce(checkpoint solid.Checkpoint, flight string, build func() (*checkpointState, error)) (*checkpointState, error) {
	cs, err, _ := f.checkpointStateBuilds.Do(fmt.Sprintf("%s:%d:%x", flight, checkpoint.Epoch, checkpoint.Root), func() (any, error) {
		if cached, ok := f.checkpointStates.Load(checkpoint); ok {
			return cached.(*checkpointState), nil
		}
		return build()
	})
	if err != nil {
		return nil, err
	}
	return cs.(*checkpointState), nil
}

func (f *ForkChoiceStore) buildCheckpointState(checkpoint solid.Checkpoint, baseState *state.CachingBeaconState) (*checkpointState, error) {
	if baseState == nil {
		return nil, errors.New("getCheckpointState: baseState not found in graph")
	}
	// By default use the no change encoding to signal that there is no future epoch here.
	if baseState.Slot() < f.computeStartSlotAtEpoch(checkpoint.Epoch) {
		log.Debug("Long checkpoint detected")
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
		mixes, baseState.GenesisValidatorsRoot(), baseState.Fork(), getUnslashedActiveBalance(baseState), state.Epoch(baseState.BeaconState), checkpoint)
	if f.belowFinalized(checkpoint) {
		return checkpointState, nil
	}
	// Cache in memory what we are left with.
	f.storeCheckpointState(checkpoint, checkpointState)
	if f.belowFinalized(checkpoint) {
		f.checkpointStates.Delete(checkpoint)
	}
	return checkpointState, nil
}

func (f *ForkChoiceStore) belowFinalized(checkpoint solid.Checkpoint) bool {
	finalized, ok := f.finalizedCheckpoint.Load().(solid.Checkpoint)
	return ok && checkpoint.Epoch < finalized.Epoch
}

// storeCheckpointState stores the state, then evicts the lowest-epoch entries
// above maxCheckpointStates, but never the current justified state, which is read
// to compute the head, or the state just stored.
func (f *ForkChoiceStore) storeCheckpointState(checkpoint solid.Checkpoint, checkpointState *checkpointState) {
	f.checkpointStatesMu.Lock()
	defer f.checkpointStatesMu.Unlock()

	f.checkpointStates.Store(checkpoint, checkpointState)
	for {
		justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
		count := 0
		var oldest solid.Checkpoint
		hasOldest := false
		f.checkpointStates.Range(func(key, _ any) bool {
			count++
			candidate := key.(solid.Checkpoint)
			if candidate == justified || candidate == checkpoint {
				return true
			}
			if !hasOldest || candidate.Epoch < oldest.Epoch {
				oldest = candidate
				hasOldest = true
			}
			return true
		})
		if count <= maxCheckpointStates || !hasOldest {
			return
		}
		f.checkpointStates.Delete(oldest)
	}
}

func getUnslashedActiveBalance(beaconState *state.CachingBeaconState) uint64 {
	epoch := state.Epoch(beaconState)
	var total uint64
	beaconState.ForEachValidator(func(validator solid.Validator, _, _ int) bool {
		if validator.Active(epoch) && !validator.Slashed() {
			total += validator.EffectiveBalance()
		}
		return true
	})
	return max(beaconState.BeaconConfig().EffectiveBalanceIncrement, total)
}
