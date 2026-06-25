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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// [New in Gloas:EIP7732] Timing helper functions.
//
// GLOAS replaces the fixed INTERVALS_PER_SLOT division with a BPS (basis points)
// system for finer-grained control of slot-internal deadlines. All functions are
// epoch-aware: pre-GLOAS epochs use the legacy IntervalsPerSlot arithmetic,
// post-GLOAS epochs use BPS constants.

// getAttestationDueMs returns the attestation deadline in milliseconds from slot start.
func (f *ForkChoiceStore) getAttestationDueMs(epoch uint64) uint64 {
	return f.beaconCfg.AttestationDueMs(epoch >= f.beaconCfg.GloasForkEpoch)
}

// getAggregateDueMs returns the aggregate deadline in milliseconds from slot start.
//
// Pre-GLOAS:  SecondsPerSlot * 1000 * 2 / IntervalsPerSlot  (2/3 of slot = 8000 ms on mainnet)
// Post-GLOAS: SecondsPerSlot * AggregateDueBpsGloas / 10    (50% of slot = 6000 ms on mainnet)
func (f *ForkChoiceStore) getAggregateDueMs(epoch uint64) uint64 {
	if epoch >= f.beaconCfg.GloasForkEpoch {
		return f.beaconCfg.SecondsPerSlot * clparams.AggregateDueBpsGloas / (clparams.BpsFactor / 1000)
	}
	return f.beaconCfg.SecondsPerSlot * 1000 * 2 / f.beaconCfg.IntervalsPerSlot
}

// getSyncMessageDueMs returns the sync committee message deadline in milliseconds.
// Same as the attestation deadline in both pre- and post-GLOAS.
func (f *ForkChoiceStore) getSyncMessageDueMs(epoch uint64) uint64 {
	return f.getAttestationDueMs(epoch)
}

// getContributionDueMs returns the sync committee contribution deadline in milliseconds.
// Same as the aggregate deadline in both pre- and post-GLOAS.
func (f *ForkChoiceStore) getContributionDueMs(epoch uint64) uint64 {
	return f.getAggregateDueMs(epoch)
}

// getPayloadAttestationDueMs returns the PTC payload attestation deadline in milliseconds.
// [New in Gloas:EIP7732] This deadline only exists post-GLOAS (75% of slot = 9000 ms on mainnet).
// Callers should only invoke this for post-GLOAS epochs; the returned value is well-defined
// regardless, but meaningless pre-GLOAS since PTC duties do not exist before GLOAS.
func (f *ForkChoiceStore) getPayloadAttestationDueMs(_ uint64) uint64 {
	return f.beaconCfg.SecondsPerSlot * clparams.PayloadAttestationDueBps / (clparams.BpsFactor / 1000)
}

// shouldApplyProposerBoost returns whether a block arriving at the current store time
// is timely enough to receive the proposer boost (i.e. arrived before the attestation deadline).
//
// This is equivalent to checking block_timeliness[ATTESTATION_TIMELINESS_INDEX] at the
// current store time. It is NOT the same as WeightStore.ShouldApplyProposerBoost(),
// which implements the spec's should_apply_proposer_boost for weight calculation
// (GLOAS: complex logic with equivocation checks; pre-GLOAS: checks proposer_boost_root != zero).
func (f *ForkChoiceStore) shouldApplyProposerBoost() bool {
	secondsSinceGenesis := f.time.Load() - f.genesisTime
	timeIntoSlotMs := (secondsSinceGenesis % f.beaconCfg.SecondsPerSlot) * 1000
	epoch := f.computeEpochAtSlot(f.Slot())
	attestationDueMs := f.getAttestationDueMs(epoch)
	return timeIntoSlotMs < attestationDueMs
}

// recordBlockTimeliness implements record_block_timeliness from the spec.
// It stores a two-element timeliness vector keyed by block root.
//
// Spec (GLOAS fork-choice.md):
//
//	store.block_timeliness[root] = [
//	    is_current_slot and time_into_slot_ms < threshold
//	    for threshold in [attestation_threshold_ms, ptc_threshold_ms]
//	]
//
// [Modified in Gloas:EIP7732]
// Pre-GLOAS:  stores [block_timely, false] in blockTimeliness.
// Post-GLOAS: stores [block_timely, block_before_ptc_deadline] in blockTimeliness.
//
// NOTE: Both elements are time-based checks on when the *block* was received.
// PTC_TIMELINESS_INDEX records whether the block arrived before the PTC deadline
// (75% of slot), NOT whether the PTC has voted the payload present.
func (f *ForkChoiceStore) recordBlockTimeliness(block *cltypes.BeaconBlock, blockRoot common.Hash) {
	if f.Slot() != block.Slot {
		return
	}

	epoch := f.computeEpochAtSlot(block.Slot)

	// Compute time_into_slot_ms
	secondsSinceGenesis := f.time.Load() - f.genesisTime
	timeIntoSlotMs := (secondsSinceGenesis % f.beaconCfg.SecondsPerSlot) * 1000

	attestationThresholdMs := f.getAttestationDueMs(epoch)

	var timeliness [clparams.NumBlockTimelinessDeadlines]bool
	timeliness[clparams.AttestationTimelinessIndex] = timeIntoSlotMs < attestationThresholdMs

	// [New in Gloas:EIP7732] Post-GLOAS: also check if block arrived before PTC deadline.
	// This is a time-based check (not a PTC vote count check).
	if epoch >= f.beaconCfg.GloasForkEpoch {
		ptcThresholdMs := f.getPayloadAttestationDueMs(epoch)
		timeliness[clparams.PtcTimelinessIndex] = timeIntoSlotMs < ptcThresholdMs
	}

	f.blockTimeliness.Store(blockRoot, timeliness)
}

// updateProposerBoostRoot implements update_proposer_boost_root from the spec.
// It sets the proposer boost root if the block is timely, the boost has not
// already been assigned this slot, AND the block's proposer matches the
// expected proposer for the current slot.
//
// Spec (GLOAS fork-choice.md):
//
//	if is_timely and is_first_block:
//	    head_state = copy(store.block_states[get_head(store).root])
//	    process_slots(head_state, slot)
//	    if block.proposer_index == get_beacon_proposer_index(head_state):
//	        store.proposer_boost_root = root
func (f *ForkChoiceStore) updateProposerBoostRoot(block *cltypes.BeaconBlock, blockRoot common.Hash) {
	timeliness, ok := f.getBlockTimeliness(blockRoot)
	if !ok {
		return
	}
	isTimely := timeliness[clparams.AttestationTimelinessIndex]
	isFirstBlock := f.proposerBoostRoot.Load().(common.Hash) == (common.Hash{})

	if !isTimely || !isFirstBlock {
		return
	}

	// [New in Gloas:EIP7732] Verify the block's proposer matches the expected proposer
	// for this slot. This check is only performed post-GLOAS; pre-GLOAS does not verify
	// proposer identity for the boost (matching legacy behavior).
	// We use the proposer lookahead (computed during state transition) to avoid
	// the expensive get_head + process_slots path from the spec.
	epoch := f.computeEpochAtSlot(block.Slot)
	if epoch >= f.beaconCfg.GloasForkEpoch {
		currentSlot := f.Slot()
		lookahead, hasLookahead := f.GetProposerLookahead(currentSlot)
		if hasLookahead {
			slotInEpoch := currentSlot % f.beaconCfg.SlotsPerEpoch
			if slotInEpoch < uint64(lookahead.Length()) {
				expectedProposer := lookahead.Get(int(slotInEpoch))
				if block.ProposerIndex != expectedProposer {
					return // Wrong proposer — no boost
				}
			}
		}
		// If we don't have a lookahead yet (e.g. first GLOAS block), fall through
		// and grant the boost optimistically.
	}

	f.proposerBoostRoot.Store(blockRoot)
}

// shouldApplyProposerBoostGloas implements the GLOAS spec's should_apply_proposer_boost logic.
// Called from WeightStore.ShouldApplyProposerBoost when in GLOAS mode.
// proposerBoostRoot must not be zero (caller checks this).
//
// Spec (GLOAS fork-choice.md):
//
//	block = store.blocks[store.proposer_boost_root]
//	parent_root = block.parent_root
//	parent = store.blocks[parent_root]
//	slot = block.slot
//
//	# Apply boost if parent not from previous slot
//	if parent.slot + 1 < slot: return True
//
//	# Apply boost if parent not weak
//	if not is_head_weak(store, parent_root): return True
//
//	# If parent is weak and from previous slot, apply if no early equivocations
//	equivocations = [root for root, block in store.blocks.items()
//	    if store.block_timeliness[root][PTC_TIMELINESS_INDEX]
//	       and block.proposer_index == parent.proposer_index
//	       and block.slot + 1 == slot
//	       and root != parent_root]
//	return len(equivocations) == 0
//
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) shouldApplyProposerBoostGloas(proposerBoostRoot common.Hash) bool {
	// Get the boosted block
	boostBlock, ok := f.forkGraph.GetBlock(proposerBoostRoot)
	if !ok || boostBlock == nil {
		return false
	}

	parentRoot := boostBlock.Block.ParentRoot
	slot := boostBlock.Block.Slot

	// Get the parent block
	parentBlock, ok := f.forkGraph.GetBlock(parentRoot)
	if !ok || parentBlock == nil {
		return false
	}

	// Apply boost if parent not from previous slot
	if parentBlock.Block.Slot+1 < slot {
		return true
	}

	// Apply boost if parent not weak
	if !f.isHeadWeak(parentRoot) {
		return true
	}

	// Parent is weak and from previous slot.
	// Apply boost only if there are no equivocating blocks:
	// blocks by the same proposer at the same slot that are PTC-timely.
	parentProposerIndex := parentBlock.Block.ProposerIndex
	hasEquivocation := false

	// Iterate over all known head blocks to find equivocations.
	// We check the headSet and also walk the fork graph's children to cover
	// all blocks at relevant slots.
	f.blockTimeliness.Range(func(key, value any) bool {
		root := key.(common.Hash)
		timeliness := value.([clparams.NumBlockTimelinessDeadlines]bool)

		// Skip the parent root itself
		if root == parentRoot {
			return true
		}

		// Check PTC timeliness (block arrived before PTC deadline)
		if !timeliness[clparams.PtcTimelinessIndex] {
			return true
		}

		// Get the block to check proposer and slot
		blk, blkOk := f.forkGraph.GetBlock(root)
		if !blkOk || blk == nil {
			return true
		}

		// Check same proposer, adjacent slot, and not the parent
		if blk.Block.ProposerIndex == parentProposerIndex &&
			blk.Block.Slot+1 == slot {
			hasEquivocation = true
			return false // stop iteration
		}

		return true
	})

	return !hasEquivocation
}

// isHeadLate returns true if the head block was not received on time.
// Uses the block_timely element (index 0) of the timeliness vector.
// [New in Gloas:EIP7732] Updated to use the two-element timeliness vector.
func (f *ForkChoiceStore) isHeadLate(root common.Hash) bool {
	raw, ok := f.blockTimeliness.Load(root)
	if !ok {
		// Unknown block — treat as late (conservative)
		return true
	}
	timeliness := raw.([clparams.NumBlockTimelinessDeadlines]bool)
	return !timeliness[clparams.AttestationTimelinessIndex]
}

// isHeadWeak returns true if the head block has insufficient attestation support
// and is therefore eligible for proposer-boost reorgs.
//
// Spec (GLOAS fork-choice.md):
//
//	reorg_threshold = calculate_committee_fraction(justified_state, REORG_HEAD_WEIGHT_THRESHOLD)
//	head_weight = get_attestation_score(store, head_node, justified_state)
//	for index in range(get_committee_count_per_slot(head_state, epoch)):
//	    committee = get_beacon_committee(head_state, head_block.slot, index)
//	    head_weight += sum(
//	        justified_state.validators[i].effective_balance
//	        for i in committee if i in store.equivocating_indices
//	    )
//	return head_weight < reorg_threshold
//
// The equivocating validator adjustment only sums validators assigned to the
// head block's slot committees (not the entire validator set).
//
// [New in Gloas:EIP7732] This function considers equivocating validators.
func (f *ForkChoiceStore) isHeadWeak(root common.Hash) bool {
	justifiedCheckpoint := f.JustifiedCheckpoint()
	checkpointState, err := f.getCheckpointState(justifiedCheckpoint)
	if err != nil || checkpointState == nil {
		return false
	}

	// Calculate reorg threshold: committee_weight * REORG_HEAD_WEIGHT_THRESHOLD / 100
	committeeWeight := checkpointState.activeBalance / f.beaconCfg.SlotsPerEpoch
	reorgThreshold := committeeWeight * clparams.ReorgHeadWeightThreshold / 100

	// Get head weight using WeightStore
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	var headWeight uint64
	if f.beaconCfg.GetCurrentStateVersion(currentEpoch) >= clparams.GloasVersion {
		node := ForkChoiceNode{
			Root:          root,
			PayloadStatus: cltypes.PayloadStatusPending,
		}
		ws := NewWeightStore(f)
		headWeight = ws.GetAttestationScore(node)
	} else {
		// Pre-GLOAS: use legacy weight from weights map
		headWeight = f.weights[root]
	}

	// Add back weight from equivocating validators assigned to the head block's slot.
	// Spec: iterate over beacon committees for the head block's slot only.
	headBlock, ok := f.forkGraph.GetBlock(root)
	if !ok || headBlock == nil {
		return false
	}
	headSlot := headBlock.Block.Slot
	epoch := f.computeEpochAtSlot(headSlot)

	// Compute the committees for the head block's slot using the checkpoint state's shuffled set.
	lenIndicies := uint64(len(checkpointState.shuffledSet))
	if lenIndicies > 0 {
		committeesPerSlot := checkpointState.committeeCount(epoch, lenIndicies)
		count := committeesPerSlot * f.beaconCfg.SlotsPerEpoch
		for ci := uint64(0); ci < committeesPerSlot; ci++ {
			index := (headSlot%f.beaconCfg.SlotsPerEpoch)*committeesPerSlot + ci
			start := (lenIndicies * index) / count
			end := (lenIndicies * (index + 1)) / count
			for _, validatorIndex := range checkpointState.shuffledSet[start:end] {
				if !f.isUnequivocating(validatorIndex) {
					continue
				}
				vi := int(validatorIndex)
				if vi < checkpointState.validatorSetSize &&
					readFromBitset(checkpointState.actives, vi) &&
					!readFromBitset(checkpointState.slasheds, vi) {
					headWeight += checkpointState.balances[vi]
				}
			}
		}
	}

	return headWeight < reorgThreshold
}

// isParentStrong returns true if the parent of the block with the given root has
// sufficient attestation support (above the REORG_PARENT_WEIGHT_THRESHOLD).
//
// Spec (GLOAS fork-choice.md):
//
//	parent_threshold = calculate_committee_fraction(justified_state, REORG_PARENT_WEIGHT_THRESHOLD)
//	block = store.blocks[root]
//	parent_payload_status = get_parent_payload_status(store, block)
//	parent_node = ForkChoiceNode(root=block.parent_root, payload_status=parent_payload_status)
//	parent_weight = get_attestation_score(store, parent_node, justified_state)
//	return parent_weight > parent_threshold
//
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isParentStrong(root common.Hash) bool {
	justifiedCheckpoint := f.JustifiedCheckpoint()
	checkpointState, err := f.getCheckpointState(justifiedCheckpoint)
	if err != nil || checkpointState == nil {
		return false
	}

	// Calculate parent threshold: committee_weight * REORG_PARENT_WEIGHT_THRESHOLD / 100
	committeeWeight := checkpointState.activeBalance / f.beaconCfg.SlotsPerEpoch
	parentThreshold := committeeWeight * clparams.ReorgParentWeightThreshold / 100

	// Get the block to determine parent payload status
	block, ok := f.forkGraph.GetBlock(root)
	if !ok || block == nil {
		return false
	}

	// Get parent payload status and construct parent node
	parentPayloadStatus := f.getParentPayloadStatus(block.Block)
	parentNode := ForkChoiceNode{
		Root:          block.Block.ParentRoot,
		PayloadStatus: parentPayloadStatus,
	}

	// Get parent weight using WeightStore
	ws := NewWeightStore(f)
	parentWeight := ws.GetAttestationScore(parentNode)

	return parentWeight > parentThreshold
}

// getBlockTimeliness returns the timeliness vector for a block root.
// Returns (timeliness, true) if found, or (zero value, false) if not tracked.
func (f *ForkChoiceStore) getBlockTimeliness(root common.Hash) ([clparams.NumBlockTimelinessDeadlines]bool, bool) {
	raw, ok := f.blockTimeliness.Load(root)
	if !ok {
		return [clparams.NumBlockTimelinessDeadlines]bool{}, false
	}
	return raw.([clparams.NumBlockTimelinessDeadlines]bool), true
}
