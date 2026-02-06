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
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// WeightStore provides methods to calculate the weight of blocks in the fork choice.
// It supports both pre-GLOAS (Phase0) and post-GLOAS weight calculation.
// [Modified in Gloas:EIP7732]
type WeightStore interface {
	// GetWeight returns the weight (in Gwei) for a ForkChoiceNode.
	// Pre-GLOAS: PayloadStatus is ignored, uses simple ancestor-based attestation scoring.
	// Post-GLOAS: Takes payload status into account via is_supporting_vote.
	GetWeight(node ForkChoiceNode) uint64

	// GetAttestationScore returns the attestation score for a ForkChoiceNode.
	GetAttestationScore(node ForkChoiceNode) uint64

	// GetProposerScore returns the proposer boost score.
	GetProposerScore() uint64

	// ShouldApplyProposerBoost returns whether the proposer boost should be applied.
	ShouldApplyProposerBoost() bool
}

// weightStore implements WeightStore interface.
type weightStore struct {
	f *ForkChoiceStore
}

// NewWeightStore creates a new WeightStore for the given ForkChoiceStore.
func NewWeightStore(f *ForkChoiceStore) WeightStore {
	return &weightStore{f: f}
}

// GetWeight returns the weight for a ForkChoiceNode.
// Pre-GLOAS: PayloadStatus is ignored, uses Phase0 logic.
// Post-GLOAS: Takes payload status into account.
func (w *weightStore) GetWeight(node ForkChoiceNode) uint64 {
	if !w.isPostGloas() {
		// Pre-GLOAS: Ignore PayloadStatus, use Phase0 logic
		return w.getWeightPhase0(node.Root)
	}

	// Post-GLOAS: Apply GLOAS-specific checks
	return w.getWeightGloas(node)
}

// getWeightPhase0 implements the Phase0 get_weight function.
// def get_weight(store: Store, root: Root) -> Gwei:
//
//	state = store.checkpoint_states[store.justified_checkpoint]
//	attestation_score = get_attestation_score(store, root, state)
//	if store.proposer_boost_root == Root():
//	    return attestation_score
//	proposer_score = Gwei(0)
//	if get_ancestor(store, store.proposer_boost_root, store.blocks[root].slot) == root:
//	    proposer_score = get_proposer_score(store)
//	return attestation_score + proposer_score
func (w *weightStore) getWeightPhase0(root common.Hash) uint64 {
	attestationScore := w.getAttestationScorePhase0(root)

	proposerBoostRoot := w.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return attestationScore
	}

	// Check if root is an ancestor of proposer_boost_root
	block, has := w.f.forkGraph.GetBlock(root)
	if !has || block == nil {
		return attestationScore
	}

	ancestor := w.f.Ancestor(proposerBoostRoot, block.Block.Slot)
	if ancestor.Root == root {
		return attestationScore + w.GetProposerScore()
	}

	return attestationScore
}

// getWeightGloas implements the GLOAS get_weight function.
// [New in Gloas:EIP7732]
// Key differences from Phase0:
// 1. Returns 0 if payload_status is PENDING
// 2. Returns 0 if block slot is not current_slot - 1
// 3. Uses is_supporting_vote for attestation scoring
// 4. Proposer boost uses is_supporting_vote check
func (w *weightStore) getWeightGloas(node ForkChoiceNode) uint64 {
	// Return 0 if payload status is PENDING
	if node.PayloadStatus == cltypes.PayloadStatusPending {
		return 0
	}

	// Get the block for this node
	block, has := w.f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return 0
	}

	// Return 0 if block slot is not exactly one slot before current slot
	currentSlot := w.f.Slot()
	if block.Block.Slot+1 != currentSlot {
		return 0
	}

	// Calculate attestation score using is_supporting_vote
	attestationScore := w.getAttestationScoreGloas(node)

	// Check if proposer boost should be applied
	if !w.ShouldApplyProposerBoost() {
		return attestationScore
	}

	proposerBoostRoot := w.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return attestationScore
	}

	// Create a LatestMessage for the proposer boost root
	// Treat proposer boost as a current-slot vote with payload_present=false
	proposerMessage := LatestMessage{
		Root:           proposerBoostRoot,
		Slot:           currentSlot,
		PayloadPresent: false,
	}

	// Check if proposer's vote supports the node
	if w.f.isSupportingVote(node, proposerMessage) {
		return attestationScore + w.GetProposerScore()
	}

	return attestationScore
}

// GetAttestationScore returns the attestation score for a ForkChoiceNode.
// Pre-GLOAS: PayloadStatus is ignored, uses ancestor-based scoring.
// Post-GLOAS: Uses is_supporting_vote for checking.
func (w *weightStore) GetAttestationScore(node ForkChoiceNode) uint64 {
	if w.isPostGloas() {
		return w.getAttestationScoreGloas(node)
	}

	return w.getAttestationScorePhase0(node.Root)
}

// getAttestationScorePhase0 implements the Phase0 get_attestation_score.
// def get_attestation_score(store: Store, root: Root, state: BeaconState) -> Gwei:
//
//	unslashed_and_active_indices = [
//	    i for i in get_active_validator_indices(state, get_current_epoch(state))
//	    if not state.validators[i].slashed
//	]
//	return Gwei(sum(
//	    state.validators[i].effective_balance
//	    for i in unslashed_and_active_indices
//	    if (i in store.latest_messages
//	        and i not in store.equivocating_indices
//	        and get_ancestor(store, store.latest_messages[i].root, store.blocks[root].slot) == root)
//	))
func (w *weightStore) getAttestationScorePhase0(root common.Hash) uint64 {
	justifiedCheckpoint := w.f.JustifiedCheckpoint()
	checkpointState, err := w.f.getCheckpointState(justifiedCheckpoint)
	if err != nil {
		return 0
	}

	block, has := w.f.forkGraph.GetBlock(root)
	if !has || block == nil {
		return 0
	}
	blockSlot := block.Block.Slot

	var score uint64
	for validatorIndex := 0; validatorIndex < checkpointState.validatorSetSize; validatorIndex++ {
		// Check if validator is active and not slashed
		if !readFromBitset(checkpointState.actives, validatorIndex) ||
			readFromBitset(checkpointState.slasheds, validatorIndex) {
			continue
		}

		// Check if validator has a latest message
		message, hasMessage := w.f.latestMessages.get(validatorIndex)
		if !hasMessage || message == (LatestMessage{}) {
			continue
		}

		// Check if validator is not equivocating
		if w.f.isUnequivocating(uint64(validatorIndex)) {
			continue
		}

		// Check if the message root's ancestor at block's slot equals root
		ancestor := w.f.Ancestor(message.Root, blockSlot)
		if ancestor.Root == root {
			score += checkpointState.balances[validatorIndex]
		}
	}

	return score
}

// getAttestationScoreGloas implements the GLOAS attestation scoring.
// [New in Gloas:EIP7732]
// Uses is_supporting_vote to check if a validator's vote supports the node.
func (w *weightStore) getAttestationScoreGloas(node ForkChoiceNode) uint64 {
	justifiedCheckpoint := w.f.JustifiedCheckpoint()
	checkpointState, err := w.f.getCheckpointState(justifiedCheckpoint)
	if err != nil {
		return 0
	}

	var score uint64
	for validatorIndex := 0; validatorIndex < checkpointState.validatorSetSize; validatorIndex++ {
		// Check if validator is active and not slashed
		if !readFromBitset(checkpointState.actives, validatorIndex) ||
			readFromBitset(checkpointState.slasheds, validatorIndex) {
			continue
		}

		// Check if validator has a latest message
		message, hasMessage := w.f.latestMessages.get(validatorIndex)
		if !hasMessage || message == (LatestMessage{}) {
			continue
		}

		// Check if validator is not equivocating
		if w.f.isUnequivocating(uint64(validatorIndex)) {
			continue
		}

		// Use is_supporting_vote for GLOAS
		if w.f.isSupportingVote(node, message) {
			score += checkpointState.balances[validatorIndex]
		}
	}

	return score
}

// GetProposerScore returns the proposer boost score.
// proposer_score = (committee_weight * PROPOSER_SCORE_BOOST) / 100
// where committee_weight = total_active_balance / SLOTS_PER_EPOCH
func (w *weightStore) GetProposerScore() uint64 {
	justifiedCheckpoint := w.f.JustifiedCheckpoint()
	checkpointState, err := w.f.getCheckpointState(justifiedCheckpoint)
	if err != nil {
		return 0
	}

	committeeWeight := checkpointState.activeBalance / w.f.beaconCfg.SlotsPerEpoch
	return (committeeWeight * w.f.beaconCfg.ProposerScoreBoost) / 100
}

// ShouldApplyProposerBoost returns whether the proposer boost should be applied.
// [New in Gloas:EIP7732]
// In GLOAS, proposer boost has additional conditions based on the current slot
// and the state of the fork choice store.
func (w *weightStore) ShouldApplyProposerBoost() bool {
	proposerBoostRoot := w.f.ProposerBoostRoot()
	return proposerBoostRoot != (common.Hash{})
}

// isPostGloas returns true if the current epoch is after the GLOAS fork epoch.
func (w *weightStore) isPostGloas() bool {
	currentSlot := w.f.Slot()
	currentEpoch := currentSlot / w.f.beaconCfg.SlotsPerEpoch
	version := w.f.beaconCfg.GetCurrentStateVersion(currentEpoch)
	return version >= clparams.GloasVersion
}

// WeightStoreReader provides read-only access to weight calculations.
// This is useful for external consumers that only need to query weights.
type WeightStoreReader interface {
	GetWeight(node ForkChoiceNode) uint64
}

// GetWeightStore returns a WeightStore for the ForkChoiceStore.
func (f *ForkChoiceStore) GetWeightStore() WeightStore {
	return NewWeightStore(f)
}

// ComputeWeightsWithAuxState computes weights for all blocks using an auxiliary state.
// This is used during head selection when we have a cached state available.
// [Modified in Gloas:EIP7732] Uses WeightStore for weight calculation.
func (f *ForkChoiceStore) ComputeWeightsWithAuxState(auxState *state.CachingBeaconState) map[common.Hash]uint64 {
	ws := NewWeightStore(f)
	weights := make(map[common.Hash]uint64)

	// Iterate through all heads and compute weights
	for head := range f.headSet {
		node := ForkChoiceNode{
			Root:          head,
			PayloadStatus: cltypes.PayloadStatusPending, // Will be determined inside GetWeight
		}
		weights[head] = ws.GetWeight(node)
	}

	return weights
}
