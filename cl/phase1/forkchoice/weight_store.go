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
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// WeightStore provides methods to calculate the weight of blocks in the fork choice.
// [New in Gloas:EIP7732]
type WeightStore interface {
	// GetWeight returns the weight (in Gwei) for a ForkChoiceNode.
	// Takes payload status into account via is_supporting_vote.
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
// [New in Gloas:EIP7732]
//
// From spec:
//
//	if node.payload_status == PAYLOAD_STATUS_PENDING or
//	   store.blocks[node.root].slot + 1 != get_current_slot(store):
//	    # calculate weight
//	    return attestation_score + proposer_score
//	else:
//	    return Gwei(0)
//
// So: PENDING OR not-previous-slot → calculate weight
// NOT PENDING AND is-previous-slot → return 0
func (w *weightStore) GetWeight(node ForkChoiceNode) uint64 {
	// Get the block for this node
	block, has := w.f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return 0
	}

	currentSlot := w.f.Slot()
	isPreviousSlot := block.Block.Slot+1 == currentSlot
	isPending := node.PayloadStatus == cltypes.PayloadStatusPending

	// If NOT PENDING AND is previous slot → return 0
	if !isPending && isPreviousSlot {
		return 0
	}

	// Otherwise (PENDING OR not previous slot) → calculate weight
	attestationScore := w.GetAttestationScore(node)

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
// [New in Gloas:EIP7732]
// Uses is_supporting_vote to check if a validator's vote supports the node.
func (w *weightStore) GetAttestationScore(node ForkChoiceNode) uint64 {
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
func (w *weightStore) ShouldApplyProposerBoost() bool {
	proposerBoostRoot := w.f.ProposerBoostRoot()
	return proposerBoostRoot != (common.Hash{})
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

// GetIndexedWeightStore returns the indexed WeightStore for optimized GLOAS weight calculation.
// Returns nil if not initialized.
func (f *ForkChoiceStore) GetIndexedWeightStore() *indexedWeightStore {
	return f.indexedWeightStore
}

// ComputeWeightsWithAuxState computes weights for all blocks using an auxiliary state.
// This is used during head selection when we have a cached state available.
// [New in Gloas:EIP7732] Uses WeightStore for weight calculation.
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
