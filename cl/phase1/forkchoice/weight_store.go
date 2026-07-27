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

// WeightStore provides methods to calculate the weight of blocks in the fork choice.
// [New in Gloas:EIP7732]
type WeightStore interface {
	// GetWeight returns the weight (in Gwei) for a ForkChoiceNode.
	// Takes payload status into account via node ancestor checks.
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
	f               *ForkChoiceStore
	checkpointState *checkpointState // cached per getHeadGloas invocation
}

// NewWeightStore creates a new WeightStore for the given ForkChoiceStore.
// It pre-fetches the checkpoint state once so that repeated calls to
// GetAttestationScore / GetProposerScore avoid redundant disk reads.
func NewWeightStore(f *ForkChoiceStore) WeightStore {
	justifiedCheckpoint := f.JustifiedCheckpoint()
	cs, _ := f.getCheckpointState(justifiedCheckpoint)
	return newWeightStoreFromCheckpointState(f, cs)
}

func newWeightStoreFromCheckpointState(f *ForkChoiceStore, cs *checkpointState) WeightStore {
	return &weightStore{f: f, checkpointState: cs}
}

// getWeight returns the weight for a ForkChoiceNode.
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
func getWeight(store WeightStore, f *ForkChoiceStore, node ForkChoiceNode) uint64 {
	if f.isPreviousSlotPayloadDecision(node) {
		return 0
	}

	attestationScore := store.GetAttestationScore(node)
	if !store.ShouldApplyProposerBoost() {
		return attestationScore
	}

	proposerBoostRoot := f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return attestationScore
	}

	proposerBoostNode := ForkChoiceNode{
		Root:          proposerBoostRoot,
		PayloadStatus: cltypes.PayloadStatusPending,
	}
	if f.isAncestor(proposerBoostNode, node) {
		return attestationScore + store.GetProposerScore()
	}

	return attestationScore
}

func getProposerScore(f *ForkChoiceStore, cs *checkpointState) uint64 {
	if cs == nil {
		return 0
	}
	committeeWeight := cs.activeBalance / f.beaconCfg.SlotsPerEpoch
	return (committeeWeight * f.beaconCfg.ProposerScoreBoost) / 100
}

func (w *weightStore) GetWeight(node ForkChoiceNode) uint64 {
	return getWeight(w, w.f, node)
}

// GetAttestationScore returns the attestation score for a ForkChoiceNode.
// [New in Gloas:EIP7732]
// Uses node ancestor checks to decide if a validator's vote supports the node.
func (w *weightStore) GetAttestationScore(node ForkChoiceNode) uint64 {
	checkpointState := w.checkpointState
	if checkpointState == nil {
		return 0
	}

	var score uint64
	for validatorIndex := 0; validatorIndex < checkpointState.validatorSetSize; validatorIndex++ {
		message, balance, ok := w.f.countableVote(checkpointState, validatorIndex)
		if !ok {
			continue
		}
		if w.f.isAncestor(w.f.getSupportedNode(message), node) {
			score += balance
		}
	}

	return score
}

// countableVote returns the validator's latest message and balance when the vote
// counts toward fork-choice weights: inside the justified registry's bounds,
// active, not slashed, not equivocating, with a non-empty message and balance.
// Every weight consumer must share this filter.
func (f *ForkChoiceStore) countableVote(cs *checkpointState, validatorIndex int) (LatestMessage, uint64, bool) {
	if validatorIndex >= cs.validatorSetSize || validatorIndex >= len(cs.balances) {
		return LatestMessage{}, 0, false
	}
	if !readFromBitset(cs.actives, validatorIndex) || readFromBitset(cs.slasheds, validatorIndex) ||
		f.isUnequivocating(uint64(validatorIndex)) {
		return LatestMessage{}, 0, false
	}
	message, has := f.latestMessages.get(validatorIndex)
	if !has || message == (LatestMessage{}) {
		return LatestMessage{}, 0, false
	}
	balance := cs.balances[validatorIndex]
	if balance == 0 {
		return LatestMessage{}, 0, false
	}
	return message, balance, true
}

// GetProposerScore returns the proposer boost score.
// proposer_score = (committee_weight * PROPOSER_SCORE_BOOST) / 100
// where committee_weight = total_active_balance / SLOTS_PER_EPOCH
func (w *weightStore) GetProposerScore() uint64 {
	return getProposerScore(w.f, w.checkpointState)
}

// ShouldApplyProposerBoost returns whether the proposer boost should be applied.
//
// Pre-GLOAS: simple check that proposer_boost_root is set.
//
// [New in Gloas:EIP7732] Post-GLOAS implements the full spec logic:
//
//	if proposer_boost_root == Root(): return False
//	block = store.blocks[proposer_boost_root]
//	parent = store.blocks[block.parent_root]
//	if parent.slot + 1 < block.slot: return True           # parent not from previous slot
//	if not is_head_weak(store, parent_root): return True    # parent not weak
//	# parent is weak and from previous slot — check for equivocations
//	equivocations = [ ... timely blocks by same proposer at same slot ]
//	return len(equivocations) == 0
func (w *weightStore) ShouldApplyProposerBoost() bool {
	proposerBoostRoot := w.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return false
	}

	// Pre-GLOAS: just check if root is set
	currentEpoch := w.f.computeEpochAtSlot(w.f.Slot())
	if w.f.beaconCfg.GetCurrentStateVersion(currentEpoch) < clparams.GloasVersion {
		return true
	}

	// [New in Gloas:EIP7732] Full spec logic
	return w.f.shouldApplyProposerBoostGloas(proposerBoostRoot)
}
