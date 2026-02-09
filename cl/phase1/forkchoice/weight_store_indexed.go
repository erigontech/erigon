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
	"sync"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// VoteEntry represents a vote stored by its direct target root.
// [New in Gloas:EIP7732]
type VoteEntry struct {
	ValidatorIndex uint64
	Slot           uint64
	PayloadPresent bool
	Balance        uint64
}

// indexedWeightStore implements WeightStore with direct root mapping.
// Instead of duplicating votes across all ancestors, we store each vote once
// under its direct target root, then traverse descendants when computing weight.
// This trades O(V) space for O(T+S) query time where T is subtree size.
// [New in Gloas:EIP7732]
type indexedWeightStore struct {
	f *ForkChoiceStore

	// directVotes maps each root to validators who directly voted for it
	// Key: block root (the actual target of the vote)
	// Value: list of votes targeting this specific root
	directVotes map[common.Hash][]VoteEntry

	// version tracks changes to invalidate the index
	version uint64

	mu sync.RWMutex
}

// Ensure indexedWeightStore implements WeightStore
var _ WeightStore = (*indexedWeightStore)(nil)

// NewIndexedWeightStore creates a new indexed WeightStore for optimized GLOAS weight calculation.
func NewIndexedWeightStore(f *ForkChoiceStore) *indexedWeightStore {
	return &indexedWeightStore{
		f:           f,
		directVotes: make(map[common.Hash][]VoteEntry),
	}
}

// IndexVote adds a vote to the direct votes index.
// Call this when a new attestation is processed.
// O(1) - just appends to the target root's list.
// Balance is looked up internally from the justified checkpoint state.
func (w *indexedWeightStore) IndexVote(validatorIndex uint64, message LatestMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Look up balance from checkpoint state
	balance := uint64(0)
	justifiedCheckpoint := w.f.JustifiedCheckpoint()
	if checkpointState, err := w.f.getCheckpointState(justifiedCheckpoint); err == nil && checkpointState != nil {
		if int(validatorIndex) < len(checkpointState.balances) {
			balance = checkpointState.balances[validatorIndex]
		}
	}

	entry := VoteEntry{
		ValidatorIndex: validatorIndex,
		Slot:           message.Slot,
		PayloadPresent: message.PayloadPresent,
		Balance:        balance,
	}

	w.directVotes[message.Root] = append(w.directVotes[message.Root], entry)
	w.version++
}

// RemoveVote removes a validator's vote from the index.
// Call this when a validator's vote changes (before adding the new vote).
// O(E) where E is entries for that root - only filters one list.
func (w *indexedWeightStore) RemoveVote(validatorIndex uint64, oldRoot common.Hash) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entries, ok := w.directVotes[oldRoot]; ok {
		// Filter out this validator's entry
		filtered := make([]VoteEntry, 0, len(entries))
		for _, e := range entries {
			if e.ValidatorIndex != validatorIndex {
				filtered = append(filtered, e)
			}
		}
		if len(filtered) == 0 {
			delete(w.directVotes, oldRoot)
		} else {
			w.directVotes[oldRoot] = filtered
		}
	}

	w.version++
}

// Invalidate clears the entire index. Call when major state changes occur.
func (w *indexedWeightStore) Invalidate() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.directVotes = make(map[common.Hash][]VoteEntry)
	w.version++
}

// GetWeight returns the weight for a ForkChoiceNode using the indexed votes.
// This implementation is GLOAS-specific. For pre-GLOAS, use weightStore.
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
func (w *indexedWeightStore) GetWeight(node ForkChoiceNode) uint64 {
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

	// Get attestation score from indexed votes
	attestationScore := w.GetAttestationScore(node)

	// Check proposer boost
	if !w.ShouldApplyProposerBoost() {
		return attestationScore
	}

	proposerBoostRoot := w.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return attestationScore
	}

	// Create a LatestMessage for the proposer boost root
	proposerMessage := LatestMessage{
		Root:           proposerBoostRoot,
		Slot:           currentSlot,
		PayloadPresent: false,
	}

	if w.f.isSupportingVote(node, proposerMessage) {
		return attestationScore + w.GetProposerScore()
	}

	return attestationScore
}

// GetAttestationScore calculates attestation score by traversing descendants.
// Complexity: O(T + S) where T is subtree size and S is total votes in subtree.
func (w *indexedWeightStore) GetAttestationScore(node ForkChoiceNode) uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	block, has := w.f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return 0
	}

	var score uint64

	// DFS traversal using stack (iterative)
	stack := []common.Hash{node.Root}

	for len(stack) > 0 {
		// Pop
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Process votes for current node
		for _, entry := range w.directVotes[current] {
			// Skip equivocating validators
			if w.f.isUnequivocating(entry.ValidatorIndex) {
				continue
			}

			// Build LatestMessage from entry
			message := LatestMessage{
				Root:           current, // The actual root the validator voted for
				Slot:           entry.Slot,
				PayloadPresent: entry.PayloadPresent,
			}

			// Use is_supporting_vote to check (handles all the complex logic)
			if w.f.isSupportingVote(node, message) {
				score += entry.Balance
			}
		}

		// Push children
		stack = append(stack, w.f.children(current)...)
	}

	return score
}

// GetProposerScore returns the proposer boost score.
func (w *indexedWeightStore) GetProposerScore() uint64 {
	justifiedCheckpoint := w.f.JustifiedCheckpoint()
	checkpointState, err := w.f.getCheckpointState(justifiedCheckpoint)
	if err != nil {
		return 0
	}

	committeeWeight := checkpointState.activeBalance / w.f.beaconCfg.SlotsPerEpoch
	return (committeeWeight * w.f.beaconCfg.ProposerScoreBoost) / 100
}

// ShouldApplyProposerBoost returns whether the proposer boost should be applied.
func (w *indexedWeightStore) ShouldApplyProposerBoost() bool {
	// TODO: Not implemented yet. New in Gloas
	proposerBoostRoot := w.f.ProposerBoostRoot()
	return proposerBoostRoot != (common.Hash{})
}
