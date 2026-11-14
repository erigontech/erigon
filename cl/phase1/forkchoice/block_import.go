// Copyright 2026 The Erigon Authors
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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type preparedAttestingIndices struct {
	indices []uint64
	err     error
}

type preparedPostState struct {
	randaoMixes         solid.HashVectorSSZ
	randaoDelta         randaoDelta
	participationEpoch  uint64
	participation       *solid.ParticipationBitList
	preverifiedSizes    preverifiedAppendListsSizes
	finalityCheckpoints finalityCheckpoints
	totalActiveBalance  uint64
	proposerLookahead   solid.Uint64VectorSSZ
	postPullupJustified solid.Checkpoint
	postPullupFinalized solid.Checkpoint
	pullupErr           error
	// attestingIndices follows the order of the block's body attestations.
	attestingIndices []preparedAttestingIndices
}

// preparePostState reads no store state. It mutates the post-state's justification fields for the pull-up
// and restores them on success, so the caller must own the post-state.
func preparePostState(postState *state.CachingBeaconState, block *cltypes.BeaconBlock) *preparedPostState {
	cfg := postState.BeaconConfig()
	prepared := &preparedPostState{
		participationEpoch: state.Epoch(postState),
		participation:      postState.CurrentEpochParticipation().Copy(),
		preverifiedSizes: preverifiedAppendListsSizes{
			validatorLength:           uint64(postState.ValidatorLength()),
			historicalRootsLength:     postState.HistoricalRootsLength(),
			historicalSummariesLength: postState.HistoricalSummariesLength(),
		},
		finalityCheckpoints: finalityCheckpoints{
			finalizedCheckpoint:         postState.FinalizedCheckpoint(),
			currentJustifiedCheckpoint:  postState.CurrentJustifiedCheckpoint(),
			previousJustifiedCheckpoint: postState.PreviousJustifiedCheckpoint(),
		},
		totalActiveBalance: postState.GetTotalActiveBalance(),
		proposerLookahead:  solid.NewUint64VectorSSZ(postState.ProposerLookahead().Length()),
	}
	postState.ProposerLookahead().CopyTo(prepared.proposerLookahead)
	if postState.Slot()%cfg.SlotsPerEpoch == 0 {
		prepared.randaoMixes = solid.NewHashVector(int(cfg.EpochsPerHistoricalVector))
		postState.RandaoMixes().CopyTo(prepared.randaoMixes)
	} else {
		prepared.randaoDelta = randaoDelta{
			epoch: state.Epoch(postState),
			delta: postState.GetRandaoMixes(state.Epoch(postState)),
		}
	}

	justificationBits := postState.JustificationBits().Copy()
	if err := statechange.ProcessJustificationBitsAndFinality(postState, nil); err != nil {
		prepared.pullupErr = err
		return prepared
	}
	prepared.postPullupJustified = postState.CurrentJustifiedCheckpoint()
	prepared.postPullupFinalized = postState.FinalizedCheckpoint()
	postState.SetPreviousJustifiedCheckpoint(prepared.finalityCheckpoints.previousJustifiedCheckpoint)
	postState.SetCurrentJustifiedCheckpoint(prepared.finalityCheckpoints.currentJustifiedCheckpoint)
	postState.SetFinalizedCheckpoint(prepared.finalityCheckpoints.finalizedCheckpoint)
	postState.SetJustificationBits(justificationBits)

	prepared.attestingIndices = make([]preparedAttestingIndices, 0, block.Body.Attestations.Len())
	block.Body.Attestations.Range(func(_ int, attestation *solid.Attestation, _ int) bool {
		indices, err := postState.GetAttestingIndicies(attestation, true)
		prepared.attestingIndices = append(prepared.attestingIndices, preparedAttestingIndices{indices: indices, err: err})
		return true
	})

	return prepared
}

// applyBlockImportLocked applies the store writes of a block import under f.mu, in the order on_block applies them.
func (f *ForkChoiceStore) applyBlockImportLocked(
	block *cltypes.BeaconBlock,
	blockRoot common.Hash,
	headBeforeBlock common.Hash,
	storeTimeMs uint64,
	postState *state.CachingBeaconState,
	prepared *preparedPostState,
) (pendingEnvelopeFound bool, err error) {
	blockEpoch := f.computeEpochAtSlot(block.Slot)
	blockVersion := f.beaconCfg.GetCurrentStateVersion(blockEpoch)
	f.updateChildren(block.Slot-1, block.ParentRoot, blockRoot) // parent slot can be innacurate
	if block.Body.ExecutionPayload != nil {
		f.eth2Roots.Add(blockRoot, block.Body.ExecutionPayload.BlockHash)
	} else if blockVersion >= clparams.GloasVersion {
		// [New in Gloas:EIP7732] No ExecutionPayload in beacon block (GLOAS separates them).
		// Inherit parent's execution head as a placeholder so FCU sends a valid (non-zero) hash.
		// OnExecutionPayload will overwrite this with the real payload hash if the envelope arrives (FULL path).
		// If the envelope never arrives (EMPTY path), the parent hash is the correct EL head anyway.
		if parentHash, ok := f.eth2Roots.Get(block.ParentRoot); ok {
			f.eth2Roots.Add(blockRoot, parentHash)
		}
	}
	// Remove the parent from the head set
	delete(f.headSet, block.ParentRoot)
	f.headSet[blockRoot] = struct{}{}
	// record_block_timeliness: store [block_timely, ptc_timely] vector.
	// [Modified in Gloas:EIP7732] Post-GLOAS stores a two-element timeliness vector;
	// pre-GLOAS stores [block_timely, false]. See recordBlockTimeliness for details.
	currentSlot := f.recordBlockTimeliness(block, blockRoot, storeTimeMs)
	// update_proposer_boost_root: conditionally set proposer boost root.
	f.updateProposerBoostRoot(headBeforeBlock, blockRoot, currentSlot)

	// [New in Gloas:EIP7732] GLOAS-specific on_block logic (post state transition)
	if blockVersion >= clparams.GloasVersion {
		// Initialize payload timeliness and data availability votes for this block
		f.payloadTimelinessVote.Store(blockRoot, [clparams.PtcSize]int8{})
		f.payloadDataAvailabilityVote.Store(blockRoot, [clparams.PtcSize]int8{})
		if block.Body.PayloadAttestations != nil {
			f.notifyPtcMessages(postState, block.Body.PayloadAttestations)
		}
		if _, ok := f.pendingLocalSelfBuildEnvelopes.Get(blockRoot); ok {
			pendingEnvelopeFound = true
		} else if _, ok := f.pendingEnvelopes.Get(blockRoot); ok {
			pendingEnvelopeFound = true
		}
	}

	if prepared.randaoMixes != nil {
		// Update randao mixes
		f.randaoMixesLists.Add(blockRoot, prepared.randaoMixes)
	} else {
		f.randaoDeltas.Add(blockRoot, prepared.randaoDelta)
	}
	f.participation.Add(prepared.participationEpoch, prepared.participation)
	f.preverifiedSizes.Add(blockRoot, prepared.preverifiedSizes)
	f.finalityCheckpoints.Add(blockRoot, prepared.finalityCheckpoints)
	if err := f.addPendingConsolidations(blockRoot, postState.PendingConsolidations()); err != nil {
		return false, err
	}
	if err := f.addPendingDeposits(blockRoot, postState.PendingDeposits()); err != nil {
		return false, err
	}
	if err := f.addPendingPartialWithdrawals(blockRoot, postState.PendingPartialWithdrawals()); err != nil {
		return false, err
	}
	if _, ok := f.proposerLookahead.Get(blockEpoch); !ok {
		f.proposerLookahead.Add(blockEpoch, prepared.proposerLookahead)
	}
	f.totalActiveBalances.Add(blockRoot, prepared.totalActiveBalance)
	// Update checkpoints
	f.updateCheckpoints(prepared.finalityCheckpoints.currentJustifiedCheckpoint, prepared.finalityCheckpoints.finalizedCheckpoint)
	if prepared.pullupErr != nil {
		return false, prepared.pullupErr
	}
	f.unrealizedJustifications.Store(blockRoot, prepared.postPullupJustified)
	f.unrealizedFinalizations.Store(blockRoot, prepared.postPullupFinalized)
	f.updateUnrealizedCheckpoints(prepared.postPullupJustified, prepared.postPullupFinalized)
	// Spec compute_pulled_up_tip: if the block is from a prior epoch, apply the post-pull-up checkpoints to the store.
	currentEpoch := f.computeEpochAtSlot(currentSlot)
	if blockEpoch < currentEpoch {
		f.updateCheckpoints(prepared.postPullupJustified, prepared.postPullupFinalized)
	}

	block.Body.Attestations.Range(func(i int, attestation *solid.Attestation, _ int) bool {
		source := &blockAttestationSource{state: postState, root: blockRoot, attestingIndices: prepared.attestingIndices[i]}
		if err := f.onAttestationLocked(attestation, true, source); err != nil {
			log.Debug("[OnBlock] block attestation not applied", "slot", block.Slot, "err", err)
		}
		return true
	})
	return pendingEnvelopeFound, nil
}
