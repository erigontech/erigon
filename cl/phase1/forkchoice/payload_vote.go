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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
)

func (f *ForkChoiceStore) calculateCommitteeFraction(s *state.CachingBeaconState, committeePercent uint64) uint64 {
	committeeWeight := s.GetTotalActiveBalance() / f.beaconCfg.SlotsPerEpoch
	return (committeeWeight * committeePercent) / 100
}

// notifyPtcMessages extracts a list of PayloadAttestationMessage from payload_attestations
// and updates the store with them. These Payload attestations are assumed to be in the
// beacon block hence signature verification is not needed.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) notifyPtcMessages(
	s *state.CachingBeaconState,
	payloadAttestations *solid.ListSSZ[*cltypes.PayloadAttestation],
) {
	if s.Slot() == 0 {
		return
	}
	if payloadAttestations == nil {
		return
	}

	// Pre-compute PTC per unique blockRoot to avoid redundant state lookups
	// for every attesting validator (PtcSize can be 512).
	type cachedPTC struct {
		ptc []uint64
	}
	ptcCache := make(map[common.Hash]*cachedPTC)

	for i := 0; i < payloadAttestations.Len(); i++ {
		payloadAttestation := payloadAttestations.Get(i)
		if payloadAttestation.Data == nil {
			continue
		}
		data := payloadAttestation.Data
		blockRoot := data.BeaconBlockRoot

		cached, ok := ptcCache[blockRoot]
		if !ok {
			blockState, err := f.forkGraph.GetState(blockRoot, false)
			if err != nil || blockState == nil {
				continue
			}
			if data.Slot != blockState.Slot() {
				continue
			}
			ptc, ok := readPTCFromWindow(blockState, data.Slot)
			if !ok {
				continue
			}
			cached = &cachedPTC{ptc: ptc}
			ptcCache[blockRoot] = cached
		}

		if payloadAttestation.AggregationBits == nil {
			continue
		}

		for j, validatorIndex := range cached.ptc {
			if payloadAttestation.AggregationBits.GetBitAt(j) {
				f.applyPayloadAttestationVote(validatorIndex, data, blockRoot, cached.ptc)
			}
		}
	}
}

func readPTCFromWindow(s *state.CachingBeaconState, slot uint64) ([]uint64, bool) {
	cfg := s.BeaconConfig()
	epoch := state.GetEpochAtSlot(cfg, slot)
	stateEpoch := s.Slot() / cfg.SlotsPerEpoch
	if epoch+1 < stateEpoch || epoch > stateEpoch+1 {
		return nil, false
	}

	slotInEpoch := slot % cfg.SlotsPerEpoch
	var index uint64
	if stateEpoch > 0 && epoch == stateEpoch-1 {
		index = slotInEpoch
	} else {
		index = (epoch-stateEpoch+1)*cfg.SlotsPerEpoch + slotInEpoch
	}

	ptcWindow := s.GetPtcWindow()
	if ptcWindow == nil || index >= uint64(ptcWindow.Length()) {
		return nil, false
	}

	vec := ptcWindow.Get(int(index))
	ptc := make([]uint64, vec.Length())
	for i := 0; i < vec.Length(); i++ {
		ptc[i] = vec.Get(i)
	}
	return ptc, true
}

// applyPayloadAttestationVote updates PTC vote tracking for a single validator.
// Used by notifyPtcMessages with pre-computed PTC to avoid redundant GetState/GetPTC calls.
func (f *ForkChoiceStore) applyPayloadAttestationVote(
	validatorIndex uint64,
	data *cltypes.PayloadAttestationData,
	blockRoot common.Hash,
	ptc []uint64,
) {
	// Find the validator's position in the PTC
	ptcIndex := -1
	for i, idx := range ptc {
		if idx == validatorIndex {
			ptcIndex = i
			break
		}
	}
	if ptcIndex == -1 {
		return
	}

	// Atomically update PTC vote arrays under mutex to prevent concurrent
	// Load→modify→Store from losing votes. See also OnPayloadAttestationMessage.
	f.ptcVoteMu.Lock()

	var timelinessVotes [clparams.PtcSize]bool
	if existing, ok := f.payloadTimelinessVote.Load(blockRoot); ok {
		timelinessVotes = existing.([clparams.PtcSize]bool)
	}
	timelinessVotes[ptcIndex] = data.PayloadPresent
	f.payloadTimelinessVote.Store(blockRoot, timelinessVotes)

	var dataAvailabilityVotes [clparams.PtcSize]bool
	if existing, ok := f.payloadDataAvailabilityVote.Load(blockRoot); ok {
		dataAvailabilityVotes = existing.([clparams.PtcSize]bool)
	}
	dataAvailabilityVotes[ptcIndex] = data.BlobDataAvailable
	f.payloadDataAvailabilityVote.Store(blockRoot, dataAvailabilityVotes)

	f.ptcVoteMu.Unlock()
}

// isPayloadTimely returns whether the execution payload for the beacon block with root
// was voted as present by the PTC, and was locally determined to be available.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isPayloadTimely(root common.Hash) bool {
	// The beacon block root must be known in payload_timeliness_vote
	voteRaw, ok := f.payloadTimelinessVote.Load(root)
	if !ok {
		return false
	}

	// If the payload is not locally available, the payload
	// is not considered available regardless of the PTC vote
	if !f.forkGraph.HasEnvelope(root) {
		return false
	}

	// Count PTC votes for payload present
	votes := voteRaw.([clparams.PtcSize]bool)
	presentCount := uint64(0)
	for i := range votes {
		if votes[i] {
			presentCount++
		}
	}

	return presentCount > f.beaconCfg.PtcSize/2
}

// isPayloadDataAvailable returns whether the blob data for the beacon block with root
// was voted as present by the PTC, and was locally determined to be available.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isPayloadDataAvailable(root common.Hash) bool {
	// The beacon block root must be known in payload_data_availability_vote
	voteRaw, ok := f.payloadDataAvailabilityVote.Load(root)
	if !ok {
		return false
	}

	// If the payload is not locally available, the blob data
	// is not considered available regardless of the PTC vote
	if !f.forkGraph.HasEnvelope(root) {
		return false
	}

	// Count PTC votes for data available
	votes := voteRaw.([clparams.PtcSize]bool)
	availableCount := uint64(0)
	for i := range votes {
		if votes[i] {
			availableCount++
		}
	}

	return availableCount > f.beaconCfg.PtcSize/2
}

// getParentPayloadStatus returns the payload status of the parent block.
// Compares current block's parent_block_hash with parent block's block_hash from their bids.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) getParentPayloadStatus(block *cltypes.BeaconBlock) cltypes.PayloadStatus {
	// Get the parent block
	parentBlock, ok := f.forkGraph.GetBlock(block.ParentRoot)
	if !ok || parentBlock == nil {
		return cltypes.PayloadStatusEmpty
	}

	// Pre-GLOAS parent blocks have no bid field. From the GLOAS fork choice
	// perspective they are treated as EMPTY: they always carried their execution
	// payload inline (no separate envelope), so the PENDING → EMPTY/FULL
	// traversal in getNodeChildren produces only an EMPTY child for them
	// (HasEnvelope returns false). Returning EMPTY here lets the next
	// generation of blocks (whose parent is pre-GLOAS) be found by the
	// EMPTY branch, unblocking the fork-choice head from advancing past the
	// GLOAS fork boundary.
	if parentBlock.Block.Body.Version < clparams.GloasVersion {
		return cltypes.PayloadStatusEmpty
	}

	// Get parent_block_hash from current block's signed_execution_payload_bid
	currentBid := block.Body.GetSignedExecutionPayloadBid()
	if currentBid == nil || currentBid.Message == nil {
		return cltypes.PayloadStatusEmpty
	}
	parentBlockHash := currentBid.Message.ParentBlockHash

	// Get message_block_hash from parent block's signed_execution_payload_bid
	parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
	if parentBid == nil || parentBid.Message == nil {
		return cltypes.PayloadStatusEmpty
	}
	messageBlockHash := parentBid.Message.BlockHash

	// Compare parent_block_hash with message_block_hash
	if parentBlockHash == messageBlockHash {
		log.Trace("[getParentPayloadStatus] FULL",
			"slot", block.Slot, "parentBlockHash", parentBlockHash, "messageBlockHash", messageBlockHash)
		return cltypes.PayloadStatusFull
	}
	log.Trace("[getParentPayloadStatus] EMPTY",
		"slot", block.Slot, "parentBlockHash", parentBlockHash, "messageBlockHash", messageBlockHash,
		"parentRoot", block.ParentRoot)
	return cltypes.PayloadStatusEmpty
}

// isParentNodeFull returns true if the parent block has a FULL payload status.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isParentNodeFull(block *cltypes.BeaconBlock) bool {
	return f.getParentPayloadStatus(block) == cltypes.PayloadStatusFull
}

// isSupportingVote returns whether a vote for message.Root supports the chain
// containing the beacon block node.Root with the payload contents indicated by
// node.PayloadStatus as head.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isSupportingVote(node ForkChoiceNode, message LatestMessage) bool {
	block, has := f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return false
	}

	if node.Root == message.Root {
		// Same root case
		if node.PayloadStatus == cltypes.PayloadStatusPending {
			return true
		}
		if message.Slot <= block.Block.Slot {
			return false
		}
		if message.PayloadPresent {
			return node.PayloadStatus == cltypes.PayloadStatusFull
		} else {
			return node.PayloadStatus == cltypes.PayloadStatusEmpty
		}
	} else {
		// Different root case - check ancestor
		ancestor := f.Ancestor(message.Root, block.Block.Slot)
		return node.Root == ancestor.Root && (node.PayloadStatus == cltypes.PayloadStatusPending ||
			node.PayloadStatus == ancestor.PayloadStatus)
	}
}

// ShouldExtendPayload returns whether the payload for the given root should be extended.
// Returns true if:
// - The payload is timely AND blob data is available (received enough PTC votes for both), OR
// - There's no proposer boost root, OR
// - The proposer boost root's parent is not this root, OR
// - The proposer boost root's parent node has FULL payload status
// Used by prepare_execution_payload to decide FULL vs EMPTY path.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) ShouldExtendPayload(root common.Hash) bool {
	// is_payload_verified: the envelope must exist locally
	if !f.forkGraph.HasEnvelope(root) {
		return false
	}

	// Check if payload is timely AND blob data is available
	if f.isPayloadTimely(root) && f.isPayloadDataAvailable(root) {
		return true
	}

	// Get proposer boost root
	proposerRoot := f.ProposerBoostRoot()

	// If no proposer boost root, return true
	if proposerRoot == (common.Hash{}) {
		return true
	}

	// Get the proposer boost block
	proposerBlock, has := f.forkGraph.GetBlock(proposerRoot)
	if !has || proposerBlock == nil {
		return true
	}

	// If proposer boost root's parent is not this root, return true
	if proposerBlock.Block.ParentRoot != root {
		return true
	}

	// Check if parent node is full
	return f.isParentNodeFull(proposerBlock.Block)
}

// getPayloadStatusTiebreaker returns a tiebreaker value for fork choice comparison.
// Used to decide between chains with different payload statuses.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) getPayloadStatusTiebreaker(node ForkChoiceNode) uint8 {
	// If status is PENDING, return as-is
	if node.PayloadStatus == cltypes.PayloadStatusPending {
		return uint8(node.PayloadStatus)
	}

	// Get the block to check its slot
	block, has := f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return uint8(node.PayloadStatus)
	}

	// If block is not from the previous slot, return status as-is
	if block.Block.Slot+1 != f.Slot() {
		return uint8(node.PayloadStatus)
	}

	// To decide on a payload from the previous slot, choose
	// between FULL and EMPTY based on ShouldExtendPayload
	if f.ShouldExtendPayload(node.Root) {
		// should_extend: identity — keep status quo
		return uint8(node.PayloadStatus)
	}
	// !should_extend: swap FULL <-> EMPTY
	if node.PayloadStatus == cltypes.PayloadStatusFull {
		return uint8(cltypes.PayloadStatusEmpty)
	}
	return uint8(cltypes.PayloadStatusFull)
}

// getNodeChildren returns the children of a fork choice node.
// For PENDING nodes, returns EMPTY and possibly FULL variants of the same root.
// For EMPTY/FULL nodes, returns child blocks with PENDING status.
// blocks is the filtered block tree from get_head.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) getNodeChildren(node ForkChoiceNode, blocks map[common.Hash]*cltypes.BeaconBlockHeader) []ForkChoiceNode {
	if node.PayloadStatus == cltypes.PayloadStatusPending {
		children := []ForkChoiceNode{
			{Root: node.Root, PayloadStatus: cltypes.PayloadStatusEmpty},
		}
		// Check disk for envelope existence to determine if FULL status is available.
		// Spec: is_payload_verified(store, node.root) — in the spec, store.payloads is populated
		// only after EL returns VALID. During forward sync the EL returns SYNCING for most
		// payloads, so requiring verifiedExecutionPayload would block the FULL path entirely.
		// Using HasEnvelope (disk persistence) as a pragmatic proxy; verifiedExecutionPayload
		// can be added once the EL catches up and validates payloads retroactively.
		if f.forkGraph.HasEnvelope(node.Root) {
			children = append(children, ForkChoiceNode{
				Root: node.Root, PayloadStatus: cltypes.PayloadStatusFull,
			})
		}
		return children
	}

	// EMPTY or FULL → find child blocks from filtered block tree
	result := make([]ForkChoiceNode, 0)
	for root, header := range blocks {
		// Use header.ParentRoot directly from blocks map
		if header.ParentRoot != node.Root {
			continue
		}
		// Need full block for getParentPayloadStatus
		block, ok := f.forkGraph.GetBlock(root)
		if !ok || block == nil {
			continue
		}
		if node.PayloadStatus == f.getParentPayloadStatus(block.Block) {
			result = append(result, ForkChoiceNode{
				Root: root, PayloadStatus: cltypes.PayloadStatusPending,
			})
		}
	}
	return result
}

// validateParentPayloadPath validates that the block builds on the correct parent payload path.
// If parent is FULL, the parent must have an execution payload state.
// If parent is EMPTY, the block's parent_block_hash must match the parent's parent_block_hash.
// Also validates that the parent execution payload is not invalidated.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) validateParentPayloadPath(block *cltypes.BeaconBlock) error {
	currentBid := block.Body.GetSignedExecutionPayloadBid()
	if currentBid == nil || currentBid.Message == nil {
		return errors.New("current block missing execution payload bid")
	}

	// Check if parent execution payload has been invalidated
	parentBlockHash := currentBid.Message.ParentBlockHash
	if status, ok := f.executionPayloadStatus.Get(parentBlockHash); ok {
		if status == execution_client.PayloadStatusInvalidated {
			return errors.New("parent execution payload is invalid")
		}
	}

	if f.isParentNodeFull(block) {
		// Parent is FULL - verify execution payload envelope exists on disk.
		// Return ErrParentEnvelopePending (not a hard error) when the envelope is
		// missing.  During forward sync the envelope may not yet be persisted (it
		// arrives in the same batch or in a later batch), so a hard error would
		// permanently reject the block and ban the peer.
		if !f.forkGraph.HasEnvelope(block.ParentRoot) {
			return ErrParentEnvelopePending
		}
	} else {
		// Parent is EMPTY - verify bid.parent_block_hash == parent_bid.parent_block_hash
		parentBlock, ok := f.forkGraph.GetBlock(block.ParentRoot)
		if !ok || parentBlock == nil {
			// Parent block was pruned from the fork graph or is the anchor block.
			// Spec assumes store.blocks contains all blocks (no pruning), but Erigon
			// prunes old blocks to save memory. Pruned blocks were already validated
			// when they were processed, so skip this validation.
			return nil
		}

		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()

		if parentBid == nil || parentBid.Message == nil {
			// Parent might be pre-GLOAS, skip this check
			return nil
		}

		// Verify parent_block_hash matches
		if currentBid.Message.ParentBlockHash != parentBid.Message.ParentBlockHash {
			return errors.New("bid parent_block_hash mismatch with parent bid")
		}
	}
	return nil
}
