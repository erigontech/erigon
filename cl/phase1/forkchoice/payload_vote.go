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

// boolToVote converts a bool to the three-state vote representation:
// true → 1, false → -1. Zero means unvoted.
func boolToVote(b bool) int8 {
	if b {
		return 1
	}
	return -1
}

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
			ptc, err := blockState.GetPTCFromWindow(data.Slot)
			if err != nil {
				continue
			}
			cached = &cachedPTC{ptc: ptc}
			ptcCache[blockRoot] = cached
		}

		if payloadAttestation.AggregationBits == nil {
			continue
		}

		for j := range cached.ptc {
			if payloadAttestation.AggregationBits.GetBitAt(j) {
				f.applyPayloadAttestationVote(j, data, blockRoot)
			}
		}
	}
}

// applyPayloadAttestationVote updates PTC vote tracking for a single PTC position.
// ptcIndex is the position in the PTC (the aggregation bit index).
func (f *ForkChoiceStore) applyPayloadAttestationVote(
	ptcIndex int,
	data *cltypes.PayloadAttestationData,
	blockRoot common.Hash,
) {
	// Atomically update PTC vote arrays under mutex to prevent concurrent
	// Load→modify→Store from losing votes. See also OnPayloadAttestationMessage.
	f.ptcVoteMu.Lock()

	var timelinessVotes [clparams.PtcSize]int8
	if existing, ok := f.payloadTimelinessVote.Load(blockRoot); ok {
		timelinessVotes = existing.([clparams.PtcSize]int8)
	}
	var dataAvailabilityVotes [clparams.PtcSize]int8
	if existing, ok := f.payloadDataAvailabilityVote.Load(blockRoot); ok {
		dataAvailabilityVotes = existing.([clparams.PtcSize]int8)
	}
	timelinessVotes[ptcIndex] = boolToVote(data.PayloadPresent)
	dataAvailabilityVotes[ptcIndex] = boolToVote(data.BlobDataAvailable)
	f.payloadTimelinessVote.Store(blockRoot, timelinessVotes)
	f.payloadDataAvailabilityVote.Store(blockRoot, dataAvailabilityVotes)

	f.ptcVoteMu.Unlock()
}

// payloadTimeliness returns whether the PTC voted the payload as timely (timely=true)
// or not timely (timely=false) for the given beacon block root.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) payloadTimeliness(root common.Hash, timely bool) bool {
	voteRaw, ok := f.payloadTimelinessVote.Load(root)
	if !ok {
		return false
	}

	// If the payload has not been accepted by the execution layer, the payload
	// is not considered available regardless of the PTC vote.
	if !f.IsPayloadVerified(root) {
		return false
	}
	votes := voteRaw.([clparams.PtcSize]int8)
	target := boolToVote(timely)
	count := uint64(0)
	for i := range votes {
		if votes[i] == target {
			count++
		}
	}
	return count > f.beaconCfg.PtcSize/2
}

// payloadDataAvailability returns whether the PTC voted blob data as available (available=true)
// or unavailable (available=false) for the given beacon block root.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) payloadDataAvailability(root common.Hash, available bool) bool {
	voteRaw, ok := f.payloadDataAvailabilityVote.Load(root)
	if !ok {
		return false
	}

	// If the payload has not been accepted by the execution layer, the blob data
	// is not considered available regardless of the PTC vote.
	if !f.IsPayloadVerified(root) {
		return false
	}
	votes := voteRaw.([clparams.PtcSize]int8)
	target := boolToVote(available)
	count := uint64(0)
	for i := range votes {
		if votes[i] == target {
			count++
		}
	}
	return count > f.beaconCfg.PtcSize/2
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

func (f *ForkChoiceStore) getNodeForRoot(root common.Hash) ForkChoiceNode {
	return f.getForkChoiceNode(root, cltypes.PayloadStatusPending)
}

func (f *ForkChoiceStore) getForkChoiceNode(root common.Hash, payloadStatus cltypes.PayloadStatus) ForkChoiceNode {
	return ForkChoiceNode{Root: root, PayloadStatus: payloadStatus}
}

func (f *ForkChoiceStore) getSupportedNode(message LatestMessage) ForkChoiceNode {
	block, has := f.forkGraph.GetBlock(message.Root)
	if !has || block == nil {
		return ForkChoiceNode{Root: common.Hash{}, PayloadStatus: cltypes.PayloadStatusPending}
	}
	if block.Block.Slot >= message.Slot {
		return ForkChoiceNode{Root: message.Root, PayloadStatus: cltypes.PayloadStatusPending}
	}
	if message.PayloadPresent {
		return ForkChoiceNode{Root: message.Root, PayloadStatus: cltypes.PayloadStatusFull}
	}
	return ForkChoiceNode{Root: message.Root, PayloadStatus: cltypes.PayloadStatusEmpty}
}

func (f *ForkChoiceStore) isAncestor(node ForkChoiceNode, ancestor ForkChoiceNode) bool {
	ancestorHeader, has := f.forkGraph.GetHeader(ancestor.Root)
	if !has {
		return false
	}
	nodeAncestor := f.getAncestor(node, ancestorHeader.Slot)
	if nodeAncestor.Root != ancestor.Root {
		return false
	}
	return nodeAncestor.PayloadStatus == ancestor.PayloadStatus ||
		ancestor.PayloadStatus == cltypes.PayloadStatusPending
}

// isPreviousSlotPayloadDecision identifies the special GLOAS fork-choice case
// where EMPTY/FULL variants of a previous-slot block are decided by the payload
// tiebreaker rather than by weight.
func (f *ForkChoiceStore) isPreviousSlotPayloadDecision(node ForkChoiceNode) bool {
	if node.PayloadStatus != cltypes.PayloadStatusEmpty && node.PayloadStatus != cltypes.PayloadStatusFull {
		return false
	}
	block, has := f.forkGraph.GetBlock(node.Root)
	if !has || block == nil {
		return false
	}
	return block.Block.Slot+1 == f.Slot()
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
	if !f.IsPayloadVerified(root) {
		return false
	}

	// Check if payload is timely AND blob data is available
	if f.payloadTimeliness(root, true) && f.payloadDataAvailability(root, true) {
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

// ShouldBuildOnFull returns whether the proposer should build on the full payload
// for the given head node. Returns false for EMPTY heads. For FULL heads, returns
// true unless the PTC voted the payload as late or blob data as unavailable.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) ShouldBuildOnFull(head ForkChoiceNode) bool {
	if head.PayloadStatus == cltypes.PayloadStatusEmpty {
		return false
	}
	if head.PayloadStatus == cltypes.PayloadStatusPending {
		return false
	}
	if !f.isPreviousSlotPayloadDecision(head) {
		return true
	}
	if f.payloadDataAvailability(head.Root, false) {
		return false
	}
	if f.payloadTimeliness(head.Root, false) {
		return false
	}
	return true
}

// getPayloadStatusTiebreaker returns a tiebreaker value for fork choice comparison.
// Used to decide between chains with different payload statuses.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) getPayloadStatusTiebreaker(node ForkChoiceNode) uint8 {
	if !f.isPreviousSlotPayloadDecision(node) {
		return uint8(node.PayloadStatus)
	}

	if node.PayloadStatus == cltypes.PayloadStatusEmpty {
		return 1
	}
	if f.ShouldExtendPayload(node.Root) {
		return 2
	}
	return 0
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
		if f.IsPayloadVerified(node.Root) {
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
