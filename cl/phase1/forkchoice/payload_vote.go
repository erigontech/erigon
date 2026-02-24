package forkchoice

import (
	"errors"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
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

	for i := 0; i < payloadAttestations.Len(); i++ {
		payloadAttestation := payloadAttestations.Get(i)
		indexedPayloadAttestation, err := s.GetIndexedPayloadAttestation(payloadAttestation)
		if err != nil {
			continue
		}

		attestingIndices := indexedPayloadAttestation.AttestingIndices
		for j := 0; j < attestingIndices.Length(); j++ {
			idx := attestingIndices.Get(j)
			msg := &cltypes.PayloadAttestationMessage{
				ValidatorIndex: idx,
				Data:           payloadAttestation.Data,
				Signature:      common.Bytes96{}, // Empty signature since it's from block
			}
			// Ignore errors for in-block attestations - they've already been validated
			_ = f.OnPayloadAttestationMessage(msg, true)
		}
	}
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

	return presentCount > clparams.PayloadTimelyThreshold
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

	return availableCount > clparams.DataAvailabilityTimelyThreshold
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
		return cltypes.PayloadStatusFull
	}
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

// shouldExtendPayload returns whether the payload for the given root should be extended.
// Returns true if:
// - The payload is timely AND blob data is available (received enough PTC votes for both), OR
// - There's no proposer boost root, OR
// - The proposer boost root's parent is not this root, OR
// - The proposer boost root's parent node has FULL payload status
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) shouldExtendPayload(root common.Hash) bool {
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
	// between FULL and EMPTY based on shouldExtendPayload
	if node.PayloadStatus == cltypes.PayloadStatusEmpty {
		return 1
	}
	// FULL case
	if f.shouldExtendPayload(node.Root) {
		return 2
	}
	return 0 // Treat as PENDING (lower priority)
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
		// Check disk for envelope existence to determine if FULL status is available
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
		// Parent is FULL - verify execution payload envelope exists on disk
		if !f.forkGraph.HasEnvelope(block.ParentRoot) {
			return errors.New("parent execution payload state not found for FULL parent")
		}
	} else {
		// Parent is EMPTY - verify bid.parent_block_hash == parent_bid.parent_block_hash
		parentBlock, ok := f.forkGraph.GetBlock(block.ParentRoot)
		if !ok || parentBlock == nil {
			return errors.New("parent block not found")
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
