package forkchoice

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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
			f.onPayloadAttestationMessage(msg, true)
		}
	}
}

// onPayloadAttestationMessage processes a payload attestation message and updates
// the PTC vote tracking in the store.
// Run upon receiving a new ptc_message from either within a block or directly on the wire.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) onPayloadAttestationMessage(
	msg *cltypes.PayloadAttestationMessage,
	isFromBlock bool,
) error {
	if msg.Data == nil {
		return nil
	}

	data := msg.Data
	blockRoot := data.BeaconBlockRoot

	// PTC attestation must be for a known block
	blockState, err := f.forkGraph.GetState(blockRoot, false)
	if err != nil || blockState == nil {
		return err // Block unknown, delay consideration until block is found
	}

	// Get the PTC for the attestation slot
	ptc, err := blockState.GetPTC(data.Slot)
	if err != nil {
		return err
	}

	// PTC votes can only change the vote for their assigned beacon block
	if data.Slot != blockState.Slot() {
		return nil
	}

	// Check that the attester is from the PTC
	ptcIndex := -1
	for i, idx := range ptc {
		if idx == msg.ValidatorIndex {
			ptcIndex = i
			break
		}
	}
	if ptcIndex == -1 {
		return nil // Validator not in PTC
	}

	// Verify the signature and check that it's for the current slot if coming from wire
	if !isFromBlock {
		// Check that the attestation is for the current slot
		if data.Slot != f.Slot() {
			return nil
		}
		// Verify the signature
		indexedAttestation := &cltypes.IndexedPayloadAttestation{
			AttestingIndices: solid.NewRawUint64List(1, []uint64{msg.ValidatorIndex}),
			Data:             data,
			Signature:        msg.Signature,
		}
		valid, err := state.IsValidIndexedPayloadAttestation(blockState, indexedAttestation)
		if err != nil {
			return err
		}
		if !valid {
			return nil
		}
	}

	// Get or initialize the PTC vote array for this block root
	var ptcVotes [clparams.PtcSize]bool
	if existing, ok := f.ptcVote.Load(blockRoot); ok {
		ptcVotes = existing.([clparams.PtcSize]bool)
	}

	// Update the ptc vote for the block
	ptcVotes[ptcIndex] = data.PayloadPresent
	f.ptcVote.Store(blockRoot, ptcVotes)

	return nil
}

// isPayloadTimely returns whether the execution payload for the beacon block with root
// was voted as present by the PTC, and was locally determined to be available.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) isPayloadTimely(root common.Hash) bool {
	// The beacon block root must be known in ptc_vote
	ptcVoteRaw, ok := f.ptcVote.Load(root)
	if !ok {
		return false
	}

	// If the payload is not locally available, the payload
	// is not considered available regardless of the PTC vote
	if _, ok := f.executionPayloadStates.Load(root); !ok {
		return false
	}

	// Count PTC votes for payload present
	ptcVotes := ptcVoteRaw.([clparams.PtcSize]bool)
	presentCount := uint64(0)
	for _, present := range ptcVotes {
		if present {
			presentCount++
		}
	}

	return presentCount > clparams.PayloadTimelyThreshold
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
// - The payload is timely (received enough PTC votes), OR
// - There's no proposer boost root, OR
// - The proposer boost root's parent is not this root, OR
// - The proposer boost root's parent node has FULL payload status
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) shouldExtendPayload(root common.Hash) bool {
	// Check if payload is timely
	if f.isPayloadTimely(root) {
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
		return uint8(cltypes.PayloadStatusEmpty)
	}
	// FULL case
	if f.shouldExtendPayload(node.Root) {
		return uint8(cltypes.PayloadStatusFull)
	}
	return uint8(cltypes.PayloadStatusPending) // Treat as PENDING (lower priority)
}
