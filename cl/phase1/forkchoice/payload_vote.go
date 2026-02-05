package forkchoice

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

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
