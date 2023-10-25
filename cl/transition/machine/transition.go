package machine

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// TransitionState will call impl..ProcessSlots, then impl.VerifyBlockSignature, then ProcessBlock, then impl.VerifyTransition
func TransitionState(impl Processor, s abstract.BeaconState, block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	if err := impl.ProcessSlots(s, currentBlock.Slot); err != nil {
		return err
	}

	if impl, ok := impl.(BlockValidator); ok {
		if err := impl.VerifyBlockSignature(s, block); err != nil {
			return err
		}
	}

	// Transition block
	if err := ProcessBlock(impl, s, block); err != nil {
		return err
	}

	if impl, ok := impl.(BlockValidator); ok {
		if currentBlock.Version() >= clparams.DenebVersion {
			verified, err := impl.VerifyKzgCommitmentsAgainstTransactions(currentBlock.Body.ExecutionPayload.Transactions, currentBlock.Body.BlobKzgCommitments)
			if err != nil {
				return fmt.Errorf("processBlock: failed to process blob kzg commitments: %w", err)
			}
			if !verified {
				return fmt.Errorf("processBlock: failed to process blob kzg commitments: commitments are not equal")
			}
		}

		// perform validation
		if err := impl.VerifyTransition(s, currentBlock); err != nil {
			return err
		}
	}

	// if validation is successful, transition
	s.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
}
