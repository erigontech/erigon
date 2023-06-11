package machine

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// TransitionState will call impl..ProcessSlots, then impl.VerifyBlockSignature, then ProcessBlock, then impl.VerifyTransition
func TransitionState(impl Interface, s *state.BeaconState, block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	if err := impl.ProcessSlots(s, currentBlock.Slot); err != nil {
		return err
	}

	if err := impl.VerifyBlockSignature(s, block); err != nil {
		return err
	}

	// Transition block
	if err := ProcessBlock(impl, s, block); err != nil {
		return err
	}

	// perform validation
	if err := impl.VerifyTransition(s, currentBlock); err != nil {
		return err
	}

	// if validation is successful, transition
	s.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
}
