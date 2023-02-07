package transition

import (
	"errors"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
)

func (s *StateTransistor) TransitionState(block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	s.processSlots(currentBlock.Slot)
	if !s.noValidate {
		valid, err := s.verifyBlockSignature(block)
		if err != nil {
			return fmt.Errorf("error validating block signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("block not valid")
		}
	}
	// Transition block
	if err := s.processBlock(block); err != nil {
		return err
	}

	if !s.noValidate {
		expectedStateRoot, err := s.state.HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to generate state root: %v", err)
		}
		if expectedStateRoot != currentBlock.StateRoot {
			return fmt.Errorf("expected state root differs from received state root")
		}
	}
	return nil
}

// transitionSlot is called each time there is a new slot to process
func (s *StateTransistor) transitionSlot() error {
	slot := s.state.Slot()
	previousStateRoot, err := s.state.HashSSZ()
	if err != nil {
		return err
	}
	s.state.SetStateRootAt(int(slot%s.beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := s.state.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		s.state.SetLatestBlockHeader(latestBlockHeader)
	}

	previousBlockRoot, err := s.state.LatestBlockHeader().HashSSZ()
	if err != nil {
		return err
	}
	s.state.SetBlockRootAt(int(slot%s.beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}

func (s *StateTransistor) processSlots(slot uint64) error {
	stateSlot := s.state.Slot()
	if slot <= stateSlot {
		return fmt.Errorf("new slot: %d not greater than state slot: %d", slot, stateSlot)
	}
	// Process each slot.
	for i := stateSlot; i < slot; i++ {
		err := s.transitionSlot()
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}
		// TODO(Someone): Add epoch transition.
		if (stateSlot+1)%s.beaconConfig.SlotsPerEpoch == 0 {
			return errors.New("cannot transition epoch: not implemented")
		}
		// TODO: add logic to process epoch updates.
		stateSlot += 1
		if err := s.state.SetSlot(stateSlot); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateTransistor) verifyBlockSignature(block *cltypes.SignedBeaconBlock) (bool, error) {
	proposer, err := s.state.ValidatorAt(int(block.Block.ProposerIndex))
	if err != nil {
		return false, err
	}
	domain, err := s.state.GetDomain(s.beaconConfig.DomainBeaconProposer, s.state.Epoch())
	if err != nil {
		return false, err
	}
	sigRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	if err != nil {
		return false, err
	}
	return bls.Verify(block.Signature[:], sigRoot[:], proposer.PublicKey[:])
}
