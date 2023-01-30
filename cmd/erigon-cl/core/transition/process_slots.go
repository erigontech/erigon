package transition

import (
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (s *StateTransistor) transitionState(block *cltypes.SignedBeaconBlock) error {
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
	// TODO add logic to process block and update state.
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
		// TODO: add logic to process epoch updates.
		stateSlot += 1
		s.state.SetSlot(stateSlot)
	}
	return nil
}

func (s *StateTransistor) verifyBlockSignature(block *cltypes.SignedBeaconBlock) (bool, error) {
	proposer := s.state.ValidatorAt(int(block.Block.ProposerIndex))
	sigRoot, err := block.Block.Body.HashSSZ()
	if err != nil {
		return false, err
	}
	sig := block.Signature
	return bls.Verify(sig[:], sigRoot[:], proposer.PublicKey[:])
}
