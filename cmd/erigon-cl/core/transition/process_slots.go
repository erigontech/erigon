package transition

import (
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (s *StateTransistor) transitionState(state *cltypes.BeaconStateBellatrix, block *cltypes.SignedBeaconBlockBellatrix, validate bool) error {
	cur_block := block.Block
	s.processSlots(state, cur_block.Slot)
	if validate {
		valid, err := s.verifyBlockSignature(state, block)
		if err != nil {
			return fmt.Errorf("error validating block signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("block not valid")
		}
	}
	// TODO add logic to process block and update state.
	if validate {
		expectedStateRoot, err := state.HashTreeRoot()
		if err != nil {
			return fmt.Errorf("unable to generate state root: %v", err)
		}
		if expectedStateRoot != cur_block.StateRoot {
			return fmt.Errorf("expected state root differs from received state root")
		}
	}
	return nil
}

// transitionSlot is called each time there is a new slot to process
func (s *StateTransistor) transitionSlot(state *cltypes.BeaconStateBellatrix) error {
	previousStateRoot, err := state.HashTreeRoot()
	if err != nil {
		return err
	}
	state.StateRoots[state.Slot%s.beaconConfig.SlotsPerHistoricalRoot] = previousStateRoot
	if state.LatestBlockHeader.Root == [32]byte{} {
		state.LatestBlockHeader.Root = previousStateRoot
	}
	previousBlockRoot, err := state.LatestBlockHeader.HashTreeRoot()
	if err != nil {
		return err
	}
	state.BlockRoots[state.Slot%s.beaconConfig.SlotsPerHistoricalRoot] = previousBlockRoot
	return nil
}

func (s *StateTransistor) processSlots(state *cltypes.BeaconStateBellatrix, slot uint64) error {
	if slot <= state.Slot {
		return fmt.Errorf("new slot: %d not greater than state slot: %d", slot, state.Slot)
	}
	// Process each slot.
	for i := state.Slot; i < slot; i++ {
		err := s.transitionSlot(state)
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}
		// TODO: add logic to process epoch updates.
		state.Slot += 1
	}
	return nil
}

func (s *StateTransistor) verifyBlockSignature(state *cltypes.BeaconStateBellatrix, block *cltypes.SignedBeaconBlockBellatrix) (bool, error) {
	proposer := state.Validators[block.Block.ProposerIndex]
	signing_root, err := block.Block.Body.HashTreeRoot()
	if err != nil {
		return false, err
	}
	return bls.Verify(block.Signature[:], signing_root[:], proposer.PublicKey[:])
}
