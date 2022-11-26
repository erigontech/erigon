package transition

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// transitionSlot is called each time there is a new slot to process
func (s *StateTransistor) transitionSlot(state *cltypes.BeaconState) error {
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

func (s *StateTransistor) processSlots(state *cltypes.BeaconState, slot uint64) error {
	if slot <= state.Slot {
		return fmt.Errorf("new slot: %d not greater than state slot: %d", slot, state.Slot)
	}
	// Process each slot
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
