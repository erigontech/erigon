package transition

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// transitionSlot is called each time there is a new slot to process
func (s *StateTransistor) transitionSlot(state *cltypes.BeaconState) error {
	previousStateRoot, err := state.HashTreeRoot()
	if err != nil {
		return err
	}
	state.StateRoots[state.Slot/s.beaconConfig.SlotsPerHistoricalRoot] = previousStateRoot
	if state.LatestBlockHeader.Root == [32]byte{} {
		state.LatestBlockHeader.Root = previousStateRoot
	}
	previousBlockRoot, err := state.LatestBlockHeader.HashTreeRoot()
	if err != nil {
		return err
	}
	state.BlockRoots[state.Slot/s.beaconConfig.SlotsPerHistoricalRoot] = previousBlockRoot
	return nil
}
