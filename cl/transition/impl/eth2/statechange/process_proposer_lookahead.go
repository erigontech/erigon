package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func ProcessProposerLookahead(s abstract.BeaconState) error {
	lookahead := s.GetProposerLookahead()
	lastEpochStart := lookahead.Length() - int(s.BeaconConfig().SlotsPerEpoch)

	// Shift out proposers in the first epoch
	//copy(lookahead[:lastEpochStart], lookahead[s.BeaconConfig().SlotsPerEpoch:])
	newLookahead := solid.NewUint64VectorSSZ(int(s.BeaconConfig().MinSeedLookahead+1) * int(s.BeaconConfig().SlotsPerEpoch))
	for i := 0; i < lastEpochStart; i++ {
		newLookahead.Set(i, lookahead.Get(i+int(s.BeaconConfig().SlotsPerEpoch)))
	}

	// Fill in the last epoch with new proposer indices
	currentEpoch := s.Slot() / s.BeaconConfig().SlotsPerEpoch
	lastEpochProposers, err := s.GetBeaconProposerIndices(currentEpoch + s.BeaconConfig().MinSeedLookahead + 1)
	if err != nil {
		return err
	}
	for i := 0; i < len(lastEpochProposers); i++ {
		newLookahead.Set(lastEpochStart+i, lastEpochProposers[i])
	}
	s.SetProposerLookahead(newLookahead)
	return nil
}
