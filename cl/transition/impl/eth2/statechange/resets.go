package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func ProcessEth1DataReset(s abstract.BeaconState) {
	nextEpoch := state.Epoch(s) + 1
	if nextEpoch%s.BeaconConfig().EpochsPerEth1VotingPeriod == 0 {
		s.ResetEth1DataVotes()
	}
}

func ProcessSlashingsReset(s abstract.BeaconState) {
	s.SetSlashingSegmentAt(int(state.Epoch(s)+1)%int(s.BeaconConfig().EpochsPerSlashingsVector), 0)

}

func ProcessRandaoMixesReset(s abstract.BeaconState) {
	currentEpoch := state.Epoch(s)
	nextEpoch := state.Epoch(s) + 1
	s.SetRandaoMixAt(int(nextEpoch%s.BeaconConfig().EpochsPerHistoricalVector), s.GetRandaoMixes(currentEpoch))
}

func ProcessParticipationFlagUpdates(state abstract.BeaconState) {
	state.ResetEpochParticipation()
}
