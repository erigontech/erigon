package transition

import (
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func ProcessEth1DataReset(s *state2.BeaconState) {
	nextEpoch := state2.Epoch(s.BeaconState) + 1
	if nextEpoch%s.BeaconConfig().EpochsPerEth1VotingPeriod == 0 {
		s.ResetEth1DataVotes()
	}
}

func ProcessSlashingsReset(s *state2.BeaconState) {
	s.SetSlashingSegmentAt(int(state2.Epoch(s.BeaconState)+1)%int(s.BeaconConfig().EpochsPerSlashingsVector), 0)

}

func ProcessRandaoMixesReset(s *state2.BeaconState) {
	currentEpoch := state2.Epoch(s.BeaconState)
	nextEpoch := state2.Epoch(s.BeaconState) + 1
	s.SetRandaoMixAt(int(nextEpoch%s.BeaconConfig().EpochsPerHistoricalVector), s.GetRandaoMixes(currentEpoch))
}

func ProcessParticipationFlagUpdates(state *state2.BeaconState) {
	state.ResetEpochParticipation()
}
