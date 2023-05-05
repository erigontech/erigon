package transition

import (
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func ProcessEth1DataReset(s *state.BeaconState) {
	nextEpoch := state.Epoch(s.BeaconState) + 1
	if nextEpoch%s.BeaconConfig().EpochsPerEth1VotingPeriod == 0 {
		s.ResetEth1DataVotes()
	}
}

func ProcessSlashingsReset(s *state.BeaconState) {
	s.SetSlashingSegmentAt(int(state.Epoch(s.BeaconState)+1)%int(s.BeaconConfig().EpochsPerSlashingsVector), 0)

}

func ProcessRandaoMixesReset(s *state.BeaconState) {
	currentEpoch := state.Epoch(s.BeaconState)
	nextEpoch := state.Epoch(s.BeaconState) + 1
	s.SetRandaoMixAt(int(nextEpoch%s.BeaconConfig().EpochsPerHistoricalVector), s.GetRandaoMixes(currentEpoch))
}

func ProcessParticipationFlagUpdates(state *state.BeaconState) {
	state.ResetEpochParticipation()
}
