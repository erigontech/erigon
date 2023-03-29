package transition

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func ProcessEth1DataReset(state *state.BeaconState) {
	nextEpoch := state.Epoch() + 1
	if nextEpoch%state.BeaconConfig().EpochsPerEth1VotingPeriod == 0 {
		state.ResetEth1DataVotes()
	}
}

func ProcessSlashingsReset(state *state.BeaconState) {
	state.SetSlashingSegmentAt(int(state.Epoch()+1)%int(state.BeaconConfig().EpochsPerSlashingsVector), 0)

}

func ProcessRandaoMixesReset(state *state.BeaconState) {
	currentEpoch := state.Epoch()
	nextEpoch := state.Epoch() + 1
	state.SetRandaoMixAt(int(nextEpoch%state.BeaconConfig().EpochsPerHistoricalVector), state.GetRandaoMixes(currentEpoch))
}

func ProcessParticipationFlagUpdates(state *state.BeaconState) {
	state.SetPreviousEpochParticipation(state.CurrentEpochParticipation())
	state.SetCurrentEpochParticipation(make([]cltypes.ParticipationFlags, len(state.Validators())))
}
