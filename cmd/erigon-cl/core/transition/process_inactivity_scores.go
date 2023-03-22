package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(state *state.BeaconState) error {
	if state.Epoch() == state.BeaconConfig().GenesisEpoch {
		return nil
	}
	previousEpoch := state.PreviousEpoch()
	for _, validatorIndex := range state.EligibleValidatorsIndicies() {
		// retrieve validator inactivity score index.
		score, err := state.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if state.IsUnslashedParticipatingIndex(previousEpoch, validatorIndex, int(state.BeaconConfig().TimelyTargetFlagIndex)) {
			score -= utils.Min64(1, score)
		} else {
			score += state.BeaconConfig().InactivityScoreBias
		}
		if !state.InactivityLeaking() {
			score -= utils.Min64(state.BeaconConfig().InactivityScoreRecoveryRate, score)
		}
		if err := state.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
	}
	return nil
}
