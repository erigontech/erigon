package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(s *state.BeaconState) error {
	if state.Epoch(s.BeaconState) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	previousEpoch := state.PreviousEpoch(s.BeaconState)
	for _, validatorIndex := range state.EligibleValidatorsIndicies(s.BeaconState) {
		// retrieve validator inactivity score index.
		score, err := s.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if state.IsUnslashedParticipatingIndex(s.BeaconState, previousEpoch, validatorIndex, int(s.BeaconConfig().TimelyTargetFlagIndex)) {
			score -= utils.Min64(1, score)
		} else {
			score += s.BeaconConfig().InactivityScoreBias
		}
		if !state.InactivityLeaking(s.BeaconState) {
			score -= utils.Min64(s.BeaconConfig().InactivityScoreRecoveryRate, score)
		}
		if err := s.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
	}
	return nil
}
