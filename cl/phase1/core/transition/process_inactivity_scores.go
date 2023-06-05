package transition

import (
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(s *state2.BeaconState) error {
	if state2.Epoch(s.BeaconState) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	previousEpoch := state2.PreviousEpoch(s.BeaconState)
	for _, validatorIndex := range state2.EligibleValidatorsIndicies(s.BeaconState) {
		// retrieve validator inactivity score index.
		score, err := s.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if state2.IsUnslashedParticipatingIndex(s.BeaconState, previousEpoch, validatorIndex, int(s.BeaconConfig().TimelyTargetFlagIndex)) {
			score -= utils.Min64(1, score)
		} else {
			score += s.BeaconConfig().InactivityScoreBias
		}
		if !state2.InactivityLeaking(s.BeaconState) {
			score -= utils.Min64(s.BeaconConfig().InactivityScoreRecoveryRate, score)
		}
		if err := s.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
	}
	return nil
}
