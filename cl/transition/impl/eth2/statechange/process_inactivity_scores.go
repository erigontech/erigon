package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(s abstract.BeaconState) error {
	if state.Epoch(s) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	previousEpoch := state.PreviousEpoch(s)
	for _, validatorIndex := range state.EligibleValidatorsIndicies(s) {
		// retrieve validator inactivity score index.
		score, err := s.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if state.IsUnslashedParticipatingIndex(s, previousEpoch, validatorIndex, int(s.BeaconConfig().TimelyTargetFlagIndex)) {
			score -= utils.Min64(1, score)
		} else {
			score += s.BeaconConfig().InactivityScoreBias
		}
		if !state.InactivityLeaking(s) {
			score -= utils.Min64(s.BeaconConfig().InactivityScoreRecoveryRate, score)
		}
		if err := s.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
	}
	return nil
}
