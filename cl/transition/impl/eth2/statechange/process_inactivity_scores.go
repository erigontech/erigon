package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(s abstract.BeaconState, eligibleValidatorsIndicies []uint64, unslashedIndicies [][]bool) error {
	if state.Epoch(s) == s.BeaconConfig().GenesisEpoch {
		return nil
	}

	for _, validatorIndex := range eligibleValidatorsIndicies {
		// retrieve validator inactivity score index.
		score, err := s.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if unslashedIndicies[s.BeaconConfig().TimelyTargetFlagIndex][validatorIndex] {
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
