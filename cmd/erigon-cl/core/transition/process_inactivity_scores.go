package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func (s *StateTransistor) ProcessInactivityScores() error {
	if s.state.Epoch() == s.beaconConfig.GenesisEpoch {
		return nil
	}
	previousEpoch := s.state.PreviousEpoch()
	for _, validatorIndex := range s.state.EligibleValidatorsIndicies() {
		// retrieve validator inactivity score index.
		score, err := s.state.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if s.state.IsUnslashedParticipatingIndex(previousEpoch, validatorIndex, int(s.beaconConfig.TimelyTargetFlagIndex)) {
			score -= utils.Min64(1, score)
		} else {
			score += s.beaconConfig.InactivityScoreBias
		}
		if !s.state.InactivityLeaking() {
			score -= utils.Min64(s.beaconConfig.InactivityScoreRecoveryRate, score)
		}
		if err := s.state.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
	}
	return nil
}
