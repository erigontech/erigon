package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func (s *StateTransistor) ProcessInactivityScores() error {
	if s.state.Epoch() == s.beaconConfig.GenesisEpoch {
		return nil
	}
	unslashedParticipatingIndicies, err := s.state.GetUnslashedParticipatingIndices(int(s.beaconConfig.TimelyHeadFlagIndex), s.state.PreviousEpoch())
	if err != nil {
		return err
	}
	isUnslashedParticipatingIndicies := make(map[uint64]bool)
	for _, validatorIndex := range unslashedParticipatingIndicies {
		isUnslashedParticipatingIndicies[validatorIndex] = true
	}
	for _, validatorIndex := range s.state.EligibleValidatorsIndicies() {
		// retrieve validator inactivity score index.
		score, err := s.state.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if isUnslashedParticipatingIndicies[validatorIndex] {
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
