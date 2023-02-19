package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
)

// Implementation defined in ETH 2.0 specs: https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#get_flag_index_deltas.
// Although giulio made it efficient hopefully. results will be written in the input map.
func (s *StateTransistor) processFlagIndexDeltas(flagIdx int, eligibleValidators []uint64) (err error) {
	// Initialize variables
	var (
		unslashedParticipatingIndicies     []uint64
		unslashedParticipatingTotalBalance uint64
		baseReward                         uint64
	)
	// Find unslashedParticipatingIndicies for this specific flag index.
	previousEpoch := s.state.PreviousEpoch()
	unslashedParticipatingIndicies, err = s.state.GetUnslashedParticipatingIndices(flagIdx, previousEpoch)
	if err != nil {
		return
	}
	// Find current weight for flag index.
	weights := s.beaconConfig.ParticipationWeights()
	weight := weights[flagIdx]
	// Compute participating indices total balance (required in rewards/penalties computation).
	unslashedParticipatingTotalBalance, err = s.state.GetTotalBalance(unslashedParticipatingIndicies)
	if err != nil {
		return
	}

	// Compute relative increments.
	unslashedParticipatingIncrements := unslashedParticipatingTotalBalance / s.beaconConfig.EffectiveBalanceIncrement
	activeIncrements := s.state.GetTotalActiveBalance() / s.beaconConfig.EffectiveBalanceIncrement
	// denominators
	rewardDenominator := activeIncrements * s.beaconConfig.WeightDenominator
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err = s.state.BaseReward(index)
		if err != nil {
			return
		}
		if s.state.IsUnslashedParticipatingIndex(previousEpoch, index, flagIdx) {
			if s.state.InactivityLeaking() {
				continue
			}
			rewardNumerator := baseReward * weight * unslashedParticipatingIncrements
			if err := s.state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
				return err
			}
		} else if flagIdx != int(s.beaconConfig.TimelyHeadFlagIndex) {
			if err := s.state.DecreaseBalance(index, baseReward*weight/s.beaconConfig.WeightDenominator); err != nil {
				return err
			}
		}
	}
	return
}

// Implemention defined in https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/beacon-chain.md#modified-get_inactivity_penalty_deltas.
func (s *StateTransistor) processInactivityDeltas(eligibleValidators []uint64) (err error) {
	previousEpoch := s.state.PreviousEpoch()
	// retrieve penalty quotient based on fork
	var penaltyQuotient uint64
	switch s.state.Version() {
	case clparams.Phase0Version:
		penaltyQuotient = s.beaconConfig.InactivityPenaltyQuotient
	case clparams.AltairVersion:
		penaltyQuotient = s.beaconConfig.InactivityPenaltyQuotientAltair
	case clparams.BellatrixVersion:
		penaltyQuotient = s.beaconConfig.InactivityPenaltyQuotientBellatrix
	}
	penaltyDenominator := s.beaconConfig.InactivityScoreBias * penaltyQuotient
	validators := s.state.Validators()
	for _, index := range eligibleValidators {
		if s.state.IsUnslashedParticipatingIndex(previousEpoch, index, int(s.beaconConfig.TimelyTargetFlagIndex)) {
			continue
		}
		inactivityScore, err := s.state.ValidatorInactivityScore(int(index))
		if err != nil {
			return err
		}

		// Process inactivity penalties.
		penaltyNumerator := validators[index].EffectiveBalance * inactivityScore
		s.state.DecreaseBalance(index, penaltyNumerator/penaltyDenominator)
	}
	return
}

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func (s *StateTransistor) ProcessRewardsAndPenalties() (err error) {
	if s.state.Epoch() == s.beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := s.state.EligibleValidatorsIndicies()
	// process each flag indexes by weight.
	for i := range s.beaconConfig.ParticipationWeights() {
		if err = s.processFlagIndexDeltas(i, eligibleValidators); err != nil {
			return
		}
	}
	// process inactivity scores now.
	err = s.processInactivityDeltas(eligibleValidators)
	return
}
