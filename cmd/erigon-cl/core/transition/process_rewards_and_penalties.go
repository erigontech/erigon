package transition

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func (s *StateTransistor) ProcessRewardsAndPenalties() (err error) {
	weights := s.beaconConfig.ParticipationWeights()
	if s.state.Epoch() == s.beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := s.state.EligibleValidatorsIndicies()
	// Initialize variables
	totalActiveBalance := s.state.GetTotalActiveBalance()
	previousEpoch := s.state.PreviousEpoch()
	validators := s.state.Validators()
	// Inactivity penalties denominator.
	inactivityPenaltyDenominator := s.beaconConfig.InactivityScoreBias * s.beaconConfig.GetPenaltyQuotient(s.state.Version())
	// Make buffer for flag indexes total balances.
	flagsTotalBalances := make([]uint64, len(weights))
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	for validatorIndex, validator := range s.state.Validators() {
		for i := range weights {
			if s.state.IsUnslashedParticipatingIndex(previousEpoch, uint64(validatorIndex), i) {
				flagsTotalBalances[i] += validator.EffectiveBalance
			}
		}
	}
	// precomputed multiplier for reward.
	rewardMultipliers := make([]uint64, len(weights))
	for i := range weights {
		rewardMultipliers[i] = weights[i] * (flagsTotalBalances[i] / s.beaconConfig.EffectiveBalanceIncrement)
	}
	rewardDenominator := (totalActiveBalance / s.beaconConfig.EffectiveBalanceIncrement) * s.beaconConfig.WeightDenominator
	var baseReward uint64
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err = s.state.BaseReward(index)
		if err != nil {
			return
		}
		for flagIdx := range weights {
			if s.state.IsUnslashedParticipatingIndex(previousEpoch, index, flagIdx) {
				if !s.state.InactivityLeaking() {
					rewardNumerator := baseReward * rewardMultipliers[flagIdx]
					if err := s.state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
			} else if flagIdx != int(s.beaconConfig.TimelyHeadFlagIndex) {
				if err := s.state.DecreaseBalance(index, baseReward*weights[flagIdx]/s.beaconConfig.WeightDenominator); err != nil {
					return err
				}
			}
		}
		if !s.state.IsUnslashedParticipatingIndex(previousEpoch, index, int(s.beaconConfig.TimelyTargetFlagIndex)) {
			inactivityScore, err := s.state.ValidatorInactivityScore(int(index))
			if err != nil {
				return err
			}
			// Process inactivity penalties.
			s.state.DecreaseBalance(index, (validators[index].EffectiveBalance*inactivityScore)/inactivityPenaltyDenominator)
		}
	}
	return
}
