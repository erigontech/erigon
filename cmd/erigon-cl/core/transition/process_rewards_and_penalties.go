package transition

import "github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func ProcessRewardsAndPenalties(state *state.BeaconState) (err error) {
	beaconConfig := state.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()
	if state.Epoch() == beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := state.EligibleValidatorsIndicies()
	// Initialize variables
	totalActiveBalance := state.GetTotalActiveBalance()
	previousEpoch := state.PreviousEpoch()
	validators := state.Validators()
	// Inactivity penalties denominator.
	inactivityPenaltyDenominator := beaconConfig.InactivityScoreBias * beaconConfig.GetPenaltyQuotient(state.Version())
	// Make buffer for flag indexes total balances.
	flagsTotalBalances := make([]uint64, len(weights))
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	for validatorIndex, validator := range state.Validators() {
		for i := range weights {
			if state.IsUnslashedParticipatingIndex(previousEpoch, uint64(validatorIndex), i) {
				flagsTotalBalances[i] += validator.EffectiveBalance
			}
		}
	}
	// precomputed multiplier for reward.
	rewardMultipliers := make([]uint64, len(weights))
	for i := range weights {
		rewardMultipliers[i] = weights[i] * (flagsTotalBalances[i] / beaconConfig.EffectiveBalanceIncrement)
	}
	rewardDenominator := (totalActiveBalance / beaconConfig.EffectiveBalanceIncrement) * beaconConfig.WeightDenominator
	var baseReward uint64
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err = state.BaseReward(index)
		if err != nil {
			return
		}
		for flagIdx := range weights {
			if state.IsUnslashedParticipatingIndex(previousEpoch, index, flagIdx) {
				if !state.InactivityLeaking() {
					rewardNumerator := baseReward * rewardMultipliers[flagIdx]
					if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
			} else if flagIdx != int(beaconConfig.TimelyHeadFlagIndex) {
				if err := state.DecreaseBalance(index, baseReward*weights[flagIdx]/beaconConfig.WeightDenominator); err != nil {
					return err
				}
			}
		}
		if !state.IsUnslashedParticipatingIndex(previousEpoch, index, int(beaconConfig.TimelyTargetFlagIndex)) {
			inactivityScore, err := state.ValidatorInactivityScore(int(index))
			if err != nil {
				return err
			}
			// Process inactivity penalties.
			state.DecreaseBalance(index, (validators[index].EffectiveBalance*inactivityScore)/inactivityPenaltyDenominator)
		}
	}
	return
}
