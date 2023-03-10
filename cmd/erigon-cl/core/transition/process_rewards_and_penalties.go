package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func processRewardsAndPenaltiesPostAltair(state *state.BeaconState) (err error) {
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

func processRewardsAndPenaltiesPhase0(state *state.BeaconState) (err error) {
	beaconConfig := state.BeaconConfig()
	if state.Epoch() == beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := state.EligibleValidatorsIndicies()
	// Initialize variables
	rewardDenominator := state.GetTotalActiveBalance() / beaconConfig.EffectiveBalanceIncrement
	validators := state.Validators()
	// Make buffer for flag indexes totTargetal balances.
	var unslashedMatchingSourceBalance, unslashedMatchingTargetBalance, unslashedMatchingHeadBalance uint64
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	for _, validator := range state.Validators() {
		if validator.Slashed {
			continue
		}
		if validator.IsPreviousMatchingSourceAttester {
			unslashedMatchingSourceBalance += validator.EffectiveBalance
		}
		if validator.IsPreviousMatchingTargetAttester {
			unslashedMatchingTargetBalance += validator.EffectiveBalance
		}
		if validator.IsPreviousMatchingHeadAttester {
			unslashedMatchingHeadBalance += validator.EffectiveBalance
		}
	}
	// Then compute their total increment.
	unslashedMatchingSourceBalance /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingTargetBalance /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingHeadBalance /= beaconConfig.EffectiveBalanceIncrement
	// precompute partially the base reward.
	baseRewardIncrement := state.BaseRewardPerIncrement()
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward := (validators[index].EffectiveBalance / beaconConfig.EffectiveBalanceIncrement) * baseRewardIncrement
		// we can use a multiplier to account for all attesting
		attested, missed := validators[index].DutiesAttested()
		// If we attested then we reward the validator.
		if attested > 0 {
			if state.InactivityLeaking() {
				if err := state.IncreaseBalance(index, baseReward*attested); err != nil {
					return err
				}
			} else {
				if !validators[index].Slashed && validators[index].IsPreviousMatchingSourceAttester {
					rewardNumerator := baseReward * unslashedMatchingSourceBalance
					if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
				if !validators[index].Slashed && validators[index].IsPreviousMatchingTargetAttester {
					rewardNumerator := baseReward * unslashedMatchingTargetBalance
					if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
				if !validators[index].Slashed && validators[index].IsPreviousMatchingHeadAttester {
					rewardNumerator := baseReward * unslashedMatchingHeadBalance
					if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
			}
			// Process inactivity of the network as a whole finalities.
			if state.InactivityLeaking() {
				// Neutralize rewards.
				if state.DecreaseBalance(index, beaconConfig.BaseRewardsPerEpoch*baseReward*(baseReward/beaconConfig.ProposerRewardQuotient)); err != nil {
					return err
				}
				if validators[index].Slashed || validators[index].IsPreviousMatchingTargetAttester {
					// Increase penalities linearly if network is leaking.
					if state.DecreaseBalance(index, validators[index].EffectiveBalance*state.FinalityDelay()/beaconConfig.InactivityPenaltyQuotient); err != nil {
						return err
					}
				}
			}
		}

		// For each missed duty we penalize the validator.
		if state.DecreaseBalance(index, baseReward*missed); err != nil {
			return err
		}

	}
	return
}

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func ProcessRewardsAndPenalties(state *state.BeaconState) error {
	if state.Version() == clparams.Phase0Version {
		return nil
	}
	return processRewardsAndPenaltiesPostAltair(state)
}
