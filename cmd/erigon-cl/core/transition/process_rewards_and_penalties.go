package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func processRewardsAndPenaltiesPostAltair(state *state.BeaconState) (err error) {
	beaconConfig := state.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()
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

// processRewardsAndPenaltiesPhase0 process rewards and penalties for phase0 state.
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
	var unslashedMatchingSourceBalanceIncrements, unslashedMatchingTargetBalanceIncrements, unslashedMatchingHeadBalanceIncrements uint64
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	for _, validator := range state.Validators() {
		if validator.Slashed {
			continue
		}
		if validator.IsPreviousMatchingSourceAttester {
			unslashedMatchingSourceBalanceIncrements += validator.EffectiveBalance
		}
		if validator.IsPreviousMatchingTargetAttester {
			unslashedMatchingTargetBalanceIncrements += validator.EffectiveBalance
		}
		if validator.IsPreviousMatchingHeadAttester {
			unslashedMatchingHeadBalanceIncrements += validator.EffectiveBalance
		}
	}
	// Then compute their total increment.
	unslashedMatchingSourceBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingTargetBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingHeadBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err := state.BaseReward(index)
		if err != nil {
			return err
		}
		// we can use a multiplier to account for all attesting
		attested, missed := validators[index].DutiesAttested()
		// If we attested then we reward the validator.
		if state.InactivityLeaking() {
			if err := state.IncreaseBalance(index, baseReward*attested); err != nil {
				return err
			}
		} else {
			if !validators[index].Slashed && validators[index].IsPreviousMatchingSourceAttester {
				rewardNumerator := baseReward * unslashedMatchingSourceBalanceIncrements
				if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !validators[index].Slashed && validators[index].IsPreviousMatchingTargetAttester {
				rewardNumerator := baseReward * unslashedMatchingTargetBalanceIncrements
				if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !validators[index].Slashed && validators[index].IsPreviousMatchingHeadAttester {
				rewardNumerator := baseReward * unslashedMatchingHeadBalanceIncrements
				if err := state.IncreaseBalance(index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
		}
		// Process inactivity of the network as a whole finalities.
		if state.InactivityLeaking() {
			proposerReward := baseReward / beaconConfig.ProposerRewardQuotient
			// Neutralize rewards.
			if state.DecreaseBalance(index, beaconConfig.BaseRewardsPerEpoch*baseReward-proposerReward); err != nil {
				return err
			}
			if validators[index].Slashed || !validators[index].IsPreviousMatchingTargetAttester {
				// Increase penalities linearly if network is leaking.
				if state.DecreaseBalance(index, validators[index].EffectiveBalance*state.FinalityDelay()/beaconConfig.InactivityPenaltyQuotient); err != nil {
					return err
				}
			}
		}

		// For each missed duty we penalize the validator.
		if state.DecreaseBalance(index, baseReward*missed); err != nil {
			return err
		}

	}
	// Lastly process late attestations
	for index, validator := range validators {
		if validator.Slashed || !validator.IsPreviousMatchingSourceAttester {
			continue
		}
		attestation := validators[index].MinPreviousInclusionDelayAttestation
		baseReward, err := state.BaseReward(uint64(index))
		if err != nil {
			return err
		}
		// Compute proposer reward.
		proposerReward := (baseReward / beaconConfig.ProposerRewardQuotient)
		if err := state.IncreaseBalance(attestation.ProposerIndex, proposerReward); err != nil {
			return err
		}
		maxAttesterReward := baseReward - proposerReward
		if err := state.IncreaseBalance(uint64(index), maxAttesterReward/attestation.InclusionDelay); err != nil {
			return err
		}
	}
	return
}

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func ProcessRewardsAndPenalties(state *state.BeaconState) error {
	if state.Epoch() == state.BeaconConfig().GenesisEpoch {
		return nil
	}
	if state.Version() == clparams.Phase0Version {
		return processRewardsAndPenaltiesPhase0(state)
	}
	return processRewardsAndPenaltiesPostAltair(state)
}
