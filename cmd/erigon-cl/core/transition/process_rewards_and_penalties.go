package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func processRewardsAndPenaltiesPostAltair(s *state.BeaconState) (err error) {
	beaconConfig := s.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()
	eligibleValidators := state.EligibleValidatorsIndicies(s.BeaconState)
	// Initialize variables
	totalActiveBalance := s.GetTotalActiveBalance()
	previousEpoch := state.PreviousEpoch(s.BeaconState)
	// Inactivity penalties denominator.
	inactivityPenaltyDenominator := beaconConfig.InactivityScoreBias * beaconConfig.GetPenaltyQuotient(s.Version())
	// Make buffer for flag indexes total balances.
	flagsTotalBalances := make([]uint64, len(weights))
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	s.ForEachValidator(func(validator *cltypes.Validator, validatorIndex, total int) bool {
		for i := range weights {
			if state.IsUnslashedParticipatingIndex(s.BeaconState, previousEpoch, uint64(validatorIndex), i) {
				flagsTotalBalances[i] += validator.EffectiveBalance()
			}
		}
		return true
	})
	// precomputed multiplier for reward.
	rewardMultipliers := make([]uint64, len(weights))
	for i := range weights {
		rewardMultipliers[i] = weights[i] * (flagsTotalBalances[i] / beaconConfig.EffectiveBalanceIncrement)
	}
	rewardDenominator := (totalActiveBalance / beaconConfig.EffectiveBalanceIncrement) * beaconConfig.WeightDenominator
	var baseReward uint64
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err = s.BaseReward(index)
		if err != nil {
			return
		}
		for flagIdx := range weights {
			if state.IsUnslashedParticipatingIndex(s.BeaconState, previousEpoch, index, flagIdx) {
				if !state.InactivityLeaking(s.BeaconState) {
					rewardNumerator := baseReward * rewardMultipliers[flagIdx]
					if err := state.IncreaseBalance(s.BeaconState, index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
			} else if flagIdx != int(beaconConfig.TimelyHeadFlagIndex) {
				if err := state.DecreaseBalance(s.BeaconState, index, baseReward*weights[flagIdx]/beaconConfig.WeightDenominator); err != nil {
					return err
				}
			}
		}
		if !state.IsUnslashedParticipatingIndex(s.BeaconState, previousEpoch, index, int(beaconConfig.TimelyTargetFlagIndex)) {
			inactivityScore, err := s.ValidatorInactivityScore(int(index))
			if err != nil {
				return err
			}
			// Process inactivity penalties.
			effectiveBalance, err := s.ValidatorEffectiveBalance(int(index))
			if err != nil {
				return err
			}
			state.DecreaseBalance(s.BeaconState, index, (effectiveBalance*inactivityScore)/inactivityPenaltyDenominator)
		}
	}
	return
}

// processRewardsAndPenaltiesPhase0 process rewards and penalties for phase0 state.
func processRewardsAndPenaltiesPhase0(s *state.BeaconState) (err error) {
	beaconConfig := s.BeaconConfig()
	if state.Epoch(s.BeaconState) == beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := state.EligibleValidatorsIndicies(s.BeaconState)
	// Initialize variables
	rewardDenominator := s.GetTotalActiveBalance() / beaconConfig.EffectiveBalanceIncrement
	// Make buffer for flag indexes totTargetal balances.
	var unslashedMatchingSourceBalanceIncrements, unslashedMatchingTargetBalanceIncrements, unslashedMatchingHeadBalanceIncrements uint64
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	s.ForEachValidator(func(validator *cltypes.Validator, idx, total int) bool {
		if validator.Slashed() {
			return true
		}
		if validator.IsPreviousMatchingSourceAttester {
			unslashedMatchingSourceBalanceIncrements += validator.EffectiveBalance()
		}
		if validator.IsPreviousMatchingTargetAttester {
			unslashedMatchingTargetBalanceIncrements += validator.EffectiveBalance()
		}
		if validator.IsPreviousMatchingHeadAttester {
			unslashedMatchingHeadBalanceIncrements += validator.EffectiveBalance()
		}
		return true
	})
	// Then compute their total increment.
	unslashedMatchingSourceBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingTargetBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingHeadBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	// Now process deltas and whats nots.
	for _, index := range eligibleValidators {
		baseReward, err := s.BaseReward(index)
		if err != nil {
			return err
		}
		currentValidator, err := s.ValidatorForValidatorIndex(int(index))
		if err != nil {
			return err
		}
		// we can use a multiplier to account for all attesting
		attested, missed := currentValidator.DutiesAttested()
		// If we attested then we reward the validator.
		if state.InactivityLeaking(s.BeaconState) {
			if err := state.IncreaseBalance(s.BeaconState, index, baseReward*attested); err != nil {
				return err
			}
		} else {
			if !currentValidator.Slashed() && currentValidator.IsPreviousMatchingSourceAttester {
				rewardNumerator := baseReward * unslashedMatchingSourceBalanceIncrements
				if err := state.IncreaseBalance(s.BeaconState, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !currentValidator.Slashed() && currentValidator.IsPreviousMatchingTargetAttester {
				rewardNumerator := baseReward * unslashedMatchingTargetBalanceIncrements
				if err := state.IncreaseBalance(s.BeaconState, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !currentValidator.Slashed() && currentValidator.IsPreviousMatchingHeadAttester {
				rewardNumerator := baseReward * unslashedMatchingHeadBalanceIncrements
				if err := state.IncreaseBalance(s.BeaconState, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
		}
		// Process inactivity of the network as a whole finalities.
		if state.InactivityLeaking(s.BeaconState) {
			proposerReward := baseReward / beaconConfig.ProposerRewardQuotient
			// Neutralize rewards.
			if state.DecreaseBalance(s.BeaconState, index, beaconConfig.BaseRewardsPerEpoch*baseReward-proposerReward); err != nil {
				return err
			}
			if currentValidator.Slashed() || !currentValidator.IsPreviousMatchingTargetAttester {
				// Increase penalities linearly if network is leaking.
				if state.DecreaseBalance(s.BeaconState, index, currentValidator.EffectiveBalance()*state.FinalityDelay(s.BeaconState)/beaconConfig.InactivityPenaltyQuotient); err != nil {
					return err
				}
			}
		}

		// For each missed duty we penalize the validator.
		if state.DecreaseBalance(s.BeaconState, index, baseReward*missed); err != nil {
			return err
		}

	}
	// Lastly process late attestations

	s.ForEachValidator(func(validator *cltypes.Validator, index, total int) bool {
		if validator.Slashed() || !validator.IsPreviousMatchingSourceAttester {
			return true
		}
		attestation := validator.MinPreviousInclusionDelayAttestation
		var baseReward uint64
		baseReward, err = s.BaseReward(uint64(index))
		if err != nil {
			return false
		}
		// Compute proposer reward.
		proposerReward := (baseReward / beaconConfig.ProposerRewardQuotient)
		if err = state.IncreaseBalance(s.BeaconState, attestation.ProposerIndex, proposerReward); err != nil {
			return false
		}
		maxAttesterReward := baseReward - proposerReward
		if err = state.IncreaseBalance(s.BeaconState, uint64(index), maxAttesterReward/attestation.InclusionDelay); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return
}

// ProcessRewardsAndPenalties applies rewards/penalties accumulated during previous epoch.
func ProcessRewardsAndPenalties(s *state.BeaconState) error {
	if state.Epoch(s.BeaconState) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	if s.Version() == clparams.Phase0Version {
		return processRewardsAndPenaltiesPhase0(s)
	}
	return processRewardsAndPenaltiesPostAltair(s)
}
