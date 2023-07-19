package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func processRewardsAndPenaltiesPostAltair(s abstract.BeaconState) (err error) {
	beaconConfig := s.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()
	eligibleValidators := state.EligibleValidatorsIndicies(s)
	// Initialize variables
	totalActiveBalance := s.GetTotalActiveBalance()
	previousEpoch := state.PreviousEpoch(s)
	// Inactivity penalties denominator.
	inactivityPenaltyDenominator := beaconConfig.InactivityScoreBias * beaconConfig.GetPenaltyQuotient(s.Version())
	// Make buffer for flag indexes total balances.
	flagsTotalBalances := make([]uint64, len(weights))
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	s.ForEachValidator(func(validator solid.Validator, validatorIndex, total int) bool {
		for i := range weights {
			if state.IsUnslashedParticipatingIndex(s, previousEpoch, uint64(validatorIndex), i) {
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
			if state.IsUnslashedParticipatingIndex(s, previousEpoch, index, flagIdx) {
				if !state.InactivityLeaking(s) {
					rewardNumerator := baseReward * rewardMultipliers[flagIdx]
					if err := state.IncreaseBalance(s, index, rewardNumerator/rewardDenominator); err != nil {
						return err
					}
				}
			} else if flagIdx != int(beaconConfig.TimelyHeadFlagIndex) {
				if err := state.DecreaseBalance(s, index, baseReward*weights[flagIdx]/beaconConfig.WeightDenominator); err != nil {
					return err
				}
			}
		}
		if !state.IsUnslashedParticipatingIndex(s, previousEpoch, index, int(beaconConfig.TimelyTargetFlagIndex)) {
			inactivityScore, err := s.ValidatorInactivityScore(int(index))
			if err != nil {
				return err
			}
			// Process inactivity penalties.
			effectiveBalance, err := s.ValidatorEffectiveBalance(int(index))
			if err != nil {
				return err
			}
			state.DecreaseBalance(s, index, (effectiveBalance*inactivityScore)/inactivityPenaltyDenominator)
		}
	}
	return
}

// processRewardsAndPenaltiesPhase0 process rewards and penalties for phase0 state.
func processRewardsAndPenaltiesPhase0(s abstract.BeaconState) (err error) {
	beaconConfig := s.BeaconConfig()
	if state.Epoch(s) == beaconConfig.GenesisEpoch {
		return nil
	}
	eligibleValidators := state.EligibleValidatorsIndicies(s)
	// Initialize variables
	rewardDenominator := s.GetTotalActiveBalance() / beaconConfig.EffectiveBalanceIncrement
	// Make buffer for flag indexes totTargetal balances.
	var unslashedMatchingSourceBalanceIncrements, unslashedMatchingTargetBalanceIncrements, unslashedMatchingHeadBalanceIncrements uint64
	// Compute all total balances for each enable unslashed validator indicies with all flags on.
	s.ForEachValidator(func(validator solid.Validator, idx, total int) bool {
		if validator.Slashed() {
			return true
		}
		var previousMatchingSourceAttester, previousMatchingTargetAttester, previousMatchingHeadAttester bool

		if previousMatchingSourceAttester, err = s.ValidatorIsPreviousMatchingSourceAttester(idx); err != nil {
			return false
		}
		if previousMatchingTargetAttester, err = s.ValidatorIsPreviousMatchingTargetAttester(idx); err != nil {
			return false
		}
		if previousMatchingHeadAttester, err = s.ValidatorIsPreviousMatchingHeadAttester(idx); err != nil {
			return false
		}
		if previousMatchingSourceAttester {
			unslashedMatchingSourceBalanceIncrements += validator.EffectiveBalance()
		}
		if previousMatchingTargetAttester {
			unslashedMatchingTargetBalanceIncrements += validator.EffectiveBalance()
		}
		if previousMatchingHeadAttester {
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
		var previousMatchingSourceAttester, previousMatchingTargetAttester, previousMatchingHeadAttester bool

		if previousMatchingSourceAttester, err = s.ValidatorIsPreviousMatchingSourceAttester(int(index)); err != nil {
			return err
		}
		if previousMatchingTargetAttester, err = s.ValidatorIsPreviousMatchingTargetAttester(int(index)); err != nil {
			return err
		}
		if previousMatchingHeadAttester, err = s.ValidatorIsPreviousMatchingHeadAttester(int(index)); err != nil {
			return err
		}

		// we can use a multiplier to account for all attesting
		var attested, missed uint64
		if currentValidator.Slashed() {
			attested, missed = 0, 3
		} else {
			if previousMatchingSourceAttester {
				attested++
			}
			if previousMatchingTargetAttester {
				attested++
			}
			if previousMatchingHeadAttester {
				attested++
			}
			missed = 3 - attested
		}

		// If we attested then we reward the validator.
		if state.InactivityLeaking(s) {
			if err := state.IncreaseBalance(s, index, baseReward*attested); err != nil {
				return err
			}
		} else {
			if !currentValidator.Slashed() && previousMatchingSourceAttester {
				rewardNumerator := baseReward * unslashedMatchingSourceBalanceIncrements
				if err := state.IncreaseBalance(s, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !currentValidator.Slashed() && previousMatchingTargetAttester {
				rewardNumerator := baseReward * unslashedMatchingTargetBalanceIncrements
				if err := state.IncreaseBalance(s, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
			if !currentValidator.Slashed() && previousMatchingHeadAttester {
				rewardNumerator := baseReward * unslashedMatchingHeadBalanceIncrements
				if err := state.IncreaseBalance(s, index, rewardNumerator/rewardDenominator); err != nil {
					return err
				}
			}
		}
		// Process inactivity of the network as a whole finalities.
		if state.InactivityLeaking(s) {
			proposerReward := baseReward / beaconConfig.ProposerRewardQuotient
			// Neutralize rewards.
			if state.DecreaseBalance(s, index, beaconConfig.BaseRewardsPerEpoch*baseReward-proposerReward); err != nil {
				return err
			}
			if currentValidator.Slashed() || !previousMatchingTargetAttester {
				// Increase penalities linearly if network is leaking.
				if state.DecreaseBalance(s, index, currentValidator.EffectiveBalance()*state.FinalityDelay(s)/beaconConfig.InactivityPenaltyQuotient); err != nil {
					return err
				}
			}
		}

		// For each missed duty we penalize the validator.
		if state.DecreaseBalance(s, index, baseReward*missed); err != nil {
			return err
		}

	}
	// Lastly process late attestations

	s.ForEachValidator(func(validator solid.Validator, index, total int) bool {
		var previousMatchingSourceAttester bool
		var attestation *solid.PendingAttestation
		if previousMatchingSourceAttester, err = s.ValidatorIsPreviousMatchingSourceAttester(index); err != nil {
			return false
		}
		if validator.Slashed() || !previousMatchingSourceAttester {
			return true
		}
		if attestation, err = s.ValidatorMinPreviousInclusionDelayAttestation(index); err != nil {
			return false
		}
		var baseReward uint64
		baseReward, err = s.BaseReward(uint64(index))
		if err != nil {
			return false
		}
		// Compute proposer reward.
		proposerReward := (baseReward / beaconConfig.ProposerRewardQuotient)
		if err = state.IncreaseBalance(s, attestation.ProposerIndex(), proposerReward); err != nil {
			return false
		}
		maxAttesterReward := baseReward - proposerReward
		if err = state.IncreaseBalance(s, uint64(index), maxAttesterReward/attestation.InclusionDelay()); err != nil {
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
func ProcessRewardsAndPenalties(s abstract.BeaconState) error {
	if state.Epoch(s) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	if s.Version() == clparams.Phase0Version {
		return processRewardsAndPenaltiesPhase0(s)
	}
	return processRewardsAndPenaltiesPostAltair(s)
}
