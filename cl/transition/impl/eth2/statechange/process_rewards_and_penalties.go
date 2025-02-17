// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package statechange

import (
	"fmt"
	"runtime"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/threading"
)

func getFlagsTotalBalances(s abstract.BeaconState, flagsUnslashedIndiciesSet [][]bool) []uint64 {
	beaconConfig := s.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()
	flagsTotalBalances := make([]uint64, len(weights))

	numWorkers := runtime.NumCPU()
	wp := threading.NewParallelExecutor()
	flagsTotalBalancesShards := make([][]uint64, len(weights))
	shardSize := s.ValidatorLength() / numWorkers

	for i := range weights {
		flagsTotalBalancesShards[i] = make([]uint64, numWorkers)
	}
	for i := 0; i < numWorkers; i++ {
		workerID := i
		from := workerID * shardSize
		to := (workerID + 1) * shardSize
		if workerID == numWorkers-1 || to > s.ValidatorLength() {
			to = s.ValidatorLength()
		}
		wp.AddWork(func() error {
			for validatorIndex := from; validatorIndex < to; validatorIndex++ {
				if validatorIndex >= s.ValidatorLength() {
					break
				}
				effectiveBalance, err := s.ValidatorEffectiveBalance(validatorIndex)
				if err != nil {
					panic(fmt.Sprintf("failed to get validator effective balance: %v", err))
				}
				for weight := range weights {
					if flagsUnslashedIndiciesSet[weight][validatorIndex] {
						flagsTotalBalancesShards[weight][workerID] += effectiveBalance
					}
				}
			}
			return nil
		})
		if to == s.ValidatorLength() {
			break
		}
	}

	wp.Execute()

	for i := range weights {
		for j := 0; j < numWorkers; j++ {
			flagsTotalBalances[i] += flagsTotalBalancesShards[i][j]
		}
	}
	return flagsTotalBalances
}

func processRewardsAndPenaltiesPostAltair(s abstract.BeaconState, eligibleValidators []uint64, flagsUnslashedIndiciesSet [][]bool) (err error) {
	beaconConfig := s.BeaconConfig()
	weights := beaconConfig.ParticipationWeights()

	// Initialize variables
	totalActiveBalance := s.GetTotalActiveBalance()
	// Inactivity penalties denominator.
	inactivityPenaltyDenominator := beaconConfig.InactivityScoreBias * beaconConfig.GetPenaltyQuotient(s.Version())
	// Make buffer for flag indexes total balances.
	flagsTotalBalances := getFlagsTotalBalances(s, flagsUnslashedIndiciesSet)
	// precomputed multiplier for reward.
	rewardMultipliers := make([]uint64, len(weights))
	for i := range weights {
		rewardMultipliers[i] = weights[i] * (flagsTotalBalances[i] / beaconConfig.EffectiveBalanceIncrement)
	}
	rewardDenominator := (totalActiveBalance / beaconConfig.EffectiveBalanceIncrement) * beaconConfig.WeightDenominator
	inactivityLeaking := state.InactivityLeaking(s)

	return threading.ParallellForLoop(1, 0, len(eligibleValidators), func(i int) error {
		index := eligibleValidators[i]
		baseReward, err := s.BaseReward(index)
		if err != nil {
			return err
		}
		delta := int64(0)
		for flagIdx := range weights {
			if flagsUnslashedIndiciesSet[flagIdx][index] {
				if !inactivityLeaking {
					delta += int64((baseReward * rewardMultipliers[flagIdx]) / rewardDenominator)
				}
			} else if flagIdx != int(beaconConfig.TimelyHeadFlagIndex) {
				delta -= int64(baseReward * weights[flagIdx] / beaconConfig.WeightDenominator)
			}
		}
		if !flagsUnslashedIndiciesSet[beaconConfig.TimelyTargetFlagIndex][index] {
			inactivityScore, err := s.ValidatorInactivityScore(int(index))
			if err != nil {
				return err
			}
			// Process inactivity penalties.
			effectiveBalance, err := s.ValidatorEffectiveBalance(int(index))
			if err != nil {
				return err
			}
			delta -= int64((effectiveBalance * inactivityScore) / inactivityPenaltyDenominator)
		}
		if delta > 0 {
			if err := state.IncreaseBalance(s, index, uint64(delta)); err != nil {
				return err
			}
		} else if err := state.DecreaseBalance(s, index, uint64(-delta)); err != nil {
			return err
		}
		return nil
	})
}

// processRewardsAndPenaltiesPhase0 process rewards and penalties for phase0 state.
func processRewardsAndPenaltiesPhase0(s abstract.BeaconState, eligibleValidators []uint64) (err error) {
	beaconConfig := s.BeaconConfig()
	if state.Epoch(s) == beaconConfig.GenesisEpoch {
		return nil
	}
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
		currentBalance, err := s.ValidatorBalance(int(index))
		if err != nil {
			return err
		}
		// If we attested then we reward the validator.
		if state.InactivityLeaking(s) {
			currentBalance += baseReward * attested
		} else {
			if !currentValidator.Slashed() && previousMatchingSourceAttester {
				currentBalance += (baseReward * unslashedMatchingSourceBalanceIncrements) / rewardDenominator
			}
			if !currentValidator.Slashed() && previousMatchingTargetAttester {
				currentBalance += (baseReward * unslashedMatchingTargetBalanceIncrements) / rewardDenominator
			}
			if !currentValidator.Slashed() && previousMatchingHeadAttester {
				currentBalance += (baseReward * unslashedMatchingHeadBalanceIncrements) / rewardDenominator
			}
		}
		// Process inactivity of the network as a whole finalities.
		if state.InactivityLeaking(s) {
			proposerReward := baseReward / beaconConfig.ProposerRewardQuotient
			currentBalance -= beaconConfig.BaseRewardsPerEpoch*baseReward - proposerReward
			if currentValidator.Slashed() || !previousMatchingTargetAttester {
				currentBalance -= currentValidator.EffectiveBalance() * state.FinalityDelay(s) / beaconConfig.InactivityPenaltyQuotient
			}
		}
		currentBalance -= baseReward * missed
		if err = s.SetValidatorBalance(int(index), currentBalance); err != nil {
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
		if err = state.IncreaseBalance(s, attestation.ProposerIndex, proposerReward); err != nil {
			return false
		}
		maxAttesterReward := baseReward - proposerReward
		if err = state.IncreaseBalance(s, uint64(index), maxAttesterReward/attestation.InclusionDelay); err != nil {
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
func ProcessRewardsAndPenalties(s abstract.BeaconState, eligibleValidators []uint64, unslashedIndicies [][]bool) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessRewardsAndPenaltiesTime).End()
	if state.Epoch(s) == s.BeaconConfig().GenesisEpoch {
		return nil
	}
	if s.Version() == clparams.Phase0Version {
		return processRewardsAndPenaltiesPhase0(s, eligibleValidators)
	}
	return processRewardsAndPenaltiesPostAltair(s, eligibleValidators, unslashedIndicies)
}
