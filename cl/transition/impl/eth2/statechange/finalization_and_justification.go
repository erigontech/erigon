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
	"runtime"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/threading"
)

// weighJustificationAndFinalization checks justification and finality of epochs and adds records to the state as needed.
func weighJustificationAndFinalization(s abstract.BeaconState, previousEpochTargetBalance, currentEpochTargetBalance uint64) error {
	totalActiveBalance := s.GetTotalActiveBalance()
	currentEpoch := state.Epoch(s)
	previousEpoch := state.PreviousEpoch(s)
	oldPreviousJustifiedCheckpoint := s.PreviousJustifiedCheckpoint()
	oldCurrentJustifiedCheckpoint := s.CurrentJustifiedCheckpoint()
	justificationBits := s.JustificationBits()
	// Process justification
	s.SetPreviousJustifiedCheckpoint(oldCurrentJustifiedCheckpoint)
	// Discard oldest bit
	copy(justificationBits[1:], justificationBits[:3])
	// Turn off current justification bit
	justificationBits[0] = false
	// Update justified checkpoint if super majority is reached on previous epoch
	if previousEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state.GetBlockRoot(s, previousEpoch)
		if err != nil {
			return err
		}

		s.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: previousEpoch, Root: checkPointRoot})
		justificationBits[1] = true
	}
	if currentEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state.GetBlockRoot(s, currentEpoch)
		if err != nil {
			return err
		}

		s.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: currentEpoch, Root: checkPointRoot})
		justificationBits[0] = true
	}
	// Process finalization
	// The 2nd/3rd/4th most recent epochs are justified, the 2nd using the 4th as source
	// The 2nd/3rd most recent epochs are justified, the 2nd using the 3rd as source
	if (justificationBits.CheckRange(1, 4) && oldPreviousJustifiedCheckpoint.Epoch+3 == currentEpoch) ||
		(justificationBits.CheckRange(1, 3) && oldPreviousJustifiedCheckpoint.Epoch+2 == currentEpoch) {
		s.SetFinalizedCheckpoint(oldPreviousJustifiedCheckpoint)
	}
	// The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
	// The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
	if (justificationBits.CheckRange(0, 3) && oldCurrentJustifiedCheckpoint.Epoch+2 == currentEpoch) ||
		(justificationBits.CheckRange(0, 2) && oldCurrentJustifiedCheckpoint.Epoch+1 == currentEpoch) {
		s.SetFinalizedCheckpoint(oldCurrentJustifiedCheckpoint)
	}
	// Write justification bits
	s.SetJustificationBits(justificationBits)
	return nil
}

func ProcessJustificationBitsAndFinality(s abstract.BeaconState, unslashedParticipatingIndicies [][]bool) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessJustificationBitsAndFinalityTime).End()
	currentEpoch := state.Epoch(s)
	beaconConfig := s.BeaconConfig()
	// Skip for first 2 epochs
	if currentEpoch <= beaconConfig.GenesisEpoch+1 {
		return nil
	}
	var previousTargetBalance, currentTargetBalance uint64
	if s.Version() == clparams.Phase0Version {
		var err error
		s.ForEachValidator(func(validator solid.Validator, idx, total int) bool {
			if validator.Slashed() {
				return true
			}
			isCurrentMatchingTargetAttester, err2 := s.ValidatorIsCurrentMatchingTargetAttester(idx)
			if err2 != nil {
				err = err2
				return false
			}
			isPreviousMatchingTargetAttester, err2 := s.ValidatorIsPreviousMatchingTargetAttester(idx)
			if err2 != nil {
				err = err2
				return false
			}

			if isCurrentMatchingTargetAttester {
				currentTargetBalance += validator.EffectiveBalance()
			}
			if isPreviousMatchingTargetAttester {
				previousTargetBalance += validator.EffectiveBalance()
			}
			return true
		})
		if err != nil {
			return err
		}
	} else {
		var err error
		previousTargetBalance, currentTargetBalance, err = computePreviousAndCurrentTargetBalancePostAltair(s, unslashedParticipatingIndicies)
		if err != nil {
			return err
		}
	}

	return weighJustificationAndFinalization(s, previousTargetBalance, currentTargetBalance)
}

func computePreviousAndCurrentTargetBalancePostAltair(s abstract.BeaconState, unslashedParticipatingIndicies [][]bool) (previousTargetBalance, currentTargetBalance uint64, err error) {
	currentEpoch := state.Epoch(s)
	beaconConfig := s.BeaconConfig()
	previousEpoch := state.PreviousEpoch(s)

	// Use bitlists to determine finality.
	currentParticipation, previousParticipation := s.EpochParticipation(true), s.EpochParticipation(false)
	numWorkers := runtime.NumCPU()
	currentTargetBalanceShards := make([]uint64, numWorkers)
	previousTargetBalanceShards := make([]uint64, numWorkers)
	shardSize := s.ValidatorSet().Length() / numWorkers
	if shardSize == 0 {
		shardSize = s.ValidatorSet().Length()
	}

	wp := threading.NewParallelExecutor()
	for i := 0; i < numWorkers; i++ {
		workerID := i
		from := workerID * shardSize
		to := from + shardSize
		if workerID == numWorkers-1 || to > s.ValidatorSet().Length() {
			to = s.ValidatorSet().Length()
		}
		wp.AddWork(func() error {
			for validatorIndex := from; validatorIndex < to; validatorIndex++ {

				validator := s.ValidatorSet().Get(validatorIndex)
				if validator.Slashed() {
					continue
				}
				effectiveBalance := validator.EffectiveBalance()
				if effectiveBalance == 0 {
					continue
				}
				if unslashedParticipatingIndicies != nil {
					if unslashedParticipatingIndicies[beaconConfig.TimelyTargetFlagIndex][validatorIndex] {
						previousTargetBalanceShards[workerID] += effectiveBalance
					}
				} else if validator.Active(previousEpoch) &&
					cltypes.ParticipationFlags(previousParticipation.Get(validatorIndex)).HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
					previousTargetBalanceShards[workerID] += effectiveBalance
				}

				if validator.Active(currentEpoch) &&
					cltypes.ParticipationFlags(currentParticipation.Get(validatorIndex)).HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
					currentTargetBalanceShards[workerID] += effectiveBalance
				}
			}
			return nil
		})
		if to == s.ValidatorSet().Length() {
			break
		}
	}

	wp.Execute()

	for i := 0; i < numWorkers; i++ {
		previousTargetBalance += previousTargetBalanceShards[i]
		currentTargetBalance += currentTargetBalanceShards[i]
	}

	return
}
