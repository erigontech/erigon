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
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func GetUnslashedIndiciesSet(cfg *clparams.BeaconChainConfig, previousEpoch uint64, validatorSet *solid.ValidatorSet, previousEpochPartecipation *solid.BitList) [][]bool {
	weights := cfg.ParticipationWeights()
	flagsUnslashedIndiciesSet := make([][]bool, len(weights))
	for i := range weights {
		flagsUnslashedIndiciesSet[i] = make([]bool, validatorSet.Length())
	}

	validatorSet.Range(func(validatorIndex int, validator solid.Validator, total int) bool {
		for i := range weights {
			flagsUnslashedIndiciesSet[i][validatorIndex] = state.IsUnslashedParticipatingIndex(validatorSet, previousEpochPartecipation, previousEpoch, uint64(validatorIndex), i)
		}
		return true
	})
	return flagsUnslashedIndiciesSet
}

// ProcessEpoch process epoch transition.
func ProcessEpoch(s abstract.BeaconState) error {
	eligibleValidators := state.EligibleValidatorsIndicies(s)
	// start := time.Now()
	var unslashedIndiciesSet [][]bool
	if s.Version() >= clparams.AltairVersion {
		unslashedIndiciesSet = GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	}
	if err := ProcessJustificationBitsAndFinality(s, unslashedIndiciesSet); err != nil {
		return err
	}
	// fmt.Println("ProcessJustificationBitsAndFinality", time.Since(start))
	// start = time.Now()

	if s.Version() >= clparams.AltairVersion {
		if err := ProcessInactivityScores(s, eligibleValidators, unslashedIndiciesSet); err != nil {
			return err
		}
	}
	// fmt.Println("ProcessInactivityScores", time.Since(start))
	// start = time.Now()
	if err := ProcessRewardsAndPenalties(s, eligibleValidators, unslashedIndiciesSet); err != nil {
		return err
	}
	// fmt.Println("ProcessRewardsAndPenalties", time.Since(start))
	// start = time.Now()
	if err := ProcessRegistryUpdates(s); err != nil {
		return err
	}
	// fmt.Println("ProcessRegistryUpdates", time.Since(start))
	// start = time.Now()
	if err := ProcessSlashings(s); err != nil {
		return err
	}
	// fmt.Println("ProcessSlashings", time.Since(start))
	ProcessEth1DataReset(s)
	// start = time.Now()
	if err := ProcessEffectiveBalanceUpdates(s); err != nil {
		return err
	}
	// fmt.Println("ProcessEffectiveBalanceUpdates", time.Since(start))
	ProcessSlashingsReset(s)
	ProcessRandaoMixesReset(s)
	if err := ProcessHistoricalRootsUpdate(s); err != nil {
		return err
	}
	if s.Version() == clparams.Phase0Version {
		if err := ProcessParticipationRecordUpdates(s); err != nil {
			return err
		}
	}

	if s.Version() >= clparams.AltairVersion {
		ProcessParticipationFlagUpdates(s)
		if err := ProcessSyncCommitteeUpdate(s); err != nil {
			return err
		}
	}
	return nil
}
