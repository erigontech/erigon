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
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/threading"
)

func GetUnslashedIndiciesSet(cfg *clparams.BeaconChainConfig, previousEpoch uint64, validatorSet *solid.ValidatorSet, previousEpochParticipation *solid.ParticipationBitList) [][]bool {
	weights := cfg.ParticipationWeights()
	flagsUnslashedIndiciesSet := make([][]bool, len(weights))
	for i := range weights {
		flagsUnslashedIndiciesSet[i] = make([]bool, validatorSet.Length())
	}

	threading.ParallellForLoop(1, 0, validatorSet.Length(), func(validatorIndex int) error {
		for i := range weights {
			flagsUnslashedIndiciesSet[i][validatorIndex] = state.IsUnslashedParticipatingIndex(validatorSet, previousEpochParticipation, previousEpoch, uint64(validatorIndex), i)
		}
		return nil
	})

	return flagsUnslashedIndiciesSet
}

// ProcessEpoch process epoch transition.
func ProcessEpoch(s abstract.BeaconState) error {
	defer monitor.ObserveElaspedTime(monitor.EpochProcessingTime).End()
	eligibleValidators := state.EligibleValidatorsIndicies(s)
	var unslashedIndiciesSet [][]bool
	if s.Version() >= clparams.AltairVersion {
		unslashedIndiciesSet = GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	}
	if err := ProcessJustificationBitsAndFinality(s, unslashedIndiciesSet); err != nil {
		return err
	}

	if s.Version() >= clparams.AltairVersion {
		if err := ProcessInactivityScores(s, eligibleValidators, unslashedIndiciesSet); err != nil {
			return err
		}
	}

	if err := ProcessRewardsAndPenalties(s, eligibleValidators, unslashedIndiciesSet); err != nil {
		return err
	}

	if err := ProcessRegistryUpdates(s); err != nil {
		return err
	}

	if err := ProcessSlashings(s); err != nil {
		return err
	}

	ProcessEth1DataReset(s)
	if s.Version() >= clparams.ElectraVersion {
		ProcessPendingDeposits(s)
		ProcessPendingConsolidations(s)
	}

	if err := ProcessEffectiveBalanceUpdates(s); err != nil {
		return err
	}

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

	if s.Version() >= clparams.FuluVersion {
		if err := ProcessProposerLookahead(s); err != nil {
			return err
		}
	}

	return nil
}
