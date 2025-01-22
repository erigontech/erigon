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
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/threading"
)

// ProcessInactivityScores will updates the inactivity registry of each validator.
func ProcessInactivityScores(s abstract.BeaconState, eligibleValidatorsIndicies []uint64, unslashedIndicies [][]bool) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessInactivityScoresTime).End()
	if state.Epoch(s) == s.BeaconConfig().GenesisEpoch {
		return nil
	}

	return threading.ParallellForLoop(1, 0, len(eligibleValidatorsIndicies), func(i int) error {
		validatorIndex := eligibleValidatorsIndicies[i]

		// retrieve validator inactivity score index.
		score, err := s.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return err
		}
		if score == 0 && unslashedIndicies[s.BeaconConfig().TimelyTargetFlagIndex][validatorIndex] {
			return nil
		}

		if unslashedIndicies[s.BeaconConfig().TimelyTargetFlagIndex][validatorIndex] {
			score -= min(1, score)
		} else {
			score += s.BeaconConfig().InactivityScoreBias
		}
		if !state.InactivityLeaking(s) {
			score -= min(s.BeaconConfig().InactivityScoreRecoveryRate, score)
		}

		if err := s.SetValidatorInactivityScore(int(validatorIndex), score); err != nil {
			return err
		}
		return nil
	})
}
