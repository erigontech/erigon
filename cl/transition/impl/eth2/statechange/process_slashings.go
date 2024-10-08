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
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func processSlashings(s abstract.BeaconState, slashingMultiplier uint64) error {
	// Get the current epoch
	epoch := state.Epoch(s)
	// Get the total active balance
	totalBalance := s.GetTotalActiveBalance()
	// Calculate the total slashing amount
	// by summing all slashings and multiplying by the provided multiplier
	slashing := state.GetTotalSlashingAmount(s) * slashingMultiplier
	// Adjust the total slashing amount to be no greater than the total active balance
	if totalBalance < slashing {
		slashing = totalBalance
	}
	beaconConfig := s.BeaconConfig()
	// Apply penalties to validators who have been slashed and reached the withdrawable epoch
	return ParallellForLoop(runtime.NumCPU(), 0, s.ValidatorSet().Length(), func(i int) error {
		validator := s.ValidatorSet().Get(i)
		if !validator.Slashed() || epoch+beaconConfig.EpochsPerSlashingsVector/2 != validator.WithdrawableEpoch() {
			return nil
		}
		// Get the effective balance increment
		increment := beaconConfig.EffectiveBalanceIncrement
		// Calculate the penalty numerator by multiplying the validator's effective balance by the total slashing amount
		penaltyNumerator := validator.EffectiveBalance() / increment * slashing
		// Calculate the penalty by dividing the penalty numerator by the total balance and multiplying by the increment
		penalty := penaltyNumerator / totalBalance * increment
		// Decrease the validator's balance by the calculated penalty
		return state.DecreaseBalance(s, uint64(i), penalty)
	})
}

func ProcessSlashings(state abstract.BeaconState) error {
	// Depending on the version of the state, use different multipliers
	switch state.Version() {
	case clparams.Phase0Version:
		return processSlashings(state, state.BeaconConfig().ProportionalSlashingMultiplier)
	case clparams.AltairVersion:
		return processSlashings(state, state.BeaconConfig().ProportionalSlashingMultiplierAltair)
	default:
		return processSlashings(state, state.BeaconConfig().ProportionalSlashingMultiplierBellatrix)
	}
}
