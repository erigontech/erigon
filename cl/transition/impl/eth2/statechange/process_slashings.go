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
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/threading"
)

func ProcessSlashings(s abstract.BeaconState) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessSlashingsTime).End()
	if s.Version().AfterOrEqual(clparams.ElectraVersion) {
		// Switch to Electra slashing
		return processSlashingsElectra(s)
	}

	epoch := state.Epoch(s)
	// Get the total active balance
	totalBalance := s.GetTotalActiveBalance()
	// Calculate the total slashing amount
	// by summing all slashings and multiplying by the provided multiplier
	slashing := state.GetTotalSlashingAmount(s) * s.BeaconConfig().GetProportionalSlashingMultiplier(s.Version())
	// Adjust the total slashing amount to be no greater than the total active balance
	if totalBalance < slashing {
		slashing = totalBalance
	}
	beaconConfig := s.BeaconConfig()
	// Apply penalties to validators who have been slashed and reached the withdrawable epoch
	return threading.ParallellForLoop(1, 0, s.ValidatorSet().Length(), func(i int) error {
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

func processSlashingsElectra(s abstract.BeaconState) error {
	// see: https://github.com/ethereum/consensus-specs/blob/dev/specs/electra/beacon-chain.md#modified-process_slashings
	epoch := state.Epoch(s)
	totalBalance := s.GetTotalActiveBalance()
	adjustTotalSlashingBalance := min(
		state.GetTotalSlashingAmount(s)*s.BeaconConfig().GetProportionalSlashingMultiplier(s.Version()),
		totalBalance,
	)
	cfg := s.BeaconConfig()
	increment := cfg.EffectiveBalanceIncrement
	penaltyPerEffectiveBalanceIncr := adjustTotalSlashingBalance / (totalBalance / increment)
	return threading.ParallellForLoop(1, 0, s.ValidatorSet().Length(), func(i int) error {
		v := s.ValidatorSet().Get(i)
		if !v.Slashed() || epoch+cfg.EpochsPerSlashingsVector/2 != v.WithdrawableEpoch() {
			return nil
		}
		effectiveBalanceIncrements := v.EffectiveBalance() / increment
		penalty := penaltyPerEffectiveBalanceIncr * effectiveBalanceIncrements
		return state.DecreaseBalance(s, uint64(i), penalty)
	})
}
