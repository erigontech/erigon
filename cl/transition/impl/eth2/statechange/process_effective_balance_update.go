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
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// ProcessEffectiveBalanceUpdates updates the effective balance of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#effective-balances-updates
func ProcessEffectiveBalanceUpdates(state abstract.BeaconState) error {
	beaconConfig := state.BeaconConfig()
	// Define non-changing constants to avoid recomputation.
	histeresisIncrement := beaconConfig.EffectiveBalanceIncrement / beaconConfig.HysteresisQuotient
	downwardThreshold := histeresisIncrement * beaconConfig.HysteresisDownwardMultiplier
	upwardThreshold := histeresisIncrement * beaconConfig.HysteresisUpwardMultiplier
	// Iterate over validator set and compute the diff of each validator.
	var err error
	var balance uint64
	state.ForEachValidator(func(validator solid.Validator, index, total int) bool {
		balance, err = state.ValidatorBalance(index)
		if err != nil {
			return false
		}
		eb := validator.EffectiveBalance()
		if balance+downwardThreshold < eb || eb+upwardThreshold < balance {
			// Set new effective balance
			effectiveBalance := min(balance-(balance%beaconConfig.EffectiveBalanceIncrement), beaconConfig.MaxEffectiveBalance)
			state.SetEffectiveBalanceForValidatorAtIndex(index, effectiveBalance)
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}
