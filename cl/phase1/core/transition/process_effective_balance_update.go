package transition

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessEffectiveBalanceUpdates updates the effective balance of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#effective-balances-updates
func ProcessEffectiveBalanceUpdates(state *state.BeaconState) error {
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
			effectiveBalance := utils.Min64(balance-(balance%beaconConfig.EffectiveBalanceIncrement), beaconConfig.MaxEffectiveBalance)
			state.SetEffectiveBalanceForValidatorAtIndex(index, effectiveBalance)
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}
