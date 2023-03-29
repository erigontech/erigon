package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessEffectiveBalanceUpdates updates the effective balance of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#effective-balances-updates
func ProcessEffectiveBalanceUpdates(state *state.BeaconState) error {
	beaconConfig := state.BeaconConfig()
	// Define non-changing constants to avoid recomputation.
	histeresisIncrement := beaconConfig.EffectiveBalanceIncrement / beaconConfig.HysteresisQuotient
	downwardThreshold := histeresisIncrement * beaconConfig.HysteresisDownwardMultiplier
	upwardThreshold := histeresisIncrement * beaconConfig.HysteresisUpwardMultiplier
	// Iterate over validator set and compute the diff of each validator.
	for index, validator := range state.Validators() {
		balance, err := state.ValidatorBalance(index)
		if err != nil {
			return err
		}
		if balance+downwardThreshold < validator.EffectiveBalance ||
			validator.EffectiveBalance+upwardThreshold < balance {
			// Set new effective balance
			validator.EffectiveBalance = utils.Min64(balance-(balance%beaconConfig.EffectiveBalanceIncrement), beaconConfig.MaxEffectiveBalance)
			if err := state.SetValidatorAt(index, validator); err != nil {
				return err
			}
		}
	}
	return nil
}
