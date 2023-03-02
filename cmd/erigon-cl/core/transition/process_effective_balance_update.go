package transition

import (
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessEffectiveBalanceUpdates updates the effective balance of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#effective-balances-updates
func (s *StateTransistor) ProcessEffectiveBalanceUpdates() error {
	// Define non-changing constants to avoid recomputation.
	histeresisIncrement := s.beaconConfig.EffectiveBalanceIncrement / s.beaconConfig.HysteresisQuotient
	downwardThreshold := histeresisIncrement * s.beaconConfig.HysteresisDownwardMultiplier
	upwardThreshold := histeresisIncrement * s.beaconConfig.HysteresisUpwardMultiplier
	// Iterate over validator set and compute the diff of each validator.
	for index, validator := range s.state.Validators() {
		balance, err := s.state.ValidatorBalance(index)
		if err != nil {
			return err
		}
		if balance+downwardThreshold < validator.EffectiveBalance ||
			validator.EffectiveBalance+upwardThreshold < balance {
			// Set new effective balance
			validator.EffectiveBalance = utils.Min64(balance-(balance%s.beaconConfig.EffectiveBalanceIncrement), s.beaconConfig.MaxEffectiveBalance)
			if err := s.state.SetValidatorAt(index, validator); err != nil {
				return err
			}
		}
	}
	return nil
}
