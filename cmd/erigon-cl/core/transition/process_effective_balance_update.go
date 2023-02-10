package transition

// ProcessEffectiveBalanceUpdates updates the effective balance of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#effective-balances-updates
func (s *StateTransistor) ProcessEffectiveBalanceUpdates() error {
	histeresisIncrement := s.beaconConfig.EffectiveBalanceIncrement / s.beaconConfig.HysteresisQuotient
	downwardThreshold := histeresisIncrement * s.beaconConfig.HysteresisDownwardMultiplier
	upwardThreshold := histeresisIncrement * s.beaconConfig.HysteresisUpwardMultiplier
	for index, validator := range s.state.Validators() {
		balance, err := s.state.ValidatorBalance(index)
		if err != nil {
			return err
		}
		if balance+downwardThreshold < validator.EffectiveBalance ||
			validator.EffectiveBalance+upwardThreshold < balance {
			validator.EffectiveBalance = balance - balance%s.beaconConfig.EffectiveBalanceIncrement
			if validator.EffectiveBalance > s.beaconConfig.MaxEffectiveBalance {
				validator.EffectiveBalance = s.beaconConfig.MaxEffectiveBalance
			}
			if err := s.state.SetValidatorAt(index, validator); err != nil {
				return err
			}
		}
	}
	return nil
}
