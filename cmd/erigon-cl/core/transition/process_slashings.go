package transition

import "github.com/ledgerwatch/erigon/cl/clparams"

func (s *StateTransistor) processSlashings(slashingMultiplier uint64) error {
	// Get the current epoch
	epoch := s.state.Epoch()
	// Get the total active balance
	totalBalance, err := s.state.GetTotalActiveBalance()
	if err != nil {
		return err
	}
	// Calculate the total slashing amount
	// by summing all slashings and multiplying by the provided multiplier
	slashing := s.state.GetTotalSlashingAmount() * slashingMultiplier
	// Adjust the total slashing amount to be no greater than the total active balance
	if totalBalance < slashing {
		slashing = totalBalance
	}
	// Apply penalties to validators who have been slashed and reached the withdrawable epoch
	for i, validator := range s.state.Validators() {
		if !validator.Slashed || epoch+s.beaconConfig.EpochsPerSlashingsVector/2 != validator.WithdrawableEpoch {
			continue
		}
		// Get the effective balance increment
		increment := s.beaconConfig.EffectiveBalanceIncrement
		// Calculate the penalty numerator by multiplying the validator's effective balance by the total slashing amount
		penaltyNumerator := validator.EffectiveBalance / increment * slashing
		// Calculate the penalty by dividing the penalty numerator by the total balance and multiplying by the increment
		penalty := penaltyNumerator / totalBalance * increment
		// Decrease the validator's balance by the calculated penalty
		s.state.DecreaseBalance(uint64(i), penalty)
	}
	return nil
}

func (s *StateTransistor) ProcessSlashings() error {
	// Depending on the version of the state, use different multipliers
	switch s.state.Version() {
	case clparams.Phase0Version:
		return s.processSlashings(s.beaconConfig.ProportionalSlashingMultiplier)
	case clparams.AltairVersion:
		return s.processSlashings(s.beaconConfig.ProportionalSlashingMultiplierAltair)
	default:
		return s.processSlashings(s.beaconConfig.ProportionalSlashingMultiplierBellatrix)
	}
}
