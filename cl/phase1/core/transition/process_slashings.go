package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func processSlashings(s *state2.BeaconState, slashingMultiplier uint64) error {
	// Get the current epoch
	epoch := state2.Epoch(s.BeaconState)
	// Get the total active balance
	totalBalance := s.GetTotalActiveBalance()
	// Calculate the total slashing amount
	// by summing all slashings and multiplying by the provided multiplier
	slashing := state2.GetTotalSlashingAmount(s.BeaconState) * slashingMultiplier
	// Adjust the total slashing amount to be no greater than the total active balance
	if totalBalance < slashing {
		slashing = totalBalance
	}
	beaconConfig := s.BeaconConfig()
	// Apply penalties to validators who have been slashed and reached the withdrawable epoch
	var err error
	s.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		if !validator.Slashed() || epoch+beaconConfig.EpochsPerSlashingsVector/2 != validator.WithdrawableEpoch() {
			return true
		}
		// Get the effective balance increment
		increment := beaconConfig.EffectiveBalanceIncrement
		// Calculate the penalty numerator by multiplying the validator's effective balance by the total slashing amount
		penaltyNumerator := validator.EffectiveBalance() / increment * slashing
		// Calculate the penalty by dividing the penalty numerator by the total balance and multiplying by the increment
		penalty := penaltyNumerator / totalBalance * increment
		// Decrease the validator's balance by the calculated penalty
		if err = state2.DecreaseBalance(s.BeaconState, uint64(i), penalty); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func ProcessSlashings(state *state2.BeaconState) error {
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
