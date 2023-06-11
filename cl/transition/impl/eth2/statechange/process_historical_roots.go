package statechange

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func ProcessParticipationRecordUpdates(state *state.BeaconState) error {
	state.SetPreviousEpochAttestations(state.CurrentEpochAttestations())
	state.ResetCurrentEpochAttestations()
	var err error
	// Also mark all current attesters as previous
	state.ForEachValidator(func(_ solid.Validator, idx, total int) bool {

		var oldCurrentMatchingSourceAttester, oldCurrentMatchingTargetAttester, oldCurrentMatchingHeadAttester bool
		var oldMinCurrentInclusionDelayAttestation *solid.PendingAttestation

		if oldCurrentMatchingSourceAttester, err = state.ValidatorIsCurrentMatchingSourceAttester(idx); err != nil {
			return false
		}
		if oldCurrentMatchingTargetAttester, err = state.ValidatorIsCurrentMatchingTargetAttester(idx); err != nil {
			return false
		}
		if oldCurrentMatchingHeadAttester, err = state.ValidatorIsCurrentMatchingHeadAttester(idx); err != nil {
			return false
		}
		if oldMinCurrentInclusionDelayAttestation, err = state.ValidatorMinCurrentInclusionDelayAttestation(idx); err != nil {
			return false
		}
		// Previous sources/target/head
		if err = state.SetValidatorIsPreviousMatchingSourceAttester(idx, oldCurrentMatchingSourceAttester); err != nil {
			return false
		}
		if err = state.SetValidatorIsPreviousMatchingTargetAttester(idx, oldCurrentMatchingTargetAttester); err != nil {
			return false
		}
		if err = state.SetValidatorIsPreviousMatchingHeadAttester(idx, oldCurrentMatchingHeadAttester); err != nil {
			return false
		}
		if err = state.SetValidatorMinPreviousInclusionDelayAttestation(idx, oldMinCurrentInclusionDelayAttestation); err != nil {
			return false
		}
		// Current sources/target/head
		if err = state.SetValidatorIsCurrentMatchingSourceAttester(idx, false); err != nil {
			return false
		}
		if err = state.SetValidatorIsCurrentMatchingTargetAttester(idx, false); err != nil {
			return false
		}
		if err = state.SetValidatorIsCurrentMatchingHeadAttester(idx, false); err != nil {
			return false
		}
		if err = state.SetValidatorMinCurrentInclusionDelayAttestation(idx, nil); err != nil {
			return false
		}
		return true
	})
	return err
}
