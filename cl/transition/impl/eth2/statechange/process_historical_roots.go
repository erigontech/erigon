package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

func ProcessParticipationRecordUpdates(s abstract.BeaconState) error {
	s.SetPreviousEpochAttestations(s.CurrentEpochAttestations())
	s.ResetCurrentEpochAttestations()
	var err error
	// Also mark all current attesters as previous
	s.ForEachValidator(func(_ solid.Validator, idx, total int) bool {

		var oldCurrentMatchingSourceAttester, oldCurrentMatchingTargetAttester, oldCurrentMatchingHeadAttester bool
		var oldMinCurrentInclusionDelayAttestation *solid.PendingAttestation

		if oldCurrentMatchingSourceAttester, err = s.ValidatorIsCurrentMatchingSourceAttester(idx); err != nil {
			return false
		}
		if oldCurrentMatchingTargetAttester, err = s.ValidatorIsCurrentMatchingTargetAttester(idx); err != nil {
			return false
		}
		if oldCurrentMatchingHeadAttester, err = s.ValidatorIsCurrentMatchingHeadAttester(idx); err != nil {
			return false
		}
		if oldMinCurrentInclusionDelayAttestation, err = s.ValidatorMinCurrentInclusionDelayAttestation(idx); err != nil {
			return false
		}
		// Previous sources/target/head
		if err = s.SetValidatorIsPreviousMatchingSourceAttester(idx, oldCurrentMatchingSourceAttester); err != nil {
			return false
		}
		if err = s.SetValidatorIsPreviousMatchingTargetAttester(idx, oldCurrentMatchingTargetAttester); err != nil {
			return false
		}
		if err = s.SetValidatorIsPreviousMatchingHeadAttester(idx, oldCurrentMatchingHeadAttester); err != nil {
			return false
		}
		if err = s.SetValidatorMinPreviousInclusionDelayAttestation(idx, oldMinCurrentInclusionDelayAttestation); err != nil {
			return false
		}
		// Current sources/target/head
		if err = s.SetValidatorIsCurrentMatchingSourceAttester(idx, false); err != nil {
			return false
		}
		if err = s.SetValidatorIsCurrentMatchingTargetAttester(idx, false); err != nil {
			return false
		}
		if err = s.SetValidatorIsCurrentMatchingHeadAttester(idx, false); err != nil {
			return false
		}
		if err = s.SetValidatorMinCurrentInclusionDelayAttestation(idx, nil); err != nil {
			return false
		}
		return true
	})
	return err
}
