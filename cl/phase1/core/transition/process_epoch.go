package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// ProcessEpoch process epoch transition.
func ProcessEpoch(state *state.BeaconState) error {
	if err := ProcessJustificationBitsAndFinality(state); err != nil {
		return err
	}
	if state.Version() >= clparams.AltairVersion {
		if err := ProcessInactivityScores(state); err != nil {
			return err
		}
	}
	if err := ProcessRewardsAndPenalties(state); err != nil {
		return err
	}
	if err := ProcessRegistryUpdates(state); err != nil {
		return err
	}
	if err := ProcessSlashings(state); err != nil {
		return err
	}
	ProcessEth1DataReset(state)
	if err := ProcessEffectiveBalanceUpdates(state); err != nil {
		return err
	}
	ProcessSlashingsReset(state)
	ProcessRandaoMixesReset(state)
	if err := ProcessHistoricalRootsUpdate(state); err != nil {
		return err
	}
	if state.Version() == clparams.Phase0Version {
		if err := ProcessParticipationRecordUpdates(state); err != nil {
			return err
		}
	}

	if state.Version() >= clparams.AltairVersion {
		ProcessParticipationFlagUpdates(state)
		if err := ProcessSyncCommitteeUpdate(state); err != nil {
			return err
		}
	}
	return nil
}

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
