package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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
	// Also mark all current attesters as previous
	state.ForEachValidator(func(validator *cltypes.Validator, idx, total int) bool {
		// Previous sources/target/head
		validator.IsPreviousMatchingSourceAttester = validator.IsCurrentMatchingSourceAttester
		validator.IsPreviousMatchingTargetAttester = validator.IsCurrentMatchingTargetAttester
		validator.IsPreviousMatchingHeadAttester = validator.IsCurrentMatchingHeadAttester
		validator.MinPreviousInclusionDelayAttestation = validator.MinCurrentInclusionDelayAttestation
		// Current sources/target/head
		validator.MinCurrentInclusionDelayAttestation = nil
		validator.IsCurrentMatchingSourceAttester = false
		validator.IsCurrentMatchingTargetAttester = false
		validator.IsCurrentMatchingHeadAttester = false
		return true
	})
	return nil
}
