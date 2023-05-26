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
	// Also mark all current attesters as previous
	state.ForEachValidator(func(_ solid.Validator, idx, total int) bool {
		phase0Data := state.Phase0DataForValidatorIndex(idx)

		// Previous sources/target/head
		phase0Data.IsPreviousMatchingSourceAttester = phase0Data.IsCurrentMatchingSourceAttester
		phase0Data.IsPreviousMatchingTargetAttester = phase0Data.IsCurrentMatchingTargetAttester
		phase0Data.IsPreviousMatchingHeadAttester = phase0Data.IsCurrentMatchingHeadAttester
		phase0Data.MinPreviousInclusionDelayAttestation = phase0Data.MinCurrentInclusionDelayAttestation
		// Current sources/target/head
		phase0Data.MinCurrentInclusionDelayAttestation = nil
		phase0Data.IsCurrentMatchingSourceAttester = false
		phase0Data.IsCurrentMatchingTargetAttester = false
		phase0Data.IsCurrentMatchingHeadAttester = false
		return true
	})
	return nil
}
