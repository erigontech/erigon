package transition

import (
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessSyncCommitteeUpdate implements processing for the sync committee update. unfortunately there is no easy way to test it.
func ProcessSyncCommitteeUpdate(state *state.BeaconState) error {
	if (state.Epoch()+1)%state.BeaconConfig().EpochsPerSyncCommitteePeriod != 0 {
		return nil
	}
	// Set new current sync committee.
	state.SetCurrentSyncCommittee(state.NextSyncCommittee())
	// Compute next new sync committee
	committee, err := state.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	state.SetNextSyncCommittee(committee)
	return nil
}
