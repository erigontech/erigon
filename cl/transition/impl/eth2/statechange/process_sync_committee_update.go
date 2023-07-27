package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// ProcessSyncCommitteeUpdate implements processing for the sync committee update. unfortunately there is no easy way to test it.
func ProcessSyncCommitteeUpdate(s abstract.BeaconState) error {
	if (state.Epoch(s)+1)%s.BeaconConfig().EpochsPerSyncCommitteePeriod != 0 {
		return nil
	}
	// Set new current sync committee.
	s.SetCurrentSyncCommittee(s.NextSyncCommittee())
	// Compute next new sync committee
	committee, err := s.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	s.SetNextSyncCommittee(committee)
	return nil
}
