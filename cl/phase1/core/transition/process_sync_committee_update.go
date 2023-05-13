package transition

import (
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// ProcessSyncCommitteeUpdate implements processing for the sync committee update. unfortunately there is no easy way to test it.
func ProcessSyncCommitteeUpdate(s *state2.BeaconState) error {
	if (state2.Epoch(s.BeaconState)+1)%s.BeaconConfig().EpochsPerSyncCommitteePeriod != 0 {
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
