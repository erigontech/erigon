package sync_contribution_pool

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

type syncContributionPoolMock struct {
	// syncContributionPool is a map of sync contributions, indexed by slot, subcommittee index and block root.
	syncContributionPool map[syncContributionKey]*cltypes.Contribution
}

func NewSyncContributionPoolMock() SyncContributionPool {
	return &syncContributionPoolMock{
		syncContributionPool: make(map[syncContributionKey]*cltypes.Contribution),
	}
}

// AddSyncContribution adds a sync committee contribution to the pool.
func (s *syncContributionPoolMock) AddSyncContribution(headState *state.CachingBeaconState, contribution *cltypes.Contribution) error {

	key := syncContributionKey{
		slot:              contribution.Slot,
		subcommitteeIndex: contribution.SubcommitteeIndex,
		beaconBlockRoot:   contribution.BeaconBlockRoot,
	}
	s.syncContributionPool[key] = contribution
	return nil
}

// AddSyncCommitteeMessage aggretates a sync committee message to a contribution to the pool.
func (s *syncContributionPoolMock) AddSyncCommitteeMessage(headState *state.CachingBeaconState, subCommitee uint64, message *cltypes.SyncCommitteeMessage) error {
	key := syncContributionKey{
		slot:              message.Slot,
		subcommitteeIndex: subCommitee,
		beaconBlockRoot:   message.BeaconBlockRoot,
	}
	s.syncContributionPool[key] = &cltypes.Contribution{
		Slot:              message.Slot,
		SubcommitteeIndex: subCommitee,
		BeaconBlockRoot:   message.BeaconBlockRoot,
		AggregationBits:   make([]byte, cltypes.SyncCommitteeAggregationBitsSize),
	}
	return nil
}

// GetSyncContribution retrieves a sync contribution from the pool.
func (s *syncContributionPoolMock) GetSyncContribution(slot, subcommitteeIndex uint64, beaconBlockRoot common.Hash) *cltypes.Contribution {
	key := syncContributionKey{
		slot:              slot,
		subcommitteeIndex: subcommitteeIndex,
		beaconBlockRoot:   beaconBlockRoot,
	}
	return s.syncContributionPool[key]
}
