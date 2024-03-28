package sync_contribution_pool

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

type syncContributionKey struct {
	slot              uint64
	subcommitteeIndex uint64
	beaconBlockRoot   common.Hash
}

type syncContributionPoolImpl struct {
	// syncContributionPool is a map of sync contributions, indexed by slot, subcommittee index and block root.
	syncContributionPool map[syncContributionKey]*cltypes.Contribution

	mu sync.Mutex
}

func NewSyncContributionPool() SyncContributionPool {
	return &syncContributionPoolImpl{
		syncContributionPool: make(map[syncContributionKey]*cltypes.Contribution),
	}
}

func (s *syncContributionPoolImpl) AddSyncContribution(headState *state.CachingBeaconState, contribution *cltypes.Contribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := syncContributionKey{
		slot:              contribution.Slot,
		subcommitteeIndex: contribution.SubcommitteeIndex,
		beaconBlockRoot:   contribution.BeaconBlockRoot,
	}

	if _, ok := s.syncContributionPool[key]; ok {
		return nil
	}

	// Make a copy.
	s.syncContributionPool[key] = &cltypes.Contribution{
		Slot:              contribution.Slot,
		SubcommitteeIndex: contribution.SubcommitteeIndex,
		BeaconBlockRoot:   contribution.BeaconBlockRoot,
		AggregationBits:   common.Copy(contribution.AggregationBits),
	}
	s.cleanupOldContributions(headState)
	return nil
}

func (s *syncContributionPoolImpl) GetSyncContribution(slot, subcommitteeIndex uint64, beaconBlockRoot common.Hash) *cltypes.Contribution {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := syncContributionKey{
		slot:              slot,
		subcommitteeIndex: subcommitteeIndex,
		beaconBlockRoot:   beaconBlockRoot,
	}

	contribution, ok := s.syncContributionPool[key]
	// Return a copies.
	if !ok {
		// if we dont have it return an empty contribution (no aggregation bits).
		return &cltypes.Contribution{
			Slot:              slot,
			SubcommitteeIndex: subcommitteeIndex,
			BeaconBlockRoot:   beaconBlockRoot,
			AggregationBits:   make([]byte, cltypes.SyncCommitteeAggregationBitsSize),
		}
	}
	return contribution.Copy()
}

func (s *syncContributionPoolImpl) cleanupOldContributions(headState *state.CachingBeaconState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.syncContributionPool {
		if headState.Slot() != key.slot {
			delete(s.syncContributionPool, key)
		}
	}
}

// AddSyncCommitteeMessage aggregates a sync committee message to a contribution to the pool.
func (s *syncContributionPoolImpl) AddSyncCommitteeMessage(headState *state.CachingBeaconState, subCommittee uint64, message *cltypes.SyncCommitteeMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := syncContributionKey{
		slot:              message.Slot,
		subcommitteeIndex: subCommittee,
		beaconBlockRoot:   message.BeaconBlockRoot,
	}

	// We retrieve a base contribution
	contribution, ok := s.syncContributionPool[key]
	if !ok {
		contribution = &cltypes.Contribution{
			Slot:              message.Slot,
			SubcommitteeIndex: subCommittee,
			BeaconBlockRoot:   message.BeaconBlockRoot,
			AggregationBits:   make([]byte, cltypes.SyncCommitteeAggregationBitsSize),
		}
	}
	// We use the a copy of this contribution
	contribution = contribution.Copy() // make a copy

}
