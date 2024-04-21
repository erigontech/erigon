package sync_contribution_pool

import (
	"errors"
	"sync"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
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

var ErrIsSuperset = errors.New("sync contribution is a superset of existing attestation")

func NewSyncContributionPool() SyncContributionPool {
	return &syncContributionPoolImpl{
		syncContributionPool: make(map[syncContributionKey]*cltypes.Contribution),
	}
}

func getSyncCommitteeFromState(s *state.CachingBeaconState) *solid.SyncCommittee {
	cfg := s.BeaconConfig()
	if cfg.SyncCommitteePeriod(s.Slot()) == cfg.SyncCommitteePeriod(s.Slot()+1) {
		return s.CurrentSyncCommittee()
	}
	return s.NextSyncCommittee()

}

func (s *syncContributionPoolImpl) AddSyncContribution(headState *state.CachingBeaconState, contribution *cltypes.Contribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := syncContributionKey{
		slot:              contribution.Slot,
		subcommitteeIndex: contribution.SubcommitteeIndex,
		beaconBlockRoot:   contribution.BeaconBlockRoot,
	}
	baseContribution := &cltypes.Contribution{
		Slot:              contribution.Slot,
		SubcommitteeIndex: contribution.SubcommitteeIndex,
		BeaconBlockRoot:   contribution.BeaconBlockRoot,
		AggregationBits:   make([]byte, cltypes.SyncCommitteeAggregationBitsSize),
		Signature:         bls.InfiniteSignature,
	}

	if val, ok := s.syncContributionPool[key]; ok {
		baseContribution = val.Copy()
	}
	// Time to aggregate the giga aggregatable.
	if utils.IsSupersetBitlist(baseContribution.AggregationBits, contribution.AggregationBits) {
		return ErrIsSuperset // Skip it if it is just a superset.
	}
	// Aggregate the bits.
	utils.MergeBitlists(baseContribution.AggregationBits, contribution.AggregationBits)
	// Aggregate the signature.
	aggregatedSignature, err := bls.AggregateSignatures([][]byte{
		baseContribution.Signature[:],
		contribution.Signature[:],
	})
	if err != nil {
		return err
	}
	copy(baseContribution.Signature[:], aggregatedSignature)

	// Make a copy.
	s.syncContributionPool[key] = baseContribution.Copy()
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
			Signature:         bls.InfiniteSignature,
		}
	}
	return contribution.Copy()
}

func (s *syncContributionPoolImpl) cleanupOldContributions(headState *state.CachingBeaconState) {

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

	cfg := headState.BeaconConfig()

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
			Signature:         bls.InfiniteSignature,
		}
	}
	// We use the a copy of this contribution
	contribution = contribution.Copy() // make a copy
	// First we find the aggregation bits to which this validator needs to be turned on.
	publicKey, err := headState.ValidatorPublicKey(int(message.ValidatorIndex))
	if err != nil {
		return err
	}

	committee := getSyncCommitteeFromState(headState).GetCommittee()
	subCommitteeSize := cfg.SyncCommitteeSize / cfg.SyncCommitteeSubnetCount
	startSubCommittee := subCommittee * subCommitteeSize
	for i := startSubCommittee; i < startSubCommittee+subCommitteeSize; i++ {
		if committee[i] == publicKey { // turn on this bit
			utils.FlipBitOn(contribution.AggregationBits, int(i-startSubCommittee))
		}
	}
	// Compute the aggregated signature.
	aggregatedSignature, err := bls.AggregateSignatures([][]byte{
		contribution.Signature[:],
		message.Signature[:],
	})
	if err != nil {
		return err
	}
	copy(contribution.Signature[:], aggregatedSignature)
	s.syncContributionPool[key] = contribution
	s.cleanupOldContributions(headState)
	return nil
}
