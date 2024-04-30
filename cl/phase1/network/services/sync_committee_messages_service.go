package services

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/Giulio2002/bls"

	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/network/subnets"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/cl/validator/sync_contribution_pool"
)

type seenSyncCommitteeMessage struct {
	subnet         uint64
	slot           uint64
	validatorIndex uint64
}

type syncCommitteeMessagesService struct {
	seenSyncCommitteeMessages map[seenSyncCommitteeMessage]struct{}
	syncedDataManager         *synced_data.SyncedDataManager
	beaconChainCfg            *clparams.BeaconChainConfig
	syncContributionPool      sync_contribution_pool.SyncContributionPool
	ethClock                  eth_clock.EthereumClock
	test                      bool

	mu sync.Mutex
}

// NewSyncCommitteeMessagesService creates a new sync committee messages service
func NewSyncCommitteeMessagesService(
	beaconChainCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	syncedDataManager *synced_data.SyncedDataManager,
	syncContributionPool sync_contribution_pool.SyncContributionPool,
	test bool,
) SyncCommitteeMessagesService {
	return &syncCommitteeMessagesService{
		seenSyncCommitteeMessages: make(map[seenSyncCommitteeMessage]struct{}),
		ethClock:                  ethClock,
		syncedDataManager:         syncedDataManager,
		beaconChainCfg:            beaconChainCfg,
		syncContributionPool:      syncContributionPool,
		test:                      test,
	}
}

// ProcessMessage processes a sync committee message
func (s *syncCommitteeMessagesService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.SyncCommitteeMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	headState := s.syncedDataManager.HeadState()
	if headState == nil {
		return ErrIgnore
	}
	// [IGNORE] The message's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. sync_committee_message.slot == current_slot.
	if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.Slot) {
		return ErrIgnore
	}

	// [REJECT] The subnet_id is valid for the given validator, i.e. subnet_id in compute_subnets_for_sync_committee(state, sync_committee_message.validator_index).
	// Note this validation implies the validator is part of the broader current sync committee along with the correct subcommittee.
	subnets, err := subnets.ComputeSubnetsForSyncCommittee(headState, msg.ValidatorIndex)
	if err != nil {
		return err
	}
	seenSyncCommitteeMessageIdentifier := seenSyncCommitteeMessage{
		subnet:         *subnet,
		slot:           msg.Slot,
		validatorIndex: msg.ValidatorIndex,
	}

	if !slices.Contains(subnets, *subnet) {
		return fmt.Errorf("validator is not into any subnet %d", *subnet)
	}
	// [IGNORE] There has been no other valid sync committee message for the declared slot for the validator referenced by sync_committee_message.validator_index.
	if _, ok := s.seenSyncCommitteeMessages[seenSyncCommitteeMessageIdentifier]; ok {
		return ErrIgnore
	}
	// [REJECT] The signature is valid for the message beacon_block_root for the validator referenced by validator_index
	if err := verifySyncCommitteeMessageSignature(headState, msg); !s.test && err != nil {
		return err
	}
	s.seenSyncCommitteeMessages[seenSyncCommitteeMessageIdentifier] = struct{}{}
	s.cleanupOldSyncCommitteeMessages() // cleanup old messages
	// Aggregate the message
	return s.syncContributionPool.AddSyncCommitteeMessage(headState, *subnet, msg)
}

// cleanupOldSyncCommitteeMessages removes old sync committee messages from the cache
func (s *syncCommitteeMessagesService) cleanupOldSyncCommitteeMessages() {
	headSlot := s.syncedDataManager.HeadSlot()
	for k := range s.seenSyncCommitteeMessages {
		if headSlot > k.slot+1 {
			delete(s.seenSyncCommitteeMessages, k)
		}
	}
}

// verifySyncCommitteeMessageSignature verifies the signature of a sync committee message
func verifySyncCommitteeMessageSignature(s *state.CachingBeaconState, msg *cltypes.SyncCommitteeMessage) error {
	publicKey, err := s.ValidatorPublicKey(int(msg.ValidatorIndex))
	if err != nil {
		return err
	}
	cfg := s.BeaconConfig()
	domain, err := s.GetDomain(cfg.DomainSyncCommittee, state.Epoch(s))
	if err != nil {
		return err
	}
	signingRoot, err := utils.Sha256(msg.BeaconBlockRoot[:], domain), nil
	if err != nil {
		return err
	}
	valid, err := bls.Verify(msg.Signature[:], signingRoot[:], publicKey[:])
	if err != nil {
		return errors.New("invalid signature")
	}
	if !valid {
		return errors.New("invalid signature")
	}
	return nil
}
