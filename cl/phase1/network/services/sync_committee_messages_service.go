// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package services

import (
	"context"
	"fmt"
	"slices"
	"sync"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
)

type seenSyncCommitteeMessage struct {
	subnet         uint64
	slot           uint64
	validatorIndex uint64
}

type syncCommitteeMessagesService struct {
	seenSyncCommitteeMessages sync.Map
	syncedDataManager         *synced_data.SyncedDataManager
	beaconChainCfg            *clparams.BeaconChainConfig
	syncContributionPool      sync_contribution_pool.SyncContributionPool
	ethClock                  eth_clock.EthereumClock
	batchSignatureVerifier    *BatchSignatureVerifier
	test                      bool

	mu sync.Mutex
}

type SyncCommitteeMessageForGossip struct {
	SyncCommitteeMessage  *cltypes.SyncCommitteeMessage
	Receiver              *sentinel.Peer
	ImmediateVerification bool
}

// NewSyncCommitteeMessagesService creates a new sync committee messages service
func NewSyncCommitteeMessagesService(
	beaconChainCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	syncedDataManager *synced_data.SyncedDataManager,
	syncContributionPool sync_contribution_pool.SyncContributionPool,
	batchSignatureVerifier *BatchSignatureVerifier,
	test bool,
) SyncCommitteeMessagesService {
	return &syncCommitteeMessagesService{
		ethClock:               ethClock,
		syncedDataManager:      syncedDataManager,
		beaconChainCfg:         beaconChainCfg,
		syncContributionPool:   syncContributionPool,
		batchSignatureVerifier: batchSignatureVerifier,
		test:                   test,
	}
}

// ProcessMessage processes a sync committee message
func (s *syncCommitteeMessagesService) ProcessMessage(ctx context.Context, subnet *uint64, msg *SyncCommitteeMessageForGossip) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The message's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. sync_committee_message.slot == current_slot.
		if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot) {
			return ErrIgnore
		}
		// [REJECT] The subnet_id is valid for the given validator, i.e. subnet_id in compute_subnets_for_sync_committee(state, sync_committee_message.validator_index).
		// Note this validation implies the validator is part of the broader current sync committee along with the correct subcommittee.
		subnets, err := subnets.ComputeSubnetsForSyncCommittee(headState, msg.SyncCommitteeMessage.ValidatorIndex)
		if err != nil {
			return err
		}
		seenSyncCommitteeMessageIdentifier := seenSyncCommitteeMessage{
			subnet:         *subnet,
			slot:           msg.SyncCommitteeMessage.Slot,
			validatorIndex: msg.SyncCommitteeMessage.ValidatorIndex,
		}

		if !slices.Contains(subnets, *subnet) {
			return fmt.Errorf("validator is not into any subnet %d", *subnet)
		}
		// [IGNORE] There has been no other valid sync committee message for the declared slot for the validator referenced by sync_committee_message.validator_index.

		if _, ok := s.seenSyncCommitteeMessages.Load(seenSyncCommitteeMessageIdentifier); ok {
			return ErrIgnore
		}
		// [REJECT] The signature is valid for the message beacon_block_root for the validator referenced by validator_index
		signature, signingRoot, pubKey, err := verifySyncCommitteeMessageSignature(headState, msg.SyncCommitteeMessage)
		if !s.test && err != nil {
			return err
		}
		aggregateVerificationData := &AggregateVerificationData{
			Signatures:  [][]byte{signature},
			SignRoots:   [][]byte{signingRoot},
			Pks:         [][]byte{pubKey},
			SendingPeer: msg.Receiver,
			F: func() {
				s.seenSyncCommitteeMessages.Store(seenSyncCommitteeMessageIdentifier, struct{}{})
				s.cleanupOldSyncCommitteeMessages() // cleanup old messages
				// ImmediateVerification is sequential so using the headState directly is safe
				if msg.ImmediateVerification {
					s.syncContributionPool.AddSyncCommitteeMessage(headState, *subnet, msg.SyncCommitteeMessage)
				} else {
					// ImmediateVerification=false is parallel so using the headState directly is unsafe
					s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
						return s.syncContributionPool.AddSyncCommitteeMessage(headState, *subnet, msg.SyncCommitteeMessage)
					})
				}
			},
		}

		if msg.ImmediateVerification {
			return s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
		} else {
			// push the signatures to verify asynchronously and run final functions after that.
			s.batchSignatureVerifier.AsyncVerifySyncCommitteeMessage(aggregateVerificationData)
		}

		// As the logic goes, if we return ErrIgnore there will be no peer banning and further publishing
		// gossip data into the network by the gossip manager. That's what we want because we will be doing that ourselves
		// in BatchSignatureVerifier service. After validating signatures, if they are valid we will publish the
		// gossip ourselves or ban the peer which sent that particular invalid signature.
		return ErrIgnore
	})
}

// cleanupOldSyncCommitteeMessages removes old sync committee messages from the cache
func (s *syncCommitteeMessagesService) cleanupOldSyncCommitteeMessages() {
	headSlot := s.syncedDataManager.HeadSlot()

	entriesToRemove := []seenSyncCommitteeMessage{}
	s.seenSyncCommitteeMessages.Range(func(key, value interface{}) bool {
		k := key.(seenSyncCommitteeMessage)
		if headSlot > k.slot+1 {
			entriesToRemove = append(entriesToRemove, k)
		}
		return true
	})
	for _, k := range entriesToRemove {
		s.seenSyncCommitteeMessages.Delete(k)
	}
}

// verifySyncCommitteeMessageSignature verifies the signature of a sync committee message
func verifySyncCommitteeMessageSignature(s *state.CachingBeaconState, msg *cltypes.SyncCommitteeMessage) ([]byte, []byte, []byte, error) {
	publicKey, err := s.ValidatorPublicKey(int(msg.ValidatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	cfg := s.BeaconConfig()
	domain, err := s.GetDomain(cfg.DomainSyncCommittee, state.Epoch(s))
	if err != nil {
		return nil, nil, nil, err
	}
	signingRoot := utils.Sha256(msg.BeaconBlockRoot[:], domain)
	return msg.Signature[:], signingRoot[:], publicKey[:], nil
}
