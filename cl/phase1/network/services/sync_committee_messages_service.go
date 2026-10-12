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

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type seenSyncCommitteeMessage struct {
	subnet         uint64
	slot           uint64
	validatorIndex uint64
}

// verifiedSyncCommitteeMessage is what the seen-key cache actually
// authenticated: the content of the one message that was signature-checked
// for that key, not just the key itself. A later submission under the same
// key is only a genuine duplicate - safe to treat as already-validated
// without re-verifying - if it carries this same content.
type verifiedSyncCommitteeMessage struct {
	beaconBlockRoot common.Hash
	signature       common.Bytes96
	// published is set by MarkPublished once a caller has successfully
	// admitted this exact content to the gossip publish queue. Until then,
	// a matching duplicate still returns nil so a retry after a failed
	// admission attempt (e.g. the queue was full) gets another chance to
	// publish, rather than being silently dropped by the seen-key cache.
	published bool
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
	Receiver              *sentinelproto.Peer
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

func (s *syncCommitteeMessagesService) Names() []string {
	names := make([]string, 0, s.beaconChainCfg.SyncCommitteeSubnetCount)
	for i := 0; i < int(s.beaconChainCfg.SyncCommitteeSubnetCount); i++ {
		names = append(names, gossip.TopicNameSyncCommittee(i))
	}
	return names
}

func (s *syncCommitteeMessagesService) IsMyGossipMessage(name string) bool {
	return gossip.IsTopicSyncCommittee(name)
}

func (s *syncCommitteeMessagesService) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (*SyncCommitteeMessageForGossip, error) {
	obj := &SyncCommitteeMessageForGossip{
		Receiver:             &sentinelproto.Peer{Pid: pid.String()},
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{},
	}
	if err := obj.SyncCommitteeMessage.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
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
		//
		// A hit here means some message for this key was already verified, not that this one was:
		// only a submission carrying that same content is a genuine duplicate, safe to wave through
		// without re-verifying. Anything else - a forged replacement, or a bug - is ignored exactly
		// like a first-time duplicate would be, without ever checking its signature. A genuine
		// duplicate is only ignored once it has actually been published; until then it still returns
		// nil so a caller whose earlier admission attempt failed gets another chance.
		if verified, ok := s.seenSyncCommitteeMessages.Load(seenSyncCommitteeMessageIdentifier); ok {
			v := verified.(verifiedSyncCommitteeMessage)
			if v.beaconBlockRoot != msg.SyncCommitteeMessage.BeaconBlockRoot || v.signature != msg.SyncCommitteeMessage.Signature {
				return ErrIgnore
			}
			if v.published {
				return ErrIgnore
			}
			return nil
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
				s.seenSyncCommitteeMessages.Store(seenSyncCommitteeMessageIdentifier, verifiedSyncCommitteeMessage{
					beaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
					signature:       msg.SyncCommitteeMessage.Signature,
				})
				s.cleanupOldSyncCommitteeMessages() // cleanup old messages
				// ImmediateVerification is sequential so using the headState directly is safe
				if msg.ImmediateVerification {
					if err := s.syncContributionPool.AddSyncCommitteeMessage(headState, *subnet, msg.SyncCommitteeMessage); err != nil {
						log.Debug("failed to add sync committee message to pool", "err", err)
					}
				} else {
					// ImmediateVerification=false is parallel so using the headState directly is unsafe
					if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
						return s.syncContributionPool.AddSyncCommitteeMessage(headState, *subnet, msg.SyncCommitteeMessage)
					}); err != nil {
						log.Debug("failed to add sync committee message to pool", "err", err)
					}
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
		return nil
	})
}

// MarkPublished records that content ProcessMessage already verified for
// this key was successfully admitted to the gossip publish queue. A later
// submission of that same content becomes ErrIgnore instead of nil, so a
// caller does not spend another admission attempt on a message already
// queued. If the entry no longer exists (evicted, or never verified with
// this content), there is nothing to mark - the next submission of this
// content is simply verified as if it were new.
func (s *syncCommitteeMessagesService) MarkPublished(subnet, slot, validatorIndex uint64, beaconBlockRoot common.Hash, signature common.Bytes96) {
	key := seenSyncCommitteeMessage{subnet: subnet, slot: slot, validatorIndex: validatorIndex}
	verified, ok := s.seenSyncCommitteeMessages.Load(key)
	if !ok {
		return
	}
	v := verified.(verifiedSyncCommitteeMessage)
	if v.beaconBlockRoot != beaconBlockRoot || v.signature != signature || v.published {
		return
	}
	v.published = true
	s.seenSyncCommitteeMessages.CompareAndSwap(key, verified, v)
}

// cleanupOldSyncCommitteeMessages removes old sync committee messages from the cache
func (s *syncCommitteeMessagesService) cleanupOldSyncCommitteeMessages() {
	headSlot := s.syncedDataManager.HeadSlot()

	entriesToRemove := []seenSyncCommitteeMessage{}
	s.seenSyncCommitteeMessages.Range(func(key, value any) bool {
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
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainSyncCommittee, state.GetEpochAtSlot(cfg, msg.Slot), s.GenesisValidatorsRoot())
	if err != nil {
		return nil, nil, nil, err
	}
	signingRoot := crypto.Sha256(msg.BeaconBlockRoot[:], domain)
	return msg.Signature[:], signingRoot[:], publicKey[:], nil
}
