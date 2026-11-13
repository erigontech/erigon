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
	"errors"
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

func (v verifiedSyncCommitteeMessage) matches(beaconBlockRoot common.Hash, signature common.Bytes96) bool {
	return v.beaconBlockRoot == beaconBlockRoot && v.signature == signature
}

type syncCommitteeMessagesService struct {
	seenSyncCommitteeMessages sync.Map
	syncedDataManager         *synced_data.SyncedDataManager
	beaconChainCfg            *clparams.BeaconChainConfig
	syncContributionPool      sync_contribution_pool.SyncContributionPool
	ethClock                  eth_clock.EthereumClock
	batchSignatureVerifier    *BatchSignatureVerifier
	test                      bool

	poolMu sync.Mutex
	// cleanedAtHeadSlot is the head slot of the last cleanup of seenSyncCommitteeMessages, guarded by poolMu.
	cleanedAtHeadSlot uint64
	// seenMu orders claims of seenSyncCommitteeMessages, under its read lock, against evictions, under its write
	// lock. evictedBelow, guarded by it, only grows: records of slots below it are evicted and are never claimed
	// again, also after the head slot goes back in a reorg.
	seenMu       sync.RWMutex
	evictedBelow uint64
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
	if err := decodeGossipSSZ(obj.SyncCommitteeMessage, data, version); err != nil {
		return nil, err
	}
	return obj, nil
}

// ProcessMessage processes a sync committee message
func (s *syncCommitteeMessagesService) ProcessMessage(ctx context.Context, subnet *uint64, msg *SyncCommitteeMessageForGossip) error {
	aggregateVerificationData, err := s.checkMessage(subnet, msg)
	if err != nil || aggregateVerificationData == nil {
		return err
	}
	// The verdict is awaited without the service or head-state lock, so a gossip message is accepted, and forwarded,
	// only once its signature is verified, while other messages and head-state writes go on.
	if msg.ImmediateVerification {
		err = s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
	} else {
		err = s.batchSignatureVerifier.VerifySyncCommitteeMessage(ctx, aggregateVerificationData)
	}
	if err != nil {
		return err
	}
	return s.storeVerifiedMessage(*subnet, msg.SyncCommitteeMessage)
}

// storeVerifiedMessage records a verified message as the one valid message for its validator and slot, and ignores it
// if another message for them was verified at the same time and recorded first, or if this content was already
// published.
func (s *syncCommitteeMessagesService) storeVerifiedMessage(subnet uint64, msg *cltypes.SyncCommitteeMessage) error {
	key := seenSyncCommitteeMessage{subnet: subnet, slot: msg.Slot, validatorIndex: msg.ValidatorIndex}
	recorded := false
	var ignored error
	// The freshness check and the claim happen under one read lock of seenMu, so no eviction runs between them. A
	// verification that ended after the head moved two slots past the message is outside the slot window the gossip
	// checks admitted it for, and a slot already evicted stays evicted.
	s.seenMu.RLock()
	if msg.Slot < s.evictedBelow || isStaleSyncCommitteeMessageSlot(msg.Slot, s.syncedDataManager.HeadSlot()) {
		ignored = ErrIgnore
	} else if verified, loaded := s.seenSyncCommitteeMessages.LoadOrStore(key, verifiedSyncCommitteeMessage{
		beaconBlockRoot: msg.BeaconBlockRoot,
		signature:       msg.Signature,
	}); !loaded {
		recorded = true
	} else if v := verified.(verifiedSyncCommitteeMessage); !v.matches(msg.BeaconBlockRoot, msg.Signature) || v.published {
		ignored = ErrIgnore
	}
	s.seenMu.RUnlock()
	if !recorded {
		return ignored
	}
	// One addition at a time holds a head-state view: an addition waiting for the pool inside a view would keep a
	// head-state write, and every reader queued behind it, waiting.
	s.poolMu.Lock()
	defer s.poolMu.Unlock()
	s.cleanupOldSyncCommitteeMessages()
	stale := false
	if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// The head may have moved past the message while it waited for the pool.
		if stale = isStaleSyncCommitteeMessageSlot(msg.Slot, headState.Slot()); stale {
			return nil
		}
		return s.syncContributionPool.AddSyncCommitteeMessage(headState, subnet, msg)
	}); err != nil {
		log.Debug("failed to add sync committee message to pool", "err", err)
	}
	if stale {
		return ErrIgnore
	}
	// An identical submission is answered from the seen map while this one waits for the pool, and may have been
	// published meanwhile; then there is nothing left for this one to publish.
	if verified, ok := s.seenSyncCommitteeMessages.Load(key); ok && verified.(verifiedSyncCommitteeMessage).published {
		return ErrIgnore
	}
	return nil
}

// checkMessage runs the gossip checks that need no signature and returns the signature to verify, or nil for a
// duplicate of an already verified message that has not been published yet.
func (s *syncCommitteeMessagesService) checkMessage(subnet *uint64, msg *SyncCommitteeMessageForGossip) (*AggregateVerificationData, error) {
	var aggregateVerificationData *AggregateVerificationData
	err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The message's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. sync_committee_message.slot == current_slot.
		if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot) {
			return ErrIgnore
		}
		// [REJECT] The subnet_id is valid for the given validator, i.e. subnet_id in compute_subnets_for_sync_committee(state, sync_committee_message.validator_index).
		// The committee is the one that signs for the message slot, which is the next committee at the last slot of a period.
		subnets, err := subnets.ComputeSubnetsForSyncCommittee(headState, msg.SyncCommitteeMessage.Slot, msg.SyncCommitteeMessage.ValidatorIndex)
		if errors.Is(err, state.ErrSyncCommitteeNotInState) {
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
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
			if !v.matches(msg.SyncCommitteeMessage.BeaconBlockRoot, msg.SyncCommitteeMessage.Signature) {
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
		aggregateVerificationData = &AggregateVerificationData{
			Signatures:  [][]byte{signature},
			SignRoots:   [][]byte{signingRoot},
			Pks:         [][]byte{pubKey},
			SendingPeer: msg.Receiver,
			F:           func() {},
		}
		return nil
	})
	return aggregateVerificationData, err
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
	if !v.matches(beaconBlockRoot, signature) || v.published {
		return
	}
	v.published = true
	s.seenSyncCommitteeMessages.CompareAndSwap(key, verified, v)
}

// cleanupOldSyncCommitteeMessages removes messages of slots before the previous one. Only messages of the current slot
// are admitted, so entries go stale only when the head slot advances, and one cleanup per head slot is enough. It
// raises evictedBelow before removing, so a removed slot cannot be claimed again. The caller holds poolMu.
func (s *syncCommitteeMessagesService) cleanupOldSyncCommitteeMessages() {
	headSlot := s.syncedDataManager.HeadSlot()
	if headSlot == s.cleanedAtHeadSlot {
		return
	}
	s.cleanedAtHeadSlot = headSlot
	if headSlot < 2 {
		return
	}
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	s.evictedBelow = max(s.evictedBelow, headSlot-1)
	s.seenSyncCommitteeMessages.Range(func(key, _ any) bool {
		if k := key.(seenSyncCommitteeMessage); k.slot < s.evictedBelow {
			s.seenSyncCommitteeMessages.Delete(k)
		}
		return true
	})
}

// isStaleSyncCommitteeMessageSlot reports whether messages of slot are older than the previous slot of the head.
func isStaleSyncCommitteeMessageSlot(slot, headSlot uint64) bool {
	return headSlot > slot+1
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
