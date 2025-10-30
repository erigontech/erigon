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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon-lib/common"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
)

type seenSyncCommitteeContribution struct {
	aggregatorIndex   uint64
	slot              uint64
	subCommitteeIndex uint64
}

type syncContributionService struct {
	syncedDataManager              *synced_data.SyncedDataManager
	beaconCfg                      *clparams.BeaconChainConfig
	syncContributionPool           sync_contribution_pool.SyncContributionPool
	seenSyncCommitteeContributions sync.Map //  map[seenSyncCommitteeContribution]struct{}
	emitters                       *beaconevents.EventEmitter
	ethClock                       eth_clock.EthereumClock
	batchSignatureVerifier         *BatchSignatureVerifier
	test                           bool

	mu sync.Mutex
}

// SignedContributionAndProofWithGossipData type represents SignedContributionAndProof with the gossip data where it's coming from.
type SignedContributionAndProofForGossip struct {
	SignedContributionAndProof *cltypes.SignedContributionAndProof
	Receiver                   *sentinel.Peer
	ImmediateVerification      bool
}

// NewSyncContributionService creates a new sync contribution service
func NewSyncContributionService(
	syncedDataManager *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	syncContributionPool sync_contribution_pool.SyncContributionPool,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.EventEmitter,
	batchSignatureVerifier *BatchSignatureVerifier,
	test bool,
) SyncContributionService {
	return &syncContributionService{
		syncedDataManager:              syncedDataManager,
		beaconCfg:                      beaconCfg,
		syncContributionPool:           syncContributionPool,
		seenSyncCommitteeContributions: sync.Map{},
		ethClock:                       ethClock,
		emitters:                       emitters,
		batchSignatureVerifier:         batchSignatureVerifier,
		test:                           test,
	}
}

// ProcessMessage processes a sync contribution message
func (s *syncContributionService) ProcessMessage(ctx context.Context, subnet *uint64, signedContribution *SignedContributionAndProofForGossip) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	contributionAndProof := signedContribution.SignedContributionAndProof.Message
	selectionProof := contributionAndProof.SelectionProof
	aggregationBits := contributionAndProof.Contribution.AggregationBits

	return s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {

		// [IGNORE] The contribution's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. contribution.slot == current_slot.
		if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(contributionAndProof.Contribution.Slot) {
			return ErrIgnore
		}

		// [REJECT] The contribution has participants -- that is, any(contribution.aggregation_bits).
		if bytes.Equal(aggregationBits, make([]byte, len(aggregationBits))) { // check if the aggregation bits are all zeros
			return errors.New("contribution has no participants")
		}

		// [REJECT] The subcommittee index is in the allowed range, i.e. contribution.subcommittee_index < SYNC_COMMITTEE_SUBNET_COUNT.
		if contributionAndProof.Contribution.SubcommitteeIndex >= clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount {
			return errors.New("subcommittee index is out of range")
		}

		aggregatorPubKey, err := headState.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
		if err != nil {
			return err
		}
		subcommiteePubsKeys, err := s.getSyncSubcommitteePubkeys(headState, contributionAndProof.Contribution.SubcommitteeIndex)
		if err != nil {
			return err
		}

		modulo := max(1, s.beaconCfg.SyncCommitteeSize/s.beaconCfg.SyncCommitteeSubnetCount/s.beaconCfg.TargetAggregatorsPerSyncSubcommittee)
		hashSignature := utils.Sha256(selectionProof[:])
		if !s.test && binary.LittleEndian.Uint64(hashSignature[:8])%modulo != 0 {
			return errors.New("selects the validator as an aggregator")
		}

		// [REJECT] The aggregator's validator index is in the declared subcommittee of the current sync committee -- i.e. state.validators[contribution_and_proof.aggregator_index].pubkey in get_sync_subcommittee_pubkeys(state, contribution.subcommittee_index).
		if !slices.Contains(subcommiteePubsKeys, aggregatorPubKey) {
			return errors.New("aggregator's validator index is not in subcommittee")
		}

		// [IGNORE] The sync committee contribution is the first valid contribution received for the aggregator with index contribution_and_proof.aggregator_index for the slot contribution.slot and subcommittee index contribution.subcommittee_index (this requires maintaining a cache of size SYNC_COMMITTEE_SIZE for this topic that can be flushed after each slot).
		if s.wasContributionSeen(contributionAndProof) {
			return ErrIgnore
		}

		// aggregate signatures for later verification
		aggregateVerificationData, err := s.GetSignaturesOnContributionSignatures(headState, contributionAndProof, signedContribution, subcommiteePubsKeys)
		if err != nil {
			return err
		}

		aggregateVerificationData.SendingPeer = signedContribution.Receiver

		// further processing will be done after async signature verification
		aggregateVerificationData.F = func() {

			// mark the valid contribution as seen
			s.markContributionAsSeen(contributionAndProof)

			// emit contribution_and_proof

			s.emitters.Operation().SendContributionProof(signedContribution.SignedContributionAndProof)
			// add the contribution to the pool
			err = s.syncContributionPool.AddSyncContribution(headState, contributionAndProof.Contribution)
			if errors.Is(err, sync_contribution_pool.ErrIsSuperset) {
				return
			}
		}

		if signedContribution.ImmediateVerification {
			return s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
		}

		// push the signatures to verify asynchronously and run final functions after that.
		s.batchSignatureVerifier.AsyncVerifySyncContribution(aggregateVerificationData)

		// As the logic goes, if we return ErrIgnore there will be no peer banning and further publishing
		// gossip data into the network by the gossip manager. That's what we want because we will be doing that ourselves
		// in BatchVerification function. After validating signatures, if they are valid we will publish the
		// gossip ourselves or ban the peer which sent that particular invalid signature.
		return nil
	})
}

func (s *syncContributionService) GetSignaturesOnContributionSignatures(
	headState *state.CachingBeaconState,
	contributionAndProof *cltypes.ContributionAndProof,
	signedContribution *SignedContributionAndProofForGossip,
	subcommiteePubsKeys []common.Bytes48) (*AggregateVerificationData, error) {

	// [REJECT] The contribution_and_proof.selection_proof is a valid signature of the SyncAggregatorSelectionData derived from the contribution by the validator with index contribution_and_proof.aggregator_index.
	signature1, signatureRoot1, pubKey1, err := verifySyncContributionSelectionProof(headState, contributionAndProof)
	if !s.test && err != nil {
		return nil, err
	}

	// [REJECT] The aggregator signature, signed_contribution_and_proof.signature, is valid.
	signature2, signatureRoot2, pubKey2, err := verifyAggregatorSignatureForSyncContribution(headState, signedContribution.SignedContributionAndProof)
	if !s.test && err != nil {
		return nil, err
	}
	// [REJECT] The aggregate signature is valid for the message beacon_block_root and aggregate pubkey derived
	// from the participation info in aggregation_bits for the subcommittee specified by the contribution.subcommittee_index.
	signature3, signatureRoot3, pubKey3, err := verifySyncContributionProofAggregatedSignature(headState, contributionAndProof.Contribution, subcommiteePubsKeys)
	if !s.test && err != nil {
		return nil, err
	}

	return &AggregateVerificationData{
		Signatures: [][]byte{signature1, signature2, signature3},
		SignRoots:  [][]byte{signatureRoot1, signatureRoot2, signatureRoot3},
		Pks:        [][]byte{pubKey1, pubKey2, pubKey3},
	}, nil
}

// def get_sync_subcommittee_pubkeys(state: BeaconState, subcommittee_index: uint64) -> Sequence[BLSPubkey]:
//
//	# Committees assigned to `slot` sign for `slot - 1`
//	# This creates the exceptional logic below when transitioning between sync committee periods
//	next_slot_epoch = compute_epoch_at_slot(Slot(state.slot + 1))
//	if compute_sync_committee_period(get_current_epoch(state)) == compute_sync_committee_period(next_slot_epoch):
//	    sync_committee = state.current_sync_committee
//	else:
//	    sync_committee = state.next_sync_committee
//	# Return pubkeys for the subcommittee index
//	sync_subcommittee_size = SYNC_COMMITTEE_SIZE // SYNC_COMMITTEE_SUBNET_COUNT
//	i = subcommittee_index * sync_subcommittee_size
//	return sync_committee.pubkeys[i:i + sync_subcommittee_size]

// getSyncSubcommitteePubkeys returns the public keys of the validators in the given subcommittee.
func (s *syncContributionService) getSyncSubcommitteePubkeys(st *state.CachingBeaconState, subcommitteeIndex uint64) ([]common.Bytes48, error) {
	var syncCommittee *solid.SyncCommittee
	if s.beaconCfg.SyncCommitteePeriod(st.Slot()) == s.beaconCfg.SyncCommitteePeriod(st.Slot()+1) {
		syncCommittee = st.CurrentSyncCommittee()
	} else {
		syncCommittee = st.NextSyncCommittee()
	}
	syncSubcommitteeSize := s.beaconCfg.SyncCommitteeSize / s.beaconCfg.SyncCommitteeSubnetCount
	i := subcommitteeIndex * syncSubcommitteeSize
	return syncCommittee.GetCommittee()[i : i+syncSubcommitteeSize], nil
}

// wasContributionSeen checks if the contribution was seen before.
func (s *syncContributionService) wasContributionSeen(contribution *cltypes.ContributionAndProof) bool {
	key := seenSyncCommitteeContribution{
		aggregatorIndex:   contribution.AggregatorIndex,
		slot:              contribution.Contribution.Slot,
		subCommitteeIndex: contribution.Contribution.SubcommitteeIndex,
	}

	_, ok := s.seenSyncCommitteeContributions.Load(key)
	return ok
}

// markContributionAsSeen marks the contribution as seen.
func (s *syncContributionService) markContributionAsSeen(contribution *cltypes.ContributionAndProof) {
	key := seenSyncCommitteeContribution{
		aggregatorIndex:   contribution.AggregatorIndex,
		slot:              contribution.Contribution.Slot,
		subCommitteeIndex: contribution.Contribution.SubcommitteeIndex,
	}
	s.seenSyncCommitteeContributions.Store(key, struct{}{})
}

// verifySyncContributionProof verifies the sync contribution proof.
func verifySyncContributionSelectionProof(st *state.CachingBeaconState, contributionAndProof *cltypes.ContributionAndProof) ([]byte, []byte, []byte, error) {
	syncAggregatorSelectionData := &cltypes.SyncAggregatorSelectionData{
		Slot:              contributionAndProof.Contribution.Slot,
		SubcommitteeIndex: contributionAndProof.Contribution.SubcommitteeIndex,
	}
	selectionProof := contributionAndProof.SelectionProof

	aggregatorPubKey, err := st.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}

	domain, err := st.GetDomain(st.BeaconConfig().DomainSyncCommitteeSelectionProof, state.GetEpochAtSlot(st.BeaconConfig(), contributionAndProof.Contribution.Slot))
	if err != nil {
		return nil, nil, nil, err
	}

	selectionDataRoot, err := fork.ComputeSigningRoot(syncAggregatorSelectionData, domain)
	if err != nil {
		return nil, nil, nil, err
	}

	return selectionProof[:], selectionDataRoot[:], aggregatorPubKey[:], nil
}

// verifySyncContributionProof verifies the contribution aggregated signature.
func verifySyncContributionProofAggregatedSignature(s *state.CachingBeaconState, contribution *cltypes.Contribution, subCommitteeKeys []common.Bytes48) ([]byte, []byte, []byte, error) {
	domain, err := s.GetDomain(s.BeaconConfig().DomainSyncCommittee, state.Epoch(s))
	if err != nil {
		return nil, nil, nil, err
	}

	msg := utils.Sha256(contribution.BeaconBlockRoot[:], domain)
	// only use the ones pertaining to the aggregation bits
	subCommitteePubsKeys := make([][]byte, 0, len(subCommitteeKeys))
	for i, key := range subCommitteeKeys {
		if utils.IsBitOn(contribution.AggregationBits, i) {
			subCommitteePubsKeys = append(subCommitteePubsKeys, common.CopyBytes(key[:]))
		}
	}

	pubKeys, err := bls.AggregatePublickKeys(subCommitteePubsKeys)
	if err != nil {
		return nil, nil, nil, err
	}

	return contribution.Signature[:], msg[:], pubKeys, nil
}

func verifyAggregatorSignatureForSyncContribution(s *state.CachingBeaconState, signedContributionAndProof *cltypes.SignedContributionAndProof) ([]byte, []byte, []byte, error) {
	contribution := signedContributionAndProof.Message.Contribution
	domain, err := s.GetDomain(s.BeaconConfig().DomainContributionAndProof, contribution.Slot/s.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return nil, nil, nil, err
	}

	signingRoot, err := fork.ComputeSigningRoot(signedContributionAndProof.Message, domain)
	if err != nil {
		return nil, nil, nil, err
	}
	aggregatorPubKey, err := s.ValidatorPublicKey(int(signedContributionAndProof.Message.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	return signedContributionAndProof.Signature[:], signingRoot[:], aggregatorPubKey[:], nil
}
