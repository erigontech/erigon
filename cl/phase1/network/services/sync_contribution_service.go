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
	"fmt"
	"slices"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
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
	seenSyncCommitteeContributions *lru.Cache[seenSyncCommitteeContribution, struct{}]
	emitters                       *beaconevents.EventEmitter
	ethClock                       eth_clock.EthereumClock
	batchSignatureVerifier         *BatchSignatureVerifier
	validatorParams                *validator_params.ValidatorParams
	proposerIndicesCache           *lru.Cache[uint64, []uint64]
	test                           bool

	mu sync.Mutex
}

// SignedContributionAndProofWithGossipData type represents SignedContributionAndProof with the gossip data where it's coming from.
type SignedContributionAndProofForGossip struct {
	SignedContributionAndProof *cltypes.SignedContributionAndProof
	Receiver                   *sentinelproto.Peer
	ImmediateVerification      bool
}

// NewSyncContributionService creates a new sync contribution service
// seenContributionSlots sizes the seen set in slots of SyncCommitteeSize keys each: a key stays
// admitted while the clock admits its slot and the two adjacent ones, plus one slot of margin.
const seenContributionSlots = 4

func NewSyncContributionService(
	syncedDataManager *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	syncContributionPool sync_contribution_pool.SyncContributionPool,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.EventEmitter,
	batchSignatureVerifier *BatchSignatureVerifier,
	validatorParams *validator_params.ValidatorParams,
	test bool,
) SyncContributionService {
	proposerIndicesCache, err := lru.New[uint64, []uint64]("proposerIndices", 3)
	if err != nil {
		panic(err)
	}
	seenSyncCommitteeContributions, err := lru.New[seenSyncCommitteeContribution, struct{}]("seenSyncCommitteeContributions", int(beaconCfg.SyncCommitteeSize)*seenContributionSlots)
	if err != nil {
		panic(err)
	}
	return &syncContributionService{
		syncedDataManager:              syncedDataManager,
		beaconCfg:                      beaconCfg,
		syncContributionPool:           syncContributionPool,
		seenSyncCommitteeContributions: seenSyncCommitteeContributions,
		ethClock:                       ethClock,
		emitters:                       emitters,
		batchSignatureVerifier:         batchSignatureVerifier,
		validatorParams:                validatorParams,
		proposerIndicesCache:           proposerIndicesCache,
		test:                           test,
	}
}

func (s *syncContributionService) Names() []string {
	return []string{gossip.TopicNameSyncCommitteeContributionAndProof}
}

func (s *syncContributionService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameSyncCommitteeContributionAndProof
}

// isLocalValidatorProposer checks if any local validator is a proposer in the current or next epoch.
// For current epoch: uses cached GetBeaconProposerIndices.
// For next epoch: uses GetProposerLookahead() (Fulu+) or always returns true (pre-Fulu).
func (s *syncContributionService) isLocalValidatorProposer(headState *state.CachingBeaconState, currentEpoch uint64, localValidators []uint64) bool {
	if headState.Version() < clparams.FuluVersion {
		return true
	}
	// Check current epoch using cached proposer indices
	currentProposers, ok := s.proposerIndicesCache.Get(currentEpoch)
	if !ok {
		var err error
		currentProposers, err = headState.GetBeaconProposerIndices(currentEpoch)
		if err == nil {
			s.proposerIndicesCache.Add(currentEpoch, currentProposers)
		}
	}
	for _, validatorIndex := range localValidators {
		if slices.Contains(currentProposers, validatorIndex) {
			return true
		}
	}

	// For Fulu+, use the efficient proposer lookahead for next epoch
	lookahead := headState.GetProposerLookahead()
	// The lookahead contains proposers for current and next epoch, skip current epoch slots
	slotsPerEpoch := int(s.beaconCfg.SlotsPerEpoch)
	for i := slotsPerEpoch; i < lookahead.Length(); i++ {
		proposerIndex := lookahead.Get(i)
		if slices.Contains(localValidators, proposerIndex) {
			return true
		}
	}
	return false
}

func (s *syncContributionService) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (*SignedContributionAndProofForGossip, error) {
	// Pre-allocate with config-aware aggregation bits size so DecodeSSZ uses the
	// correct byte length for the current preset (minimal: 1 byte, mainnet: 16 bytes).
	aggBitsSize := int(s.beaconCfg.SyncCommitteeSize / s.beaconCfg.SyncCommitteeSubnetCount / 8)
	contrib := &cltypes.Contribution{}
	contrib.SetAggregationBitsSize(aggBitsSize)
	obj := &SignedContributionAndProofForGossip{
		Receiver: &sentinelproto.Peer{Pid: pid.String()},
		SignedContributionAndProof: &cltypes.SignedContributionAndProof{
			Message: &cltypes.ContributionAndProof{
				Contribution: contrib,
			},
		},
	}
	if err := obj.SignedContributionAndProof.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
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
		if contributionAndProof.Contribution.SubcommitteeIndex >= s.beaconCfg.SyncCommitteeSubnetCount {
			return errors.New("subcommittee index is out of range")
		}

		// [REJECT] The selection proof selects the validator as an aggregator for the slot. It does not
		// depend on the state, so it runs before the committee lookup.
		modulo := max(1, s.beaconCfg.SyncCommitteeSize/s.beaconCfg.SyncCommitteeSubnetCount/s.beaconCfg.TargetAggregatorsPerSyncSubcommittee)
		hashSignature := crypto.Sha256(selectionProof[:])
		if !s.test && binary.LittleEndian.Uint64(hashSignature[:8])%modulo != 0 {
			return errors.New("selects the validator as an aggregator")
		}

		aggregatorPubKey, err := headState.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
		if err != nil {
			return err
		}
		subcommiteePubsKeys, err := s.getSyncSubcommitteePubkeys(headState, contributionAndProof.Contribution.Slot, contributionAndProof.Contribution.SubcommitteeIndex)
		if errors.Is(err, state.ErrSyncCommitteeNotInState) {
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		if err != nil {
			return err
		}

		// [REJECT] The aggregator's validator index is in the declared subcommittee of the sync committee that signs for the contribution slot -- i.e. state.validators[contribution_and_proof.aggregator_index].pubkey in get_sync_subcommittee_pubkeys(state, contribution.subcommittee_index).
		if !slices.Contains(subcommiteePubsKeys, aggregatorPubKey) {
			return errors.New("aggregator's validator index is not in subcommittee")
		}

		// [IGNORE] The sync committee contribution is the first valid contribution received for the aggregator with index contribution_and_proof.aggregator_index for the slot contribution.slot and subcommittee index contribution.subcommittee_index (this requires maintaining a cache of size SYNC_COMMITTEE_SIZE for this topic that can be flushed after each slot).
		if s.wasContributionSeen(contributionAndProof) {
			return nil
		}

		// Check if any local validator is a proposer in this or the next epoch
		var localValidatorIsProposer bool
		localValidators := s.validatorParams.GetValidators()
		if len(localValidators) > 0 {
			currentEpoch := state.Epoch(headState)
			localValidatorIsProposer = s.isLocalValidatorProposer(headState, currentEpoch, localValidators)
		}

		if !localValidatorIsProposer && !signedContribution.ImmediateVerification {
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
	subcommiteePubsKeys []common.Bytes48,
) (*AggregateVerificationData, error) {
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

// getSyncSubcommitteePubkeys returns the public keys of the given subcommittee of the committee that signs for slot
// (get_sync_subcommittee_pubkeys, with the committee chosen by state.SyncCommitteeForMessageSlot).
func (s *syncContributionService) getSyncSubcommitteePubkeys(st *state.CachingBeaconState, slot, subcommitteeIndex uint64) ([]common.Bytes48, error) {
	syncCommittee, err := state.SyncCommitteeForMessageSlot(st, slot)
	if err != nil {
		return nil, err
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

	return s.seenSyncCommitteeContributions.Contains(key)
}

// markContributionAsSeen marks the contribution as seen. A late verification callback for a
// slot the clock no longer admits is skipped, so it cannot evict a key that is still live.
func (s *syncContributionService) markContributionAsSeen(contribution *cltypes.ContributionAndProof) {
	if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(contribution.Contribution.Slot) {
		return
	}
	key := seenSyncCommitteeContribution{
		aggregatorIndex:   contribution.AggregatorIndex,
		slot:              contribution.Contribution.Slot,
		subCommitteeIndex: contribution.Contribution.SubcommitteeIndex,
	}
	s.seenSyncCommitteeContributions.Add(key, struct{}{})
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

	cfg := st.BeaconConfig()
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainSyncCommitteeSelectionProof, state.GetEpochAtSlot(cfg, contributionAndProof.Contribution.Slot), st.GenesisValidatorsRoot())
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
	cfg := s.BeaconConfig()
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainSyncCommittee, state.GetEpochAtSlot(cfg, contribution.Slot), s.GenesisValidatorsRoot())
	if err != nil {
		return nil, nil, nil, err
	}

	msg := crypto.Sha256(contribution.BeaconBlockRoot[:], domain)
	// only use the ones pertaining to the aggregation bits
	subCommitteePubsKeys := make([][]byte, 0, len(subCommitteeKeys))
	for i, key := range subCommitteeKeys {
		if utils.IsBitOn(contribution.AggregationBits, i) {
			subCommitteePubsKeys = append(subCommitteePubsKeys, bytes.Clone(key[:]))
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
	cfg := s.BeaconConfig()
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainContributionAndProof, state.GetEpochAtSlot(cfg, contribution.Slot), s.GenesisValidatorsRoot())
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
