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
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type SignedAggregateAndProofForGossip struct {
	SignedAggregateAndProof *cltypes.SignedAggregateAndProof
	Receiver                *sentinelproto.Peer
	ImmediateProcess        bool
	TopicForkDigest         *common.Bytes4
}

func (m *SignedAggregateAndProofForGossip) SetTopicForkDigest(digest common.Bytes4) {
	m.TopicForkDigest = &digest
}

type seenAggregateIndex struct {
	epoch uint64
	index uint64
}

const seenAggregateCacheSize = 10_000

type aggregateAndProofServiceImpl struct {
	syncedDataManager      *synced_data.SyncedDataManager
	forkchoiceStore        forkchoice.ForkChoiceStorage
	ethClock               eth_clock.EthereumClock
	beaconCfg              *clparams.BeaconChainConfig
	opPool                 pool.OperationsPool
	test                   bool
	batchSignatureVerifier *BatchSignatureVerifier
	seenAggreatorIndexes   *lru.Cache[seenAggregateIndex, common.Hash]
	targetStateBuildClaims *lru.Cache[targetStateBuildClaim, struct{}]
	validatorParams        *validator_params.ValidatorParams

	awaitingBlock *blockArrivalQueue
}

func NewAggregateAndProofService(
	ctx context.Context,
	syncedDataManager *synced_data.SyncedDataManager,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	opPool pool.OperationsPool,
	test bool,
	batchSignatureVerifier *BatchSignatureVerifier,
	validatorParams *validator_params.ValidatorParams,
) AggregateAndProofService {
	seenAggCache, err := lru.New[seenAggregateIndex, common.Hash]("seenAggregate", seenAggregateCacheSize)
	if err != nil {
		panic(err)
	}
	a := &aggregateAndProofServiceImpl{
		syncedDataManager:      syncedDataManager,
		forkchoiceStore:        forkchoiceStore,
		ethClock:               ethClock,
		beaconCfg:              beaconCfg,
		opPool:                 opPool,
		test:                   test,
		batchSignatureVerifier: batchSignatureVerifier,
		seenAggreatorIndexes:   seenAggCache,
		targetStateBuildClaims: newTargetStateBuildClaims(),
		validatorParams:        validatorParams,
		awaitingBlock: newBlockArrivalQueue(
			forkchoiceStore,
			int(beaconCfg.MaxCommitteesPerSlot*beaconCfg.TargetAggregatorsPerCommittee),
			time.Duration(beaconCfg.SecondsPerSlot)*time.Second,
		),
	}
	go a.awaitingBlock.run(ctx)
	return a
}

func (a *aggregateAndProofServiceImpl) Names() []string {
	return []string{gossip.TopicNameBeaconAggregateAndProof}
}

func (a *aggregateAndProofServiceImpl) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameBeaconAggregateAndProof
}

func (a *aggregateAndProofServiceImpl) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (*SignedAggregateAndProofForGossip, error) {
	obj := &SignedAggregateAndProofForGossip{
		Receiver:                &sentinelproto.Peer{Pid: pid.String()},
		SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{},
	}
	if err := decodeGossipSSZ(obj.SignedAggregateAndProof, data, version); err != nil {
		return nil, err
	}
	return obj, nil
}

func (a *aggregateAndProofServiceImpl) checkSeenAggregator(aggregateAndProof *SignedAggregateAndProofForGossip) (seenAggregateIndex, common.Hash, error) {
	signedAggregate := aggregateAndProof.SignedAggregateAndProof
	seenIndex := seenAggregateIndex{
		epoch: signedAggregate.Message.Aggregate.Data.Target.Epoch,
		index: signedAggregate.Message.AggregatorIndex,
	}
	var aggregateRoot common.Hash
	if aggregateAndProof.ImmediateProcess {
		root, err := signedAggregate.HashSSZ()
		if err != nil {
			return seenIndex, aggregateRoot, err
		}
		aggregateRoot = root
	}
	if seenRoot, ok := a.seenAggreatorIndexes.Peek(seenIndex); ok {
		if aggregateAndProof.ImmediateProcess && seenRoot == aggregateRoot {
			// ErrAggregatorAlreadySeen means this exact signed aggregate was validated before.
			return seenIndex, aggregateRoot, fmt.Errorf("%w: %w", ErrIgnore, ErrAggregatorAlreadySeen)
		}
		return seenIndex, aggregateRoot, fmt.Errorf("%w: %w", ErrIgnore, ErrAggregatorAlreadyKnown)
	}
	return seenIndex, aggregateRoot, nil
}

func (a *aggregateAndProofServiceImpl) verifyAggregateBeforeBuild(aggregateAndProof *SignedAggregateAndProofForGossip) error {
	if _, _, err := a.checkSeenAggregator(aggregateAndProof); err != nil {
		return err
	}
	signedAggregate := aggregateAndProof.SignedAggregateAndProof
	var signature1, signingRoot1, pubKey1, signature2, signingRoot2, pubKey2 []byte
	if err := a.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		var err error
		signature1, signingRoot1, pubKey1, err = AggregateAndProofSignature(headState, signedAggregate.Message)
		if err != nil {
			return err
		}
		signature2, signingRoot2, pubKey2, err = AggregatorSignature(headState, signedAggregate)
		return err
	}); err != nil {
		return err
	}
	valid, err := blsVerifyMultipleSignatures(
		[][]byte{signature1, signature2},
		[][]byte{signingRoot1, signingRoot2},
		[][]byte{pubKey1, pubKey2},
	)
	if err != nil {
		return err
	}
	if !valid {
		return ErrInvalidBlsSignature
	}
	return nil
}

func (a *aggregateAndProofServiceImpl) ProcessMessage(
	ctx context.Context,
	subnet *uint64,
	aggregateAndProof *SignedAggregateAndProofForGossip,
) error {
	if aggregateAndProof == nil || aggregateAndProof.SignedAggregateAndProof == nil ||
		aggregateAndProof.SignedAggregateAndProof.Message == nil ||
		aggregateAndProof.SignedAggregateAndProof.Message.Aggregate == nil ||
		aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data == nil ||
		aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.AggregationBits == nil {
		return errors.New("invalid aggregate and proof")
	}
	selectionProof := aggregateAndProof.SignedAggregateAndProof.Message.SelectionProof
	aggregateData := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data
	aggregate := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate
	target := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.Target
	slot := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	committeeIndex := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex

	// Note: the original "future slot" check used HeadSlot() which can lag behind
	// block production in solo-validator setups. The epoch check below (line ~218)
	// and the finalized ancestor check (line ~234) provide sufficient validation.

	epoch := slot / a.beaconCfg.SlotsPerEpoch
	clversion := a.beaconCfg.GetCurrentStateVersion(epoch)
	if aggregateAndProof.TopicForkDigest != nil {
		messageForkDigest, err := a.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			return fmt.Errorf("%w: compute aggregate fork digest: %w", ErrIgnore, err)
		}
		if messageForkDigest != *aggregateAndProof.TopicForkDigest {
			return fmt.Errorf("%w: aggregate fork digest does not match topic", ErrIgnore)
		}
	}
	aggregateAndProof.SignedAggregateAndProof.SetVersion(clversion)
	if err := aggregate.ValidateForConfig(a.beaconCfg, clversion); err != nil {
		return err
	}
	if clversion.AfterOrEqual(clparams.ElectraVersion) {
		if aggregate.CommitteeBits == nil {
			return errors.New("invalid aggregate and proof: missing committee bits")
		}
		// [REJECT] len(committee_indices) == 1, where committee_indices = get_committee_indices(aggregate).
		indices := aggregate.CommitteeBits.GetOnIndices()
		if len(indices) != 1 {
			return fmt.Errorf("invalid committee_bits length in aggregate and proof: %v", len(indices))
		}

		if clversion.AfterOrEqual(clparams.GloasVersion) {
			// [New in GLOAS] [REJECT] aggregate.data.index >= 2
			if aggregate.Data.CommitteeIndex >= 2 {
				return errors.New("invalid committee_index in aggregate and proof: must be < 2")
			}
		} else {
			// [Electra/Fulu] [REJECT] aggregate.data.index == 0
			if aggregate.Data.CommitteeIndex != 0 {
				return errors.New("invalid committee_index in aggregate and proof")
			}
		}
		committeeIndex = uint64(indices[0])
	}

	var (
		aggregateVerificationData *AggregateVerificationData
		aggregateRoot             common.Hash
		attestingIndices          []uint64
		seenIndex                 seenAggregateIndex
	)
	if err := a.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// If our head state is too far from the aggregate's epoch, committee
		// computations will use a stale RANDAO mix and produce wrong results.
		// Note: uses epoch (from slot), not target.Epoch, so malformed messages
		// with wrong target.Epoch still reach the reject check below.
		// [IGNORE] the epoch of aggregate.data.slot is either the current or previous epoch
		// Use the wall-clock slot and highest seen slot to widen the accepted epoch
		// window when the head state lags.
		prevEpoch, currEpoch := validationEpochRange(headState, a.forkchoiceStore.HighestSeen(), a.ethClock.GetCurrentSlot(), a.beaconCfg.SlotsPerEpoch)
		if epoch < prevEpoch || epoch > currEpoch {
			return fmt.Errorf("%w: epoch outside validation range: %d (prev=%d, curr=%d)", ErrIgnore, epoch, prevEpoch, currEpoch)
		}
		return nil
	}); err != nil {
		return err
	}

	// [REJECT] The aggregate attestation's epoch matches its target -- i.e. aggregate.data.target.epoch == compute_epoch_at_slot(aggregate.data.slot)
	if aggregateData.Target.Epoch != epoch {
		return errors.New("invalid target epoch in aggregate and proof")
	}
	// [IGNORE] The block being voted for (aggregate.data.beacon_block_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue aggregates for processing once block is retrieved).
	// Checked before the finalized ancestor, which an unknown block always fails.
	blockHeader, blockHeaderOK := a.forkchoiceStore.GetHeader(aggregateData.BeaconBlockRoot)
	if !blockHeaderOK {
		if !aggregateAndProof.ImmediateProcess {
			a.awaitingBlock.add(aggregateData.BeaconBlockRoot, func(ctx context.Context) { _ = a.ProcessMessage(ctx, nil, aggregateAndProof) })
		}
		return fmt.Errorf("%w: block not seen: %v", ErrIgnore, aggregateData.BeaconBlockRoot)
	}
	committee, committeesPerSlot, err := beaconCommittee(
		a.forkchoiceStore, a.syncedDataManager, target, aggregateData.BeaconBlockRoot, slot, committeeIndex,
		a.beaconCfg.SlotsPerEpoch, func() error { return a.verifyAggregateBeforeBuild(aggregateAndProof) },
		claimTargetStateBuild(a.targetStateBuildClaims, aggregateAndProof.SignedAggregateAndProof.Message.AggregatorIndex, target.Epoch),
	)
	if err != nil {
		return err
	}
	// [REJECT] The committee index is within the expected range -- i.e. index < get_committee_count_per_slot(state, aggregate.data.target.epoch).
	if committeeIndex >= committeesPerSlot {
		return errors.New("invalid committee index in aggregate and proof")
	}

	finalizedCheckpoint := a.forkchoiceStore.FinalizedCheckpoint()
	finalizedSlot := finalizedCheckpoint.Epoch * a.beaconCfg.SlotsPerEpoch
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by aggregate.data.beacon_block_root -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	if a.forkchoiceStore.Ancestor(
		aggregateData.BeaconBlockRoot,
		finalizedSlot,
	).Root != finalizedCheckpoint.Root {
		return fmt.Errorf("%w: invalid finalized checkpoint: %v", ErrIgnore, finalizedCheckpoint.Root)
	}

	// [New in GLOAS] [REJECT] aggregate.data.index == 0 if block.slot == aggregate.data.slot
	// This means: when same slot, index MUST be 0 (reject if not 0)
	if clversion.AfterOrEqual(clparams.GloasVersion) {
		if blockHeader.Slot == slot && aggregate.Data.CommitteeIndex != 0 {
			return errors.New("invalid committee_index in aggregate and proof: must be 0 when block.slot == aggregate.data.slot")
		}

		if aggregate.Data.CommitteeIndex == 1 {
			if !a.forkchoiceStore.IsPayloadVerified(aggregateData.BeaconBlockRoot) {
				return unverifiedGloasPayloadError(a.forkchoiceStore, aggregateData.BeaconBlockRoot)
			}
		}
	}

	// [IGNORE] The aggregate is the first valid aggregate received for the aggregator with index aggregate_and_proof.aggregator_index for the epoch aggregate.data.target.epoch
	seenIndex, aggregateRoot, err = a.checkSeenAggregator(aggregateAndProof)
	if err != nil {
		return err
	}

	// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, aggregate.data.slot, index)).
	if aggregate.AggregationBits.Bits() != len(committee) {
		return fmt.Errorf("aggregation bits length %d does not match committee size %d", aggregate.AggregationBits.Bits(), len(committee))
	}
	for i, validatorIndex := range committee {
		if aggregate.AggregationBits.GetBitAt(i) {
			attestingIndices = append(attestingIndices, validatorIndex)
		}
	}
	if len(attestingIndices) == 0 {
		return errors.New("no attesting indicies")
	}
	// [REJECT] The aggregator's validator index is within the committee -- i.e. aggregate_and_proof.aggregator_index in get_beacon_committee(state, aggregate.data.slot, index).
	if !slices.Contains(committee, aggregateAndProof.SignedAggregateAndProof.Message.AggregatorIndex) {
		return errors.New("committee index not in committee")
	}
	// [REJECT] The aggregate attestation's target block is an ancestor of the block named in the LMD vote -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, aggregate.data.target.epoch) == aggregate.data.target.root
	if a.forkchoiceStore.Ancestor(
		aggregateData.BeaconBlockRoot,
		target.Epoch*a.beaconCfg.SlotsPerEpoch,
	).Root != target.Root {
		return errors.New("invalid target block")
	}
	if a.test {
		return nil
	}

	// [REJECT] aggregate_and_proof.selection_proof selects the validator as an aggregator for the slot -- i.e. is_aggregator(state, aggregate.data.slot, index, aggregate_and_proof.selection_proof) returns True.
	if !state.IsAggregator(a.beaconCfg, uint64(len(committee)), selectionProof) {
		log.Warn("receveived aggregate and proof from invalid aggregator")
		return errors.New("invalid aggregate and proof")
	}

	monitor.ObserveNumberOfAggregateSignatures(len(attestingIndices))
	monitor.ObserveAggregateQuality(len(attestingIndices), len(committee))
	monitor.ObserveCommitteeSize(float64(len(committee)))

	// A node with validators verifies every aggregate: it relays them and packs them into its own blocks.
	if len(a.validatorParams.GetValidators()) > 0 || aggregateAndProof.ImmediateProcess {
		// Set beacon config on the aggregate's attestation so HashSSZ uses the correct
		// AggregationBits limit for the active preset (e.g. minimal: 8192, mainnet: 131072).
		aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.SetBeaconConfig(a.beaconCfg)
		// aggregate signatures for later verification
		if err := a.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
			var err error
			aggregateVerificationData, err = GetSignaturesOnAggregate(headState, aggregateAndProof.SignedAggregateAndProof, attestingIndices)
			return err
		}); err != nil {
			return err
		}
	}
	if aggregateVerificationData == nil {
		return ErrIgnore
	}
	aggregateVerificationData.F = func() {
		a.opPool.AttestationsPool.Insert(
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Signature,
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate,
		)
		a.forkchoiceStore.ProcessAttestingIndicies(
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate,
			attestingIndices,
		)
		// Keep the root of the first aggregate validated for this aggregator and epoch.
		a.seenAggreatorIndexes.ContainsOrAdd(seenIndex, aggregateRoot)
	}
	// for this specific request, collect data for potential peer banning or gossip publishing
	aggregateVerificationData.SendingPeer = aggregateAndProof.Receiver

	if aggregateAndProof.ImmediateProcess {
		return a.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
	}

	return a.batchSignatureVerifier.VerifyAggregateProof(ctx, aggregateVerificationData)
}

func GetSignaturesOnAggregate(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
	attestingIndicies []uint64,
) (*AggregateVerificationData, error) {
	// [REJECT] The aggregate_and_proof.selection_proof is a valid signature of the aggregate.data.slot by the validator with index aggregate_and_proof.aggregator_index.
	signature1, signatureRoot1, pubKey1, err := AggregateAndProofSignature(s, aggregateAndProof.Message)
	if err != nil {
		return nil, err
	}

	// [REJECT] The aggregator signature, signed_aggregate_and_proof.signature, is valid.
	signature2, signatureRoot2, pubKey2, err := AggregatorSignature(s, aggregateAndProof)
	if err != nil {
		return nil, err
	}

	// [REJECT] Verifying here the validator signatures for the aggregate
	signature3, signatureRoot3, pubKey3, err := AggregateMessageSignature(s, aggregateAndProof, attestingIndicies)
	if err != nil {
		return nil, err
	}

	return &AggregateVerificationData{
		Signatures: [][]byte{signature1, signature2, signature3},
		SignRoots:  [][]byte{signatureRoot1, signatureRoot2, signatureRoot3},
		Pks:        [][]byte{pubKey1, pubKey2, pubKey3},
	}, nil
}

func AggregateAndProofSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.AggregateAndProof,
) ([]byte, []byte, []byte, error) {
	slot := aggregate.Aggregate.Data.Slot
	publicKey, err := state.ValidatorPublicKey(int(aggregate.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	cfg := state.BeaconConfig()
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainSelectionProof, slot/cfg.SlotsPerEpoch, state.GenesisValidatorsRoot())
	if err != nil {
		return nil, nil, nil, err
	}
	slotRoot := merkle_tree.Uint64Root(slot)
	signingRoot := crypto.Sha256(slotRoot[:], domain)
	return aggregate.SelectionProof[:], signingRoot[:], publicKey[:], nil
}

func AggregatorSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.SignedAggregateAndProof,
) ([]byte, []byte, []byte, error) {
	publicKey, err := state.ValidatorPublicKey(int(aggregate.Message.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	cfg := state.BeaconConfig()
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainAggregateAndProof, aggregate.Message.Aggregate.Data.Slot/cfg.SlotsPerEpoch, state.GenesisValidatorsRoot())
	if err != nil {
		return nil, nil, nil, err
	}
	signingRoot, err := fork.ComputeSigningRoot(aggregate.Message, domain)
	if err != nil {
		return nil, nil, nil, err
	}
	return aggregate.Signature[:], signingRoot[:], publicKey[:], nil
}

func AggregateMessageSignature(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
	attestingIndicies []uint64,
) ([]byte, []byte, []byte, error) {
	indexedAttestation := state.GetIndexedAttestation(
		aggregateAndProof.Message.Aggregate,
		attestingIndicies,
	)

	inds := indexedAttestation.AttestingIndices
	if inds.Length() == 0 {
		return nil, nil, nil, errors.New("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := make([][]byte, 0, inds.Length())
	if err := solid.RangeErr[uint64](inds, func(_ int, v uint64, _ int) error {
		val, err := s.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return err
		}
		pk := val.PublicKeyBytes()
		pks = append(pks, bytes.Clone(pk))
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	domain, err := fork.ComputeDomainAtEpoch(s.BeaconConfig(), s.BeaconConfig().DomainBeaconAttester, indexedAttestation.Data.Target.Epoch, s.GenesisValidatorsRoot())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get the domain: %w", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(indexedAttestation.Data, domain)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get signing root: %w", err)
	}

	pubKeys, err := bls.AggregatePublickKeys(pks)
	if err != nil {
		return nil, nil, nil, err
	}

	return indexedAttestation.Signature[:], signingRoot[:], pubKeys, nil
}
