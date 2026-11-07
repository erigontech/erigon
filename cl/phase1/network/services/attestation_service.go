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
	"time"

	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	computeSubnetForAttestation  = subnets.ComputeSubnetForAttestation
	computeCommitteeCountPerSlot = subnets.ComputeCommitteeCountPerSlot
)

func validationEpochRange(headState *state.CachingBeaconState, highestSeenSlot, currentSlot, slotsPerEpoch uint64) (uint64, uint64) {
	headEpoch := state.Epoch(headState)
	currEpoch := headEpoch
	if currentSlot < highestSeenSlot {
		currentSlot = highestSeenSlot
	}
	if currentSlot/slotsPerEpoch > headEpoch {
		currEpoch = headEpoch + 1
	}
	// Shuffling for headEpoch+2 uses the head epoch's final RANDAO mix, provided no later block has been seen.
	if currentSlot/slotsPerEpoch > headEpoch+1 && highestSeenSlot <= headState.Slot() {
		currEpoch = headEpoch + 2
	}
	return state.PreviousEpoch(headState), currEpoch
}

func beaconCommittee(
	fc forkchoice.ForkChoiceStorageReader,
	syncedData synced_data.SyncedData,
	target solid.Checkpoint,
	blockRoot common.Hash,
	slot,
	committeeIndex,
	slotsPerEpoch uint64,
	verifyBeforeBuild func() error,
	claimBuild func() bool,
) ([]uint64, uint64, error) {
	var (
		committee         []uint64
		committeesPerSlot uint64
		useTargetState    bool
	)
	err := syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, headRoot common.Hash, _ uint64) error {
		if !fc.HeadStateHasTargetShuffling(headState, headRoot, target) {
			useTargetState = true
			return nil
		}
		committeesPerSlot = computeCommitteeCountPerSlot(headState, slot, slotsPerEpoch)
		if committeeIndex >= committeesPerSlot {
			return nil
		}
		var err error
		committee, err = headState.GetBeaconCommitee(slot, committeeIndex)
		committee = slices.Clone(committee)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	if !useTargetState {
		return committee, committeesPerSlot, nil
	}
	target.Root = fc.Ancestor(blockRoot, target.Epoch*slotsPerEpoch).Root
	if target.Root == (common.Hash{}) {
		return nil, 0, fmt.Errorf("%w: target block ancestor is unknown", ErrIgnore)
	}
	if verifyBeforeBuild != nil {
		if err := verifyBeforeBuild(); err != nil {
			return nil, 0, err
		}
	}
	committee, committeesPerSlot, err = fc.TargetBeaconCommittee(target, slot, committeeIndex, claimBuild)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: target beacon committee: %w", ErrIgnore, err)
	}
	return committee, committeesPerSlot, nil
}

// Each entry is added by an uncached target state build, so a signer's claim is only evicted after that many other builds.
const targetStateBuildClaimCacheSize = 16_384

type targetStateBuildClaim struct {
	epoch          uint64
	validatorIndex uint64
}

func newTargetStateBuildClaims() *lru.Cache[targetStateBuildClaim, struct{}] {
	claims, err := lru.New[targetStateBuildClaim, struct{}]("target_state_build_claims", targetStateBuildClaimCacheSize)
	if err != nil {
		panic(err)
	}
	return claims
}

// claimTargetStateBuild lets a signer start one uncached target state build per target epoch.
func claimTargetStateBuild(claims *lru.Cache[targetStateBuildClaim, struct{}], validatorIndex, targetEpoch uint64) func() bool {
	return func() bool {
		claimed, _ := claims.ContainsOrAdd(targetStateBuildClaim{epoch: targetEpoch, validatorIndex: validatorIndex}, struct{}{})
		return !claimed
	}
}

func attestationSignatureData(
	syncedData synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	data *solid.AttestationData,
	validatorIndex,
	targetEpoch uint64,
) (common.Bytes48, [32]byte, error) {
	var (
		domain []byte
		pubKey common.Bytes48
	)
	if err := syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// Committee members are active, so their indices were created before the finalized checkpoint and the head state has the same keys.
		var err error
		pubKey, err = headState.ValidatorPublicKey(int(validatorIndex))
		if err != nil {
			return fmt.Errorf("unable to get public key: %w", err)
		}
		domain, err = fork.ComputeDomainAtEpoch(beaconCfg, beaconCfg.DomainBeaconAttester, targetEpoch, headState.GenesisValidatorsRoot())
		if err != nil {
			return fmt.Errorf("unable to get the domain: %w", err)
		}
		return nil
	}); err != nil {
		return common.Bytes48{}, [32]byte{}, err
	}
	signingRoot, err := computeSigningRoot(data, domain)
	if err != nil {
		return common.Bytes48{}, [32]byte{}, fmt.Errorf("unable to get signing root: %w", err)
	}
	return pubKey, signingRoot, nil
}

type attestationService struct {
	ctx                    context.Context
	forkchoiceStore        forkchoice.ForkChoiceStorage
	aggregationPool        aggregation.AggregationPool
	ethClock               eth_clock.EthereumClock
	syncedDataManager      synced_data.SyncedData
	beaconCfg              *clparams.BeaconChainConfig
	netCfg                 *clparams.NetworkConfig
	emitters               *beaconevents.EventEmitter
	batchSignatureVerifier *BatchSignatureVerifier
	// validatorAttestationSeen maps from epoch to validator index. This is used to ignore duplicate validator attestations in the same epoch.
	validatorAttestationSeen *lru.CacheWithTTL[uint64, uint64] // validator index -> epoch
	validatorClaimMu         sync.Mutex
	targetStateBuildClaims   *lru.Cache[targetStateBuildClaim, struct{}]
	// attestationProcessed           *lru.CacheWithTTL[[32]byte, struct{}]
	awaitingBlock *blockArrivalQueue
}

// AttestationWithGossipData type represents attestation with the gossip data where it's coming from.
type AttestationForGossip struct {
	Attestation       *solid.Attestation
	SingleAttestation *solid.SingleAttestation // New container after Electra
	Receiver          *sentinelproto.Peer
	// ImmediateProcess indicates whether the attestation should be processed immediately or able to be scheduled for later processing.
	ImmediateProcess bool
	TopicForkDigest  *common.Bytes4
}

func (a *AttestationForGossip) SetTopicForkDigest(digest common.Bytes4) {
	a.TopicForkDigest = &digest
}

func NewAttestationService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	aggregationPool aggregation.AggregationPool,
	ethClock eth_clock.EthereumClock,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	netCfg *clparams.NetworkConfig,
	emitters *beaconevents.EventEmitter,
	batchSignatureVerifier *BatchSignatureVerifier,
) AttestationService {
	epochDuration := time.Duration(beaconCfg.SlotsPerEpoch*beaconCfg.SecondsPerSlot) * time.Second
	a := &attestationService{
		ctx:                      ctx,
		forkchoiceStore:          forkchoiceStore,
		aggregationPool:          aggregationPool,
		ethClock:                 ethClock,
		syncedDataManager:        syncedDataManager,
		beaconCfg:                beaconCfg,
		netCfg:                   netCfg,
		emitters:                 emitters,
		batchSignatureVerifier:   batchSignatureVerifier,
		validatorAttestationSeen: lru.NewWithTTL[uint64, uint64]("validator_attestation_seen", validatorAttestationCacheSize, epochDuration),
		targetStateBuildClaims:   newTargetStateBuildClaims(),
		// attestationProcessed:     lru.NewWithTTL[[32]byte, struct{}]("attestation_processed", validatorAttestationCacheSize, epochDuration),
		awaitingBlock: newBlockArrivalQueue(forkchoiceStore, maxAttestationsAwaitingBlock, time.Duration(beaconCfg.SecondsPerSlot)*time.Second),
	}
	go a.awaitingBlock.run(ctx)
	return a
}

func (s *attestationService) Names() []string {
	names := make([]string, 0, s.netCfg.AttestationSubnetCount)
	for i := 0; i < int(s.netCfg.AttestationSubnetCount); i++ {
		names = append(names, gossip.TopicNameBeaconAttestation(uint64(i)))
	}
	return names
}

func (s *attestationService) IsMyGossipMessage(name string) bool {
	return gossip.IsTopicBeaconAttestation(name)
}

func (s *attestationService) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (*AttestationForGossip, error) {
	obj := &AttestationForGossip{
		Receiver:         &sentinelproto.Peer{Pid: pid.String()},
		ImmediateProcess: false,
	}
	obj.SingleAttestation = &solid.SingleAttestation{}
	if err := obj.SingleAttestation.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *attestationService) ProcessMessage(ctx context.Context, subnet *uint64, att *AttestationForGossip) error {
	var (
		root           common.Hash
		slot           uint64
		committeeIndex uint64
		targetEpoch    uint64
		signature      [96]byte
		data           *solid.AttestationData
	)
	if att.Attestation != nil {
		slot = att.Attestation.Data.Slot
	} else {
		slot = att.SingleAttestation.Data.Slot
	}
	attEpoch := s.ethClock.GetEpochAtSlot(slot)
	clVersion := s.beaconCfg.GetCurrentStateVersion(attEpoch)
	if att.TopicForkDigest != nil {
		messageForkDigest, err := s.ethClock.ComputeForkDigest(attEpoch)
		if err != nil {
			return fmt.Errorf("%w: compute attestation fork digest: %w", ErrIgnore, err)
		}
		if messageForkDigest != *att.TopicForkDigest {
			return fmt.Errorf("%w: attestation fork digest does not match topic", ErrIgnore)
		}
	}

	var err error
	if clVersion >= clparams.ElectraVersion {
		if att.SingleAttestation == nil {
			return errors.New("single attestation is empty")
		}
		root = att.SingleAttestation.Data.BeaconBlockRoot
		slot = att.SingleAttestation.Data.Slot
		committeeIndex = att.SingleAttestation.CommitteeIndex
		targetEpoch = att.SingleAttestation.Data.Target.Epoch
		signature = att.SingleAttestation.Signature
		data = att.SingleAttestation.Data
	} else {
		// deneb and before case
		if att.Attestation == nil {
			return errors.New("attestation is empty")
		}
		root = att.Attestation.Data.BeaconBlockRoot
		slot = att.Attestation.Data.Slot
		committeeIndex = att.Attestation.Data.CommitteeIndex
		targetEpoch = att.Attestation.Data.Target.Epoch
		signature = att.Attestation.Signature
		data = att.Attestation.Data
	}

	// Commented because we have a check in validatorAttestationSeen that does the same thing.

	// key, err := att.Attestation.HashSSZ()
	// if err != nil {
	// 	return err
	// }
	// if _, ok := s.attestationProcessed.Get(key); ok {
	// 	return ErrIgnore
	// }
	// s.attestationProcessed.Add(key, struct{}{})

	// [IGNORE] attestation.data.slot is within the last ATTESTATION_PROPAGATION_SLOT_RANGE slots (within a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) --
	// i.e. attestation.data.slot + ATTESTATION_PROPAGATION_SLOT_RANGE >= current_slot >= attestation.data.slot (a client MAY queue future attestations for processing at the appropriate slot).
	currentSlot := s.ethClock.GetCurrentSlot()
	if currentSlot < slot || currentSlot > slot+s.netCfg.AttestationPropagationSlotRange {
		return fmt.Errorf("not in propagation range: %w", ErrIgnore)
	}
	// [REJECT] The attestation's epoch matches its target -- i.e. attestation.data.target.epoch == compute_epoch_at_slot(attestation.data.slot)
	if targetEpoch != slot/s.beaconCfg.SlotsPerEpoch {
		return errors.New("epoch mismatch")
	}

	if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// If our head state is too far from the attestation epoch, committee
		// computations will use a stale RANDAO mix and produce wrong results.
		prevEpoch, currEpoch := validationEpochRange(headState, s.forkchoiceStore.HighestSeen(), currentSlot, s.beaconCfg.SlotsPerEpoch)
		if attEpoch < prevEpoch || attEpoch > currEpoch {
			return fmt.Errorf("head epoch %d too far from attestation epoch %d (prev=%d, curr=%d): %w",
				state.Epoch(headState), attEpoch, prevEpoch, currEpoch, ErrIgnore)
		}
		return nil
	}); err != nil {
		return err
	}
	if clVersion >= clparams.ElectraVersion {
		if clVersion >= clparams.GloasVersion {
			// [New in Gloas] [REJECT] attestation.data.index < 2
			if att.SingleAttestation.Data.CommitteeIndex >= 2 {
				return errors.New("attestation data index must be less than 2")
			}
		} else {
			// [REJECT] attestation.data.index == 0
			if att.SingleAttestation.Data.CommitteeIndex != 0 {
				return errors.New("committee index must be 0")
			}
		}
	}

	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).
	blockHeader, ok := s.forkchoiceStore.GetHeader(root)
	if !ok {
		if !att.ImmediateProcess {
			s.awaitingBlock.add(root, func(ctx context.Context) { _ = s.ProcessMessage(ctx, subnet, att) })
		}
		return fmt.Errorf("%w: %w: %v", ErrIgnore, ErrBlockNotSeen, root)
	}

	var (
		verifyBeforeBuild func() error
		claimBuild        func() bool
	)
	if clVersion >= clparams.ElectraVersion {
		claimBuild = claimTargetStateBuild(s.targetStateBuildClaims, att.SingleAttestation.AttesterIndex, targetEpoch)
		verifyBeforeBuild = func() error {
			validatorIndex := att.SingleAttestation.AttesterIndex
			if epochLastTime, ok := s.validatorAttestationSeen.Get(validatorIndex); ok && epochLastTime == targetEpoch {
				return fmt.Errorf("%w: %w", ErrIgnore, ErrAttestationAlreadySeen)
			}
			pubKey, signingRoot, err := attestationSignatureData(
				s.syncedDataManager, s.beaconCfg, data, validatorIndex, targetEpoch,
			)
			if err != nil {
				return err
			}
			valid, err := blsVerifyMultipleSignatures(
				[][]byte{signature[:]},
				[][]byte{signingRoot[:]},
				[][]byte{pubKey[:]},
			)
			if err != nil {
				return err
			}
			if !valid {
				return ErrInvalidBlsSignature
			}
			return nil
		}
	}
	committee, committeesPerSlot, err := beaconCommittee(
		s.forkchoiceStore, s.syncedDataManager, data.Target, root, slot, committeeIndex,
		s.beaconCfg.SlotsPerEpoch, verifyBeforeBuild, claimBuild,
	)
	if err != nil {
		return err
	}
	// [REJECT] The committee index is within the expected range
	if committeeIndex >= committeesPerSlot {
		return fmt.Errorf("committee index out of range, %d >= %d", committeeIndex, committeesPerSlot)
	}
	// [REJECT] The attestation is for the correct subnet -- i.e. compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, index) == subnet_id
	subnetID := computeSubnetForAttestation(committeesPerSlot, slot, committeeIndex, s.beaconCfg.SlotsPerEpoch, s.netCfg.AttestationSubnetCount)
	if subnet == nil || subnetID != *subnet {
		return errors.New("wrong subnet")
	}

	var (
		vIndex      uint64
		attestation *solid.Attestation // SingleAttestation will be transformed to Attestation struct with given member index in committee
	)
	if clVersion <= clparams.DenebVersion {
		// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, attestation.data.slot, index)).
		bits := att.Attestation.AggregationBits.Bytes()
		expectedAggregationBitsLength := len(committee)
		actualAggregationBitsLength := utils.GetBitlistLength(bits)
		if actualAggregationBitsLength != expectedAggregationBitsLength {
			return fmt.Errorf("aggregation bits count mismatch: %d != %d", actualAggregationBitsLength, expectedAggregationBitsLength)
		}

		//[REJECT] The attestation is unaggregated -- that is, it has exactly one participating validator (len([bit for bit in aggregation_bits if bit]) == 1, i.e. exactly 1 bit is set).
		setBits := 0
		onBitIndex := 0 // Aggregationbits is []byte, so we need to iterate over all bits.
		for i := range bits {
			for j := range 8 {
				if bits[i]&(1<<uint(j)) != 0 {
					if i*8+j >= len(committee) {
						continue
					}
					setBits++
					onBitIndex = i*8 + j
				}
			}
		}
		if setBits == 0 {
			return ErrIgnore // Ignore if it is just an empty bitlist
		}
		if setBits != 1 {
			return errors.New("attestation does not have exactly one participating validator")
		}
		if onBitIndex >= len(committee) {
			return errors.New("on bit index out of committee range")
		}
		vIndex = committee[onBitIndex]
		attestation = att.Attestation
	} else {
		// electra and after
		// [REJECT] The attester is a member of the committee -- i.e. attestation.attester_index in get_beacon_committee(state, attestation.data.slot, index).
		memIndexInCommittee := contains(att.SingleAttestation.AttesterIndex, committee)
		if memIndexInCommittee < 0 {
			return fmt.Errorf("attester is not a member of the committee. attester index %d committeeIndex %v", att.SingleAttestation.AttesterIndex, committeeIndex)
		}
		vIndex = att.SingleAttestation.AttesterIndex
		attestation, err = att.SingleAttestation.ToAttestation(memIndexInCommittee, len(committee), int(s.beaconCfg.MaxCommitteesPerSlot), s.beaconCfg)
		if err != nil {
			return fmt.Errorf("invalid committee index: %w", err)
		}
	}

	// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
	if epochLastTime, ok := s.validatorAttestationSeen.Get(vIndex); ok && epochLastTime == targetEpoch {
		return fmt.Errorf("%w: %w", ErrIgnore, ErrAttestationAlreadySeen)
	}
	// [New in Gloas] [REJECT] attestation.data.index == 0 if block.slot == attestation.data.slot
	if clVersion >= clparams.GloasVersion {
		if blockHeader.Slot == data.Slot && data.CommitteeIndex != 0 {
			return errors.New("attestation data index must be 0 when block slot equals attestation slot")
		}
		if data.CommitteeIndex == 1 && !s.forkchoiceStore.IsPayloadVerified(root) {
			return unverifiedGloasPayloadError(s.forkchoiceStore, root)
		}
	}
	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	startSlotAtEpoch := targetEpoch * s.beaconCfg.SlotsPerEpoch
	if targetBlock := s.forkchoiceStore.Ancestor(root, startSlotAtEpoch); targetBlock.Root != data.Target.Root {
		return fmt.Errorf("invalid target block. root %v targetEpoch %v attTargetBlockRoot %v targetBlock %v", root.Hex(), targetEpoch, data.Target.Root.Hex(), targetBlock.Root.Hex())
	}
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	startSlotAtEpoch = s.forkchoiceStore.FinalizedCheckpoint().Epoch * s.beaconCfg.SlotsPerEpoch
	if s.forkchoiceStore.Ancestor(root, startSlotAtEpoch).Root != s.forkchoiceStore.FinalizedCheckpoint().Root {
		return fmt.Errorf("invalid finalized checkpoint %w", ErrIgnore)
	}

	// [REJECT] The signature of attestation is valid.
	pubKey, signingRoot, err := attestationSignatureData(s.syncedDataManager, s.beaconCfg, data, vIndex, targetEpoch)
	if err != nil {
		return err
	}

	aggregateVerificationData := &AggregateVerificationData{
		Signatures:  [][]byte{signature[:]},
		SignRoots:   [][]byte{signingRoot[:]},
		Pks:         [][]byte{pubKey[:]},
		SendingPeer: att.Receiver,
		F: func() {
			s.onVerifiedAttestation(vIndex, targetEpoch, attestation, att)
		},
	}

	if att.ImmediateProcess {
		return s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
	}

	return s.batchSignatureVerifier.VerifyAttestation(ctx, aggregateVerificationData)
}

func contains[T comparable](target T, slices []T) int {
	for i, s := range slices {
		if s == target {
			return i
		}
	}
	return -1
}

func (s *attestationService) onVerifiedAttestation(vIndex, targetEpoch uint64, attestation *solid.Attestation, att *AttestationForGossip) {
	start := time.Now()
	defer monitor.ObserveAggregateAttestation(start)
	// Claimed only now: a message dropped before its signature was
	// verified must not consume the validator's slot for the epoch.
	if !s.claimValidatorAttestation(vIndex, targetEpoch) {
		return
	}
	if err := s.aggregationPool.AddAttestation(attestation); errors.Is(err, aggregation.ErrPoolLimit) || errors.Is(err, aggregation.ErrVoterAlreadyCounted) {
		dataRoot, _ := attestation.Data.HashSSZ()
		log.Debug("aggregation pool refused attestation", "slot", attestation.Data.Slot,
			"committees", attestation.CommitteeBits.GetOnIndices(), "dataRoot", common.Hash(dataRoot), "err", err)
	} else if err != nil && !errors.Is(err, aggregation.ErrIsSuperset) {
		log.Warn("could not add attestation to the aggregation pool", "slot", attestation.Data.Slot, "err", err)
	}
	// send to subscribers
	if att.Attestation != nil {
		s.emitters.Operation().SendAttestation(att.Attestation)
	} else if att.SingleAttestation != nil {
		s.emitters.Operation().SendSingleAttestation(att.SingleAttestation)
	}
}

// claimValidatorAttestation records the validator's vote for the target epoch and reports whether
// this call made the claim. Copies of one vote can pass the seen check before either is verified.
func (s *attestationService) claimValidatorAttestation(vIndex, targetEpoch uint64) bool {
	s.validatorClaimMu.Lock()
	defer s.validatorClaimMu.Unlock()
	if epoch, ok := s.validatorAttestationSeen.Get(vIndex); ok && epoch == targetEpoch {
		return false
	}
	s.validatorAttestationSeen.Add(vIndex, targetEpoch)
	return true
}
