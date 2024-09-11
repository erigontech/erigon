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
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"
)

var (
	computeSubnetForAttestation  = subnets.ComputeSubnetForAttestation
	computeCommitteeCountPerSlot = subnets.ComputeCommitteeCountPerSlot
	computeSigningRoot           = fork.ComputeSigningRoot
	blsVerify                    = bls.Verify
	blsVerifyMultipleSignatures  = bls.VerifyMultipleSignatures
	batchCheckInterval           = 50 * time.Millisecond
)

type attestationService struct {
	ctx                context.Context
	forkchoiceStore    forkchoice.ForkChoiceStorage
	committeeSubscribe committee_subscription.CommitteeSubscribe
	ethClock           eth_clock.EthereumClock
	syncedDataManager  synced_data.SyncedData
	beaconCfg          *clparams.BeaconChainConfig
	netCfg             *clparams.NetworkConfig
	emitters           *beaconevents.EventEmitter
	sentinel           sentinel.SentinelClient
	verifyAndExecute   chan *AggregateVerificationData
	// validatorAttestationSeen maps from epoch to validator index. This is used to ignore duplicate validator attestations in the same epoch.
	validatorAttestationSeen       *lru.CacheWithTTL[uint64, uint64] // validator index -> epoch
	attestationProcessed           *lru.CacheWithTTL[[32]byte, struct{}]
	attestationsToBeLaterProcessed sync.Map
}

// AttestationWithGossipData type represents attestation with the gossip data where it's coming from.
type AttestationWithGossipData struct {
	Attestation *solid.Attestation
	GossipData  *sentinel.GossipData
}

func NewAttestationService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	committeeSubscribe committee_subscription.CommitteeSubscribe,
	ethClock eth_clock.EthereumClock,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	netCfg *clparams.NetworkConfig,
	emitters *beaconevents.EventEmitter,
	sentinel sentinel.SentinelClient,
) AttestationService {
	epochDuration := time.Duration(beaconCfg.SlotsPerEpoch*beaconCfg.SecondsPerSlot) * time.Second
	a := &attestationService{
		ctx:                      ctx,
		forkchoiceStore:          forkchoiceStore,
		committeeSubscribe:       committeeSubscribe,
		ethClock:                 ethClock,
		syncedDataManager:        syncedDataManager,
		beaconCfg:                beaconCfg,
		netCfg:                   netCfg,
		emitters:                 emitters,
		sentinel:                 sentinel,
		verifyAndExecute:         make(chan *AggregateVerificationData, 128),
		validatorAttestationSeen: lru.NewWithTTL[uint64, uint64]("validator_attestation_seen", validatorAttestationCacheSize, epochDuration),
		attestationProcessed:     lru.NewWithTTL[[32]byte, struct{}]("attestation_processed", validatorAttestationCacheSize, epochDuration),
	}

	go a.startAttestationBatchSignatureVerification()
	go a.loop(ctx)
	return a
}

// When receiving AttestationWithGossipData, we simply collect all the signature verification data
// and verify them together - running all the final functions afterwards
func (a *attestationService) startAttestationBatchSignatureVerification() {
	ticker := time.NewTicker(batchCheckInterval)
	aggregateVerificationData := make([]*AggregateVerificationData, 0, 128)
	for {
		select {
		case verification := <-a.verifyAndExecute:
			aggregateVerificationData = append(aggregateVerificationData, verification)
			if len(aggregateVerificationData)*3 > BatchSignatureVerificationThreshold {
				a.processSignatureVerification(aggregateVerificationData)
				aggregateVerificationData = make([]*AggregateVerificationData, 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		case <-ticker.C:
			if len(aggregateVerificationData) != 0 {
				a.processSignatureVerification(aggregateVerificationData)
				aggregateVerificationData = make([]*AggregateVerificationData, 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		}
	}
}

// processSignatureVerification Runs signature verification for all the signatures altogether, if it
// succeeds we publish all accumulated gossip data. If verification fails, start verifying each AggregateVerificationData one by
// one, publish corresponding gossip data if verification succeeds, if not ban the corresponding peer that sent it.
func (a *attestationService) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) {
	signatures, signRoots, pks, fns :=
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([]func(), 0, 64)

	for _, v := range aggregateVerificationData {
		signatures, signRoots, pks, fns =
			append(signatures, v.Signatures...),
			append(signRoots, v.SignRoots...),
			append(pks, v.Pks...),
			append(fns, v.F)
	}
	if err := a.runBatchVerification(signatures, signRoots, pks, fns); err != nil {
		a.handleIncorrectSignatures(aggregateVerificationData)
		log.Warn(err.Error())
		return
	}

	// Everything went well, run corresponding Fs and send all the gossip data to the network
	for _, v := range aggregateVerificationData {
		v.F()
		if a.sentinel != nil && v.GossipData != nil {
			if _, err := a.sentinel.PublishGossip(a.ctx, v.GossipData); err != nil {
				log.Warn("failed publish gossip", "err", err)
			}
		}
	}
}

// we could locate failing signature with binary search but for now let's choose simplicity over optimisation.
func (a *attestationService) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) {
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Warn("attestation_service signature verification failed with the error: " + err.Error())
			if a.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				a.sentinel.BanPeer(a.ctx, v.GossipData.Peer)
			}
			continue
		}

		if !valid {
			log.Warn("attestation_service signature verification failed")
			if a.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				a.sentinel.BanPeer(a.ctx, v.GossipData.Peer)
			}
			continue
		}

		// run corresponding function and publish the gossip into the network
		v.F()

		if a.sentinel != nil && v.GossipData != nil {
			if _, err := a.sentinel.PublishGossip(a.ctx, v.GossipData); err != nil {
				log.Warn("failed publish gossip", "err", err)
			}
		}
	}
}

func (a *attestationService) runBatchVerification(signatures [][]byte, signRoots [][]byte, pks [][]byte, fns []func()) error {
	valid, err := blsVerifyMultipleSignatures(signatures, signRoots, pks)
	if err != nil {
		return errors.New("attestation_service batch signature verification failed with the error: " + err.Error())
	}

	if !valid {
		return errors.New("attestation_service signature verification failed")
	}

	return nil
}

func (s *attestationService) ProcessMessage(ctx context.Context, subnet *uint64, att *AttestationWithGossipData) error {
	var (
		root           = att.Attestation.AttestantionData().BeaconBlockRoot()
		slot           = att.Attestation.AttestantionData().Slot()
		committeeIndex = att.Attestation.AttestantionData().CommitteeIndex()
		targetEpoch    = att.Attestation.AttestantionData().Target().Epoch()
	)
	headState := s.syncedDataManager.HeadStateReader()
	if headState == nil {
		return ErrIgnore
	}

	key, err := att.Attestation.HashSSZ()
	if err != nil {
		return err
	}
	if _, ok := s.attestationProcessed.Get(key); ok {
		return ErrIgnore
	}
	s.attestationProcessed.Add(key, struct{}{})

	// [REJECT] The committee index is within the expected range
	committeeCount := computeCommitteeCountPerSlot(headState, slot, s.beaconCfg.SlotsPerEpoch)
	if committeeIndex >= committeeCount {
		return fmt.Errorf("committee index out of range, %d >= %d", committeeIndex, committeeCount)
	}
	// [REJECT] The attestation is for the correct subnet -- i.e. compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, index) == subnet_id
	subnetId := computeSubnetForAttestation(committeeCount, slot, committeeIndex, s.beaconCfg.SlotsPerEpoch, s.netCfg.AttestationSubnetCount)
	if subnet == nil || subnetId != *subnet {
		return errors.New("wrong subnet")
	}
	// [IGNORE] attestation.data.slot is within the last ATTESTATION_PROPAGATION_SLOT_RANGE slots (within a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) --
	// i.e. attestation.data.slot + ATTESTATION_PROPAGATION_SLOT_RANGE >= current_slot >= attestation.data.slot (a client MAY queue future attestations for processing at the appropriate slot).
	currentSlot := s.ethClock.GetCurrentSlot()
	if currentSlot < slot || currentSlot > slot+s.netCfg.AttestationPropagationSlotRange {
		return fmt.Errorf("not in propagation range %w", ErrIgnore)
	}
	// [REJECT] The attestation's epoch matches its target -- i.e. attestation.data.target.epoch == compute_epoch_at_slot(attestation.data.slot)
	if targetEpoch != slot/s.beaconCfg.SlotsPerEpoch {
		return errors.New("epoch mismatch")
	}
	// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, attestation.data.slot, index)).
	beaconCommittee, err := s.forkchoiceStore.GetBeaconCommitee(slot, committeeIndex)
	if err != nil {
		return err
	}
	bits := att.Attestation.AggregationBits()
	expectedAggregationBitsLength := len(beaconCommittee)
	actualAggregationBitsLength := utils.GetBitlistLength(bits)
	if actualAggregationBitsLength != expectedAggregationBitsLength {
		return fmt.Errorf("aggregation bits count mismatch: %d != %d", actualAggregationBitsLength, expectedAggregationBitsLength)
	}

	//[REJECT] The attestation is unaggregated -- that is, it has exactly one participating validator (len([bit for bit in aggregation_bits if bit]) == 1, i.e. exactly 1 bit is set).
	setBits := 0
	onBitIndex := 0 // Aggregationbits is []byte, so we need to iterate over all bits.
	for i := 0; i < len(bits); i++ {
		for j := 0; j < 8; j++ {
			if bits[i]&(1<<uint(j)) != 0 {
				if i*8+j >= len(beaconCommittee) {
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
	// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
	if err != nil {
		return err
	}
	if onBitIndex >= len(beaconCommittee) {
		return errors.New("on bit index out of committee range")
	}
	// mark the validator as seen
	vIndex := beaconCommittee[onBitIndex]
	epochLastTime, ok := s.validatorAttestationSeen.Get(vIndex)
	if ok && epochLastTime == targetEpoch {
		return fmt.Errorf("validator already seen in target epoch %w", ErrIgnore)
	}
	s.validatorAttestationSeen.Add(vIndex, targetEpoch)

	// [REJECT] The signature of attestation is valid.
	signature := att.Attestation.Signature()
	pubKey, err := headState.ValidatorPublicKey(int(beaconCommittee[onBitIndex]))
	if err != nil {
		return fmt.Errorf("unable to get public key: %v", err)
	}
	domain, err := headState.GetDomain(s.beaconCfg.DomainBeaconAttester, targetEpoch)
	if err != nil {
		return fmt.Errorf("unable to get the domain: %v", err)
	}
	signingRoot, err := computeSigningRoot(att.Attestation.AttestantionData(), domain)
	if err != nil {
		return fmt.Errorf("unable to get signing root: %v", err)
	}

	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).
	if _, ok := s.forkchoiceStore.GetHeader(root); !ok {
		s.scheduleAttestationForLaterProcessing(att)
		return ErrIgnore
	}

	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	startSlotAtEpoch := targetEpoch * s.beaconCfg.SlotsPerEpoch
	if targetBlock := s.forkchoiceStore.Ancestor(root, startSlotAtEpoch); targetBlock != att.Attestation.AttestantionData().Target().BlockRoot() {
		return fmt.Errorf("invalid target block. root %v targetEpoch %v attTargetBlockRoot %v targetBlock %v", root.Hex(), targetEpoch, att.Attestation.AttestantionData().Target().BlockRoot().Hex(), targetBlock.Hex())
	}
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	startSlotAtEpoch = s.forkchoiceStore.FinalizedCheckpoint().Epoch() * s.beaconCfg.SlotsPerEpoch
	if s.forkchoiceStore.Ancestor(root, startSlotAtEpoch) != s.forkchoiceStore.FinalizedCheckpoint().BlockRoot() {
		return fmt.Errorf("invalid finalized checkpoint %w", ErrIgnore)
	}

	aggregateVerificationData := &AggregateVerificationData{
		Signatures: [][]byte{signature[:]},
		SignRoots:  [][]byte{signingRoot[:]},
		Pks:        [][]byte{pubKey[:]},
		GossipData: att.GossipData,
		F: func() {
			err = s.committeeSubscribe.CheckAggregateAttestation(att.Attestation)
			if errors.Is(err, aggregation.ErrIsSuperset) {
				return
			}
			if err != nil {
				log.Warn("could not check aggregate attestation", "err", err)
				return
			}
			s.emitters.Operation().SendAttestation(att.Attestation)
		},
	}

	// push the signatures to verify asynchronously and run final functions after that.
	s.verifyAndExecute <- aggregateVerificationData

	// As the logic goes, if we return ErrIgnore there will be no peer banning and further publishing
	// gossip data into the network by the gossip manager. That's what we want because we will be doing that ourselves
	// in startBatchSignatureVerification function. After validating signatures, if they are valid we will publish the
	// gossip ourselves or ban the peer which sent that particular invalid signature.
	return ErrIgnore
}

type attestationJob struct {
	att          *AttestationWithGossipData
	creationTime time.Time
	subnet       uint64
}

func (a *attestationService) scheduleAttestationForLaterProcessing(att *AttestationWithGossipData) {
	key, err := att.Attestation.HashSSZ()
	if err != nil {
		return
	}
	a.attestationsToBeLaterProcessed.Store(key, &attestationJob{
		att:          att,
		creationTime: time.Now(),
	})
}

func (a *attestationService) loop(ctx context.Context) {
	ticker := time.NewTicker(singleAttestationIntervalTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		a.attestationsToBeLaterProcessed.Range(func(key, value any) bool {
			k := key.([32]byte)
			v := value.(*attestationJob)
			if time.Now().After(v.creationTime.Add(singleAttestationJobExpiry)) {
				a.attestationsToBeLaterProcessed.Delete(k)
				return true
			}

			root := v.att.Attestation.AttestantionData().BeaconBlockRoot()
			if _, ok := a.forkchoiceStore.GetHeader(root); !ok {
				return true
			}
			a.ProcessMessage(ctx, &v.subnet, v.att)
			return true
		})
	}
}
