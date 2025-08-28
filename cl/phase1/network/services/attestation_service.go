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
	"time"

	"github.com/erigontech/erigon-lib/common"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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
)

type attestationService struct {
	ctx                    context.Context
	forkchoiceStore        forkchoice.ForkChoiceStorage
	committeeSubscribe     committee_subscription.CommitteeSubscribe
	ethClock               eth_clock.EthereumClock
	syncedDataManager      synced_data.SyncedData
	beaconCfg              *clparams.BeaconChainConfig
	netCfg                 *clparams.NetworkConfig
	emitters               *beaconevents.EventEmitter
	batchSignatureVerifier *BatchSignatureVerifier
	// validatorAttestationSeen maps from epoch to validator index. This is used to ignore duplicate validator attestations in the same epoch.
	validatorAttestationSeen *lru.CacheWithTTL[uint64, uint64] // validator index -> epoch
	// attestationProcessed           *lru.CacheWithTTL[[32]byte, struct{}]
	// attestationsToBeLaterProcessed sync.Map
}

// AttestationWithGossipData type represents attestation with the gossip data where it's coming from.
type AttestationForGossip struct {
	Attestation       *solid.Attestation
	SingleAttestation *solid.SingleAttestation // New container after Electra
	Receiver          *sentinel.Peer
	// ImmediateProcess indicates whether the attestation should be processed immediately or able to be scheduled for later processing.
	ImmediateProcess bool
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
	batchSignatureVerifier *BatchSignatureVerifier,
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
		batchSignatureVerifier:   batchSignatureVerifier,
		validatorAttestationSeen: lru.NewWithTTL[uint64, uint64]("validator_attestation_seen", validatorAttestationCacheSize, epochDuration),
		//attestationProcessed:     lru.NewWithTTL[[32]byte, struct{}]("attestation_processed", validatorAttestationCacheSize, epochDuration),
	}

	//go a.loop(ctx)
	return a
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
		return fmt.Errorf("not in propagation range %w", ErrIgnore)
	}
	// [REJECT] The attestation's epoch matches its target -- i.e. attestation.data.target.epoch == compute_epoch_at_slot(attestation.data.slot)
	if targetEpoch != slot/s.beaconCfg.SlotsPerEpoch {
		return errors.New("epoch mismatch")
	}

	var (
		domain      []byte
		pubKey      common.Bytes48
		attestation *solid.Attestation // SingleAttestation will be transformed to Attestation struct with given member index in committee
	)
	if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
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
		beaconCommittee, err := headState.GetBeaconCommitee(slot, committeeIndex)
		if err != nil {
			return err
		}
		var vIndex uint64
		if clVersion <= clparams.DenebVersion {
			// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, attestation.data.slot, index)).
			bits := att.Attestation.AggregationBits.Bytes()
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
			if onBitIndex >= len(beaconCommittee) {
				return errors.New("on bit index out of committee range")
			}
			vIndex = beaconCommittee[onBitIndex]
			attestation = att.Attestation
		} else {
			// electra and after
			// [REJECT] attestation.data.index == 0
			if att.SingleAttestation.Data.CommitteeIndex != 0 {
				return errors.New("committee index must be 0")
			}
			// [REJECT] The attester is a member of the committee -- i.e. attestation.attester_index in get_beacon_committee(state, attestation.data.slot, index).
			memIndexInCommittee := contains(att.SingleAttestation.AttesterIndex, beaconCommittee)
			if memIndexInCommittee < 0 {
				//return errors.New("attester is not a member of the committee")
				return fmt.Errorf("attester is not a member of the committee. attester index %d committeeIndex %v", att.SingleAttestation.AttesterIndex, committeeIndex)
			}
			vIndex = att.SingleAttestation.AttesterIndex
			attestation = att.SingleAttestation.ToAttestation(memIndexInCommittee, len(beaconCommittee))
		}
		// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
		// mark the validator as seen
		epochLastTime, ok := s.validatorAttestationSeen.Get(vIndex)
		if ok && epochLastTime == targetEpoch {
			return fmt.Errorf("validator already seen in target epoch %w", ErrIgnore)
		}
		s.validatorAttestationSeen.Add(vIndex, targetEpoch)

		// [REJECT] The signature of attestation is valid.
		pubKey, err = headState.ValidatorPublicKey(int(vIndex))
		if err != nil {
			return fmt.Errorf("unable to get public key: %v", err)
		}
		domain, err = headState.GetDomain(s.beaconCfg.DomainBeaconAttester, targetEpoch)
		if err != nil {
			return fmt.Errorf("unable to get the domain: %v", err)
		}
		return nil
	}); err != nil {
		return err
	}
	signingRoot, err := computeSigningRoot(data, domain)
	if err != nil {
		return fmt.Errorf("unable to get signing root: %v", err)
	}

	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).
	if _, ok := s.forkchoiceStore.GetHeader(root); !ok {
		//s.scheduleAttestationForLaterProcessing(att)
		return ErrIgnore
	}

	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	startSlotAtEpoch := targetEpoch * s.beaconCfg.SlotsPerEpoch
	if targetBlock := s.forkchoiceStore.Ancestor(root, startSlotAtEpoch); targetBlock != data.Target.Root {
		return fmt.Errorf("invalid target block. root %v targetEpoch %v attTargetBlockRoot %v targetBlock %v", root.Hex(), targetEpoch, data.Target.Root.Hex(), targetBlock.Hex())
	}
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	startSlotAtEpoch = s.forkchoiceStore.FinalizedCheckpoint().Epoch * s.beaconCfg.SlotsPerEpoch
	if s.forkchoiceStore.Ancestor(root, startSlotAtEpoch) != s.forkchoiceStore.FinalizedCheckpoint().Root {
		return fmt.Errorf("invalid finalized checkpoint %w", ErrIgnore)
	}

	aggregateVerificationData := &AggregateVerificationData{
		Signatures:  [][]byte{signature[:]},
		SignRoots:   [][]byte{signingRoot[:]},
		Pks:         [][]byte{pubKey[:]},
		SendingPeer: att.Receiver,
		F: func() {
			start := time.Now()
			defer monitor.ObserveAggregateAttestation(start)
			if err = s.committeeSubscribe.AggregateAttestation(attestation); errors.Is(err, aggregation.ErrIsSuperset) {
				return
			} else if err != nil {
				log.Warn("could not check aggregate attestation", "err", err)
				return
			}
			// send to subscribers
			if att.Attestation != nil {
				s.emitters.Operation().SendAttestation(att.Attestation)
			} else if att.SingleAttestation != nil {
				s.emitters.Operation().SendSingleAttestation(att.SingleAttestation)
			}
		},
	}

	if att.ImmediateProcess {
		return s.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)

	}

	// push the signatures to verify asynchronously and run final functions after that.
	s.batchSignatureVerifier.AsyncVerifyAttestation(aggregateVerificationData)

	// As the logic goes, if we return ErrIgnore there will be no peer banning and further publishing
	// gossip data into the network by the gossip manager. That's what we want because we will be doing that ourselves
	// in BatchSignatureVerifier service. After validating signatures, if they are valid we will publish the
	// gossip ourselves or ban the peer which sent that particular invalid signature.
	return ErrIgnore
}

// type attestationJob struct {
// 	att          *AttestationWithGossipData
// 	creationTime time.Time
// 	subnet       uint64
// }

// func (a *attestationService) scheduleAttestationForLaterProcessing(att *AttestationWithGossipData) {
// 	key, err := att.Attestation.HashSSZ()
// 	if err != nil {
// 		return
// 	}
// 	a.attestationsToBeLaterProcessed.Store(key, &attestationJob{
// 		att:          att,
// 		creationTime: time.Now(),
// 	})
// }

// func (a *attestationService) loop(ctx context.Context) {
// 	ticker := time.NewTicker(singleAttestationIntervalTick)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-ticker.C:
// 		}
// 		a.attestationsToBeLaterProcessed.Range(func(key, value any) bool {
// 			k := key.([32]byte)
// 			v := value.(*attestationJob)
// 			if time.Now().After(v.creationTime.Add(singleAttestationJobExpiry)) {
// 				a.attestationsToBeLaterProcessed.Delete(k)
// 				return true
// 			}

// 			root := v.att.Attestation.Data.BeaconBlockRoot
// 			if _, ok := a.forkchoiceStore.GetHeader(root); !ok {
// 				return true
// 			}
// 			a.ProcessMessage(ctx, &v.subnet, v.att)
// 			return true
// 		})
// 	}
// }

func contains[T comparable](target T, slices []T) int {
	for i, s := range slices {
		if s == target {
			return i
		}
	}
	return -1
}
