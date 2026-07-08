// Copyright 2026 The Erigon Authors
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

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seenPayloadAttestationKey tracks seen attestations per (slot, validatorIndex).
type seenPayloadAttestationKey struct {
	slot           uint64
	validatorIndex uint64
}

// pendingPayloadAttestationKey tracks attestations waiting for their block to arrive.
// Key is (blockRoot, validatorIndex) since each validator can only submit one attestation per block.
type pendingPayloadAttestationKey struct {
	blockRoot      common.Hash
	validatorIndex uint64
}

const (
	// seenPayloadAttestationCacheSize: PTC has 512 validators per slot.
	// With clock disparity, we may see attestations for ~2 slots.
	// 512 * 4 = 2048 provides safety margin.
	seenPayloadAttestationCacheSize        = 2048
	pendingPayloadAttestationExpiry        = 30 * time.Second
	pendingPayloadAttestationCheckInterval = 100 * time.Millisecond
	maxPendingAttestations                 = 2048
)

type payloadAttestationService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	ethClock        eth_clock.EthereumClock
	netCfg          *clparams.NetworkConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen attestations: (slot, validatorIndex) -> struct{}
	seenAttestationsCache *lru.Cache[seenPayloadAttestationKey, struct{}]

	// Pending attestations waiting for block to arrive
	pending *pendingJobQueue[pendingPayloadAttestationKey, *cltypes.PayloadAttestationMessage]
}

// NewPayloadAttestationService creates a new payload attestation service.
// [New in Gloas:EIP7732]
func NewPayloadAttestationService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	ethClock eth_clock.EthereumClock,
	netCfg *clparams.NetworkConfig,
	emitters *beaconevents.EventEmitter,
) PayloadAttestationService {
	seenCache, err := lru.New[seenPayloadAttestationKey, struct{}]("seen_payload_attestations", seenPayloadAttestationCacheSize)
	if err != nil {
		panic(err)
	}
	s := &payloadAttestationService{
		forkchoiceStore:       forkchoiceStore,
		ethClock:              ethClock,
		netCfg:                netCfg,
		emitters:              emitters,
		seenAttestationsCache: seenCache,
	}
	s.pending = s.newPendingQueue()
	go s.pending.loop(ctx)
	return s
}

func (s *payloadAttestationService) newPendingQueue() *pendingJobQueue[pendingPayloadAttestationKey, *cltypes.PayloadAttestationMessage] {
	return newPendingJobQueue(maxPendingAttestations, pendingPayloadAttestationExpiry, pendingPayloadAttestationCheckInterval,
		s.tryProcessPendingAttestation,
		func(key pendingPayloadAttestationKey) {
			log.Trace("Pending payload attestation expired", "blockRoot", key.blockRoot)
		})
}

func (s *payloadAttestationService) Names() []string {
	return []string{gossip.TopicNamePayloadAttestation}
}

func (s *payloadAttestationService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.PayloadAttestationMessage, error) {
	msg := &cltypes.PayloadAttestationMessage{}
	if err := msg.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return msg, nil
}

// ProcessMessage processes a payload attestation message according to the gossip spec.
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/_features/epbs/p2p-interface.md#payload_attestation_message
// [New in Gloas:EIP7732]
func (s *payloadAttestationService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.PayloadAttestationMessage) error {
	if msg == nil || msg.Data == nil {
		return fmt.Errorf("nil payload attestation message")
	}

	data := msg.Data
	slot := data.Slot
	validatorIndex := msg.ValidatorIndex
	blockRoot := data.BeaconBlockRoot

	log.Trace("Received payload attestation message via gossip",
		"slot", slot,
		"validatorIndex", validatorIndex,
		"blockRoot", blockRoot)

	// [IGNORE] The message's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance)
	if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(slot) {
		return fmt.Errorf("%w: payload attestation slot %d is not current slot (with clock disparity)", ErrIgnore, slot)
	}

	// [IGNORE] The payload_attestation_message is the first valid message received from the validator
	seenKey := seenPayloadAttestationKey{
		slot:           slot,
		validatorIndex: validatorIndex,
	}
	if s.seenAttestationsCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen payload attestation from validator %d for slot %d", ErrIgnore, validatorIndex, slot)
	}

	// [IGNORE] The message's block root has been seen (via gossip or non-gossip sources)
	// A client MAY queue attestation for processing once the block is retrieved.
	blockHeader, ok := s.forkchoiceStore.GetHeader(blockRoot)
	if !ok {
		// Block hasn't arrived yet, queue attestation for later processing
		s.queuePendingAttestation(blockRoot, msg)
		log.Trace("Queued payload attestation for later processing",
			"blockRoot", blockRoot,
			"validatorIndex", validatorIndex)
		return nil
	}
	// [IGNORE] The block referenced by data.beacon_block_root is at data.slot.
	if blockHeader.Slot != slot {
		return fmt.Errorf("%w: payload attestation slot %d does not match referenced block slot %d", ErrIgnore, slot, blockHeader.Slot)
	}

	// Process through forkchoice which handles:
	// [IGNORE] block state not found
	// [REJECT] validator is not in PTC
	// [REJECT] signature verification
	if err := s.forkchoiceStore.OnPayloadAttestationMessage(msg, false); err != nil {
		// Preserve IGNORE vs REJECT distinction from forkchoice
		// forkchoice.ErrIgnore != services.ErrIgnore, so we need to convert
		if errors.Is(err, forkchoice.ErrIgnore) {
			return fmt.Errorf("%w: %v", ErrIgnore, err)
		}
		return fmt.Errorf("forkchoice rejected payload attestation: %w", err)
	}

	// Mark as seen AFTER successful validation
	s.seenAttestationsCache.Add(seenKey, struct{}{})

	// Emit SSE event for payload_attestation_message [New in Gloas:EIP7732]
	s.emitters.Operation().SendPayloadAttestationMessage(msg)

	log.Trace("Processed payload attestation message via gossip",
		"slot", slot,
		"validatorIndex", validatorIndex,
		"blockRoot", blockRoot,
		"payloadPresent", data.PayloadPresent,
		"blobDataAvailable", data.BlobDataAvailable)

	return nil
}

// queuePendingAttestation adds an attestation to the pending queue for later processing.
func (s *payloadAttestationService) queuePendingAttestation(blockRoot common.Hash, msg *cltypes.PayloadAttestationMessage) {
	s.pending.enqueue(pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: msg.ValidatorIndex,
	}, msg)
}

// tryProcessPendingAttestation re-runs validation via ProcessMessage once the block has arrived,
// dropping attestations that are no longer for the current slot.
func (s *payloadAttestationService) tryProcessPendingAttestation(ctx context.Context, key pendingPayloadAttestationKey, msg *cltypes.PayloadAttestationMessage) (func(), bool) {
	if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.Data.Slot) {
		log.Trace("Pending payload attestation slot mismatch", "blockRoot", key.blockRoot)
		return nil, true
	}

	if _, ok := s.forkchoiceStore.GetHeader(key.blockRoot); !ok {
		return nil, false // Block still not here, keep waiting
	}

	return func() {
		if err := s.ProcessMessage(ctx, nil, msg); err != nil {
			log.Trace("Failed to process pending payload attestation", "blockRoot", key.blockRoot, "err", err)
		}
	}, true
}
