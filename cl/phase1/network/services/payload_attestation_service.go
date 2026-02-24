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
	"sync"
	"sync/atomic"
	"time"

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

// pendingPayloadAttestationJob represents a pending attestation waiting for its block.
type pendingPayloadAttestationJob struct {
	msg          *cltypes.PayloadAttestationMessage
	creationTime time.Time
}

const (
	// seenPayloadAttestationCacheSize: PTC has 512 validators per slot.
	// With clock disparity, we may see attestations for ~2 slots.
	// 512 * 4 = 2048 provides safety margin.
	seenPayloadAttestationCacheSize        = 2048
	pendingPayloadAttestationExpiry        = 30 * time.Second
	pendingPayloadAttestationCheckInterval = 100 * time.Millisecond
)

type payloadAttestationService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	ethClock        eth_clock.EthereumClock
	netCfg          *clparams.NetworkConfig

	// Cache to track seen attestations: (slot, validatorIndex) -> struct{}
	seenAttestationsCache *lru.Cache[seenPayloadAttestationKey, struct{}]

	// Pending attestations waiting for block to arrive
	pendingAttestations sync.Map // pendingPayloadAttestationKey -> *pendingPayloadAttestationJob
	pendingCount        atomic.Int32
	pendingCond         *sync.Cond
}

// NewPayloadAttestationService creates a new payload attestation service.
// [New in Gloas:EIP7732]
func NewPayloadAttestationService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	ethClock eth_clock.EthereumClock,
	netCfg *clparams.NetworkConfig,
) PayloadAttestationService {
	seenCache, err := lru.New[seenPayloadAttestationKey, struct{}]("seen_payload_attestations", seenPayloadAttestationCacheSize)
	if err != nil {
		panic(err)
	}
	s := &payloadAttestationService{
		forkchoiceStore:       forkchoiceStore,
		ethClock:              ethClock,
		netCfg:                netCfg,
		seenAttestationsCache: seenCache,
		pendingCond:           sync.NewCond(&sync.Mutex{}),
	}
	go s.loop(ctx)
	return s
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

	log.Debug("Received payload attestation message via gossip",
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
	if _, ok := s.forkchoiceStore.GetHeader(blockRoot); !ok {
		// Block hasn't arrived yet, queue attestation for later processing
		s.queuePendingAttestation(blockRoot, msg)
		log.Debug("Queued payload attestation for later processing",
			"blockRoot", blockRoot,
			"validatorIndex", validatorIndex)
		return nil
	}

	// Process through forkchoice which handles:
	// [IGNORE] block state not found, slot mismatch
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

	log.Debug("Processed payload attestation message via gossip",
		"slot", slot,
		"validatorIndex", validatorIndex,
		"blockRoot", blockRoot,
		"payloadPresent", data.PayloadPresent,
		"blobDataAvailable", data.BlobDataAvailable)

	return nil
}

// queuePendingAttestation adds an attestation to the pending queue for later processing.
func (s *payloadAttestationService) queuePendingAttestation(blockRoot common.Hash, msg *cltypes.PayloadAttestationMessage) {
	key := pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: msg.ValidatorIndex,
	}

	// Only add if not already present
	if _, loaded := s.pendingAttestations.LoadOrStore(key, &pendingPayloadAttestationJob{
		msg:          msg,
		creationTime: time.Now(),
	}); !loaded {
		s.pendingCount.Add(1)
		s.pendingCond.Signal()
	}
}

// loop is the background goroutine that processes pending attestations.
func (s *payloadAttestationService) loop(ctx context.Context) {
	for {
		// Wait until there are pending attestations
		s.pendingCond.L.Lock()
		for s.pendingCount.Load() == 0 {
			select {
			case <-ctx.Done():
				s.pendingCond.L.Unlock()
				return
			default:
			}
			s.pendingCond.Wait()
		}
		s.pendingCond.L.Unlock()

		// Poll until all pending attestations are processed
		ticker := time.NewTicker(pendingPayloadAttestationCheckInterval)
		for s.pendingCount.Load() > 0 {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				s.processPendingAttestations(ctx)
			}
		}
		ticker.Stop()
	}
}

// processPendingAttestations checks and processes any pending attestations whose blocks have arrived.
func (s *payloadAttestationService) processPendingAttestations(ctx context.Context) {
	s.pendingAttestations.Range(func(key, value any) bool {
		pendingKey := key.(pendingPayloadAttestationKey)
		job := value.(*pendingPayloadAttestationJob)

		// Check expiry
		if time.Since(job.creationTime) > pendingPayloadAttestationExpiry {
			s.pendingAttestations.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Debug("Pending payload attestation expired", "blockRoot", pendingKey.blockRoot)
			return true
		}

		// Check if attestation is still for current slot (with clock disparity allowance)
		if !s.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(job.msg.Data.Slot) {
			s.pendingAttestations.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Debug("Pending payload attestation slot mismatch", "blockRoot", pendingKey.blockRoot)
			return true
		}

		// Check if block has arrived
		if _, ok := s.forkchoiceStore.GetHeader(pendingKey.blockRoot); !ok {
			return true // Block still not here, keep waiting
		}

		// Block arrived, remove from pending and process
		s.pendingAttestations.Delete(pendingKey)
		s.pendingCount.Add(-1)

		// Re-run validation via ProcessMessage
		if err := s.ProcessMessage(ctx, nil, job.msg); err != nil {
			log.Debug("Failed to process pending payload attestation", "blockRoot", pendingKey.blockRoot, "err", err)
		}
		return true
	})
}
