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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
	messageRoot    common.Hash
}

type validatedRESTPayloadAttestation struct {
	mu           sync.Mutex
	messageRoot  common.Hash
	creationTime time.Time
	refs         int
	validated    atomic.Bool
}

const (
	// seenPayloadAttestationCacheSize: PTC has 512 validators per slot.
	// With clock disparity, we may see attestations for ~2 slots.
	// 512 * 4 = 2048 provides safety margin.
	seenPayloadAttestationCacheSize            = 2048
	pendingPayloadAttestationExpiry            = 30 * time.Second
	pendingPayloadAttestationCheckInterval     = 100 * time.Millisecond
	maxPendingAttestations                     = 2048
	maxConcurrentPayloadAttestationValidations = clparams.PtcSize
)

type payloadAttestationService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	ethClock        eth_clock.EthereumClock
	netCfg          *clparams.NetworkConfig
	emitters        *beaconevents.EventEmitter
	epbsPool        *pool.EpbsPool

	// Cache to track seen attestations: (slot, validatorIndex) -> struct{}
	seenAttestationsCache *lru.Cache[seenPayloadAttestationKey, struct{}]

	// Pending attestations waiting for block to arrive.
	pending             *pendingJobQueue[pendingPayloadAttestationKey, *cltypes.PayloadAttestationMessage]
	validationAdmission chan struct{}
	validatedRESTMu     sync.Mutex
	validatedREST       map[seenPayloadAttestationKey]*validatedRESTPayloadAttestation
	now                 func() time.Time
}

// NewPayloadAttestationService creates a new payload attestation service.
// [New in Gloas:EIP7732]
func NewPayloadAttestationService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	ethClock eth_clock.EthereumClock,
	netCfg *clparams.NetworkConfig,
	epbsPool *pool.EpbsPool,
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
		epbsPool:              epbsPool,
		emitters:              emitters,
		seenAttestationsCache: seenCache,
		validationAdmission:   make(chan struct{}, maxConcurrentPayloadAttestationValidations),
		validatedREST:         make(map[seenPayloadAttestationKey]*validatedRESTPayloadAttestation),
		now:                   time.Now,
	}
	s.pending = s.newPendingQueue()
	go s.pending.loop(ctx)
	return s
}

func (s *payloadAttestationService) newPendingQueue() *pendingJobQueue[pendingPayloadAttestationKey, *cltypes.PayloadAttestationMessage] {
	return newPendingJobQueue(maxPendingAttestations, pendingPayloadAttestationExpiry, pendingPayloadAttestationCheckInterval,
		s.tryProcessPendingAttestation,
		func(key pendingPayloadAttestationKey, _ *cltypes.PayloadAttestationMessage) {
			log.Trace("Pending payload attestation expired", "blockRoot", key.blockRoot)
		})
}

func (s *payloadAttestationService) Names() []string {
	return []string{gossip.TopicNamePayloadAttestation}
}

func (s *payloadAttestationService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.PayloadAttestationMessage, error) {
	msg := &cltypes.PayloadAttestationMessage{}
	if err := msg.DecodeSSZStrict(data, int(version)); err != nil {
		return nil, err
	}
	return msg, nil
}

// ProcessMessage processes a payload attestation message according to the gossip spec.
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/_features/epbs/p2p-interface.md#payload_attestation_message
// [New in Gloas:EIP7732]
func (s *payloadAttestationService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.PayloadAttestationMessage) error {
	return s.processMessage(ctx, msg, false, nil)
}

func (s *payloadAttestationService) ProcessRESTMessage(ctx context.Context, msg *cltypes.PayloadAttestationMessage, publish func() error) error {
	return s.processMessage(ctx, msg, false, publish)
}

var (
	ErrAttestationDuplicate = errors.New("payload attestation duplicate")
	ErrAttestationRetryable = errors.New("payload attestation retryable")
	ErrAttestationConflict  = errors.New("payload attestation conflicts with validated message")
	ErrAttestationCapacity  = errors.New("validated payload attestation retry capacity reached")
)

func (s *payloadAttestationService) processMessage(ctx context.Context, msg *cltypes.PayloadAttestationMessage, queueMissing bool, publish func() error) error {
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

	if !isPayloadAttestationSlotCurrent(s.ethClock, s.now(), slot) {
		return fmt.Errorf("%w: payload attestation slot %d is not current slot (with clock disparity)", ErrIgnore, slot)
	}

	// [IGNORE] The payload_attestation_message is the first valid message received from the validator
	seenKey := seenPayloadAttestationKey{
		slot:           slot,
		validatorIndex: validatorIndex,
	}
	if s.seenAttestationsCache.Contains(seenKey) {
		return fmt.Errorf("%w: %w: already seen payload attestation from validator %d for slot %d", ErrIgnore, ErrAttestationDuplicate, validatorIndex, slot)
	}

	// [IGNORE] The message's block root has been seen (via gossip or non-gossip sources)
	// A client MAY queue attestation for processing once the block is retrieved.
	blockHeader, ok := s.forkchoiceStore.GetHeader(blockRoot)
	if !ok {
		if !queueMissing {
			return fmt.Errorf("%w: block not available", ErrIgnore)
		}
		if !s.queuePendingAttestation(blockRoot, msg) {
			return fmt.Errorf("%w: %w: block not available", ErrIgnore, ErrAttestationCapacity)
		}
		log.Trace("Queued payload attestation for later processing",
			"blockRoot", blockRoot,
			"validatorIndex", validatorIndex)
		return fmt.Errorf("%w: %w: block not available", ErrIgnore, ErrAttestationQueued)
	}
	// [IGNORE] The block referenced by data.beacon_block_root is at data.slot.
	if blockHeader.Slot != slot {
		return fmt.Errorf("%w: payload attestation slot %d does not match referenced block slot %d", ErrIgnore, slot, blockHeader.Slot)
	}
	messageRoot, err := msg.HashSSZ()
	if err != nil {
		return fmt.Errorf("hash payload attestation: %w", err)
	}
	if publish == nil {
		coordinator := s.acquireExistingRESTAttestation(seenKey, messageRoot)
		if coordinator != nil {
			coordinator.mu.Lock()
			if coordinator.validated.Load() {
				if s.seenAttestationsCache.Contains(seenKey) {
					coordinator.mu.Unlock()
					s.releaseValidatedRESTAttestation(seenKey, coordinator, false)
					return fmt.Errorf("%w: %w: already seen payload attestation from validator %d for slot %d", ErrIgnore, ErrAttestationDuplicate, validatorIndex, slot)
				}
				s.commitPayloadAttestation(seenKey, msg)
				coordinator.mu.Unlock()
				s.releaseValidatedRESTAttestation(seenKey, coordinator, false)
				return nil
			}
			coordinator.mu.Unlock()
			s.releaseValidatedRESTAttestation(seenKey, coordinator, false)
		}
		if err := s.validatePayloadAttestation(ctx, msg); err != nil {
			return err
		}
		s.commitPayloadAttestation(seenKey, msg)
		return nil
	}
	coordinator, err := s.acquireValidatedRESTAttestation(seenKey, messageRoot)
	if err != nil {
		return err
	}
	retainCoordinator := false
	defer func() { s.releaseValidatedRESTAttestation(seenKey, coordinator, retainCoordinator) }()
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if s.seenAttestationsCache.Contains(seenKey) {
		return fmt.Errorf("%w: %w: already seen payload attestation from validator %d for slot %d", ErrIgnore, ErrAttestationDuplicate, validatorIndex, slot)
	}
	if !coordinator.validated.Load() {
		if err := s.validatePayloadAttestation(ctx, msg); err != nil {
			return err
		}
		coordinator.validated.Store(true)
	}
	if err := publish(); err != nil {
		retainCoordinator = true
		return err
	}
	s.commitPayloadAttestation(seenKey, msg)
	return nil
}

func isPayloadAttestationSlotCurrent(clock eth_clock.EthereumClock, now time.Time, slot uint64) bool {
	if slot == math.MaxUint64 {
		return false
	}
	slotStart := clock.GetSlotTime(slot)
	nextSlotStart := clock.GetSlotTime(slot + 1)
	slotUnix := slotStart.Unix()
	nextSlotUnix := nextSlotStart.Unix()
	if slotUnix < 0 || nextSlotUnix <= slotUnix {
		return false
	}
	secondsPerSlot := uint64(nextSlotUnix - slotUnix)
	genesisTime := clock.GenesisTime()
	if genesisTime > math.MaxInt64 || slot > (math.MaxUint64-genesisTime)/secondsPerSlot {
		return false
	}
	expectedSlotUnix := genesisTime + slot*secondsPerSlot
	if expectedSlotUnix > math.MaxInt64 || slotUnix != int64(expectedSlotUnix) || uint64(nextSlotUnix)-expectedSlotUnix != secondsPerSlot {
		return false
	}
	lowerBound := slotStart.Add(-gloasMaximumClockDisparity)
	upperBound := nextSlotStart.Add(gloasMaximumClockDisparity)
	if lowerBound.After(slotStart) || upperBound.Before(nextSlotStart) {
		return false
	}
	return !now.Before(lowerBound) && !now.After(upperBound)
}

func (s *payloadAttestationService) validatePayloadAttestation(ctx context.Context, msg *cltypes.PayloadAttestationMessage) error {
	select {
	case s.validationAdmission <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("%w: %w: payload attestation validation canceled: %v", ErrIgnore, ErrAttestationRetryable, ctx.Err()) //nolint:errorlint // converting cancellation to IGNORE
	}
	defer func() { <-s.validationAdmission }()
	err := s.forkchoiceStore.OnPayloadAttestationMessage(ctx, msg, false)
	if err == nil {
		return nil
	}
	if errors.Is(err, forkchoice.ErrIgnore) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w: %v", ErrIgnore, ErrAttestationRetryable, err) //nolint:errorlint // converting forkchoice errors to gossip outcomes
	}
	return fmt.Errorf("forkchoice rejected payload attestation: %w", err)
}

func (s *payloadAttestationService) commitPayloadAttestation(seenKey seenPayloadAttestationKey, msg *cltypes.PayloadAttestationMessage) {
	s.seenAttestationsCache.Add(seenKey, struct{}{})
	if s.epbsPool != nil {
		s.epbsPool.PayloadAttestations.Add(pool.PayloadAttestationKey{Slot: seenKey.slot, ValidatorIndex: seenKey.validatorIndex}, msg)
	}
	s.emitters.Operation().SendPayloadAttestationMessage(msg)
}

func (s *payloadAttestationService) acquireValidatedRESTAttestation(key seenPayloadAttestationKey, messageRoot common.Hash) (*validatedRESTPayloadAttestation, error) {
	s.validatedRESTMu.Lock()
	defer s.validatedRESTMu.Unlock()
	if s.validatedREST == nil {
		s.validatedREST = make(map[seenPayloadAttestationKey]*validatedRESTPayloadAttestation)
	}
	now := time.Now()
	for candidateKey, candidate := range s.validatedREST {
		if candidate.refs == 0 && now.Sub(candidate.creationTime) > pendingPayloadAttestationExpiry {
			delete(s.validatedREST, candidateKey)
		}
	}
	if existing := s.validatedREST[key]; existing != nil {
		if existing.messageRoot != messageRoot {
			return nil, fmt.Errorf("%w for slot %d validator %d", ErrAttestationConflict, key.slot, key.validatorIndex)
		}
		existing.refs++
		return existing, nil
	}
	if len(s.validatedREST) >= maxPendingAttestations {
		return nil, ErrAttestationCapacity
	}
	entry := &validatedRESTPayloadAttestation{messageRoot: messageRoot, creationTime: now, refs: 1}
	s.validatedREST[key] = entry
	return entry, nil
}

func (s *payloadAttestationService) acquireExistingRESTAttestation(key seenPayloadAttestationKey, messageRoot common.Hash) *validatedRESTPayloadAttestation {
	s.validatedRESTMu.Lock()
	defer s.validatedRESTMu.Unlock()
	entry := s.validatedREST[key]
	if entry == nil || entry.messageRoot != messageRoot {
		return nil
	}
	entry.refs++
	return entry
}

func (s *payloadAttestationService) releaseValidatedRESTAttestation(key seenPayloadAttestationKey, entry *validatedRESTPayloadAttestation, retain bool) {
	s.validatedRESTMu.Lock()
	defer s.validatedRESTMu.Unlock()
	entry.refs--
	if current := s.validatedREST[key]; current == entry && entry.refs == 0 && !retain {
		delete(s.validatedREST, key)
	}
}

// queuePendingAttestation adds an attestation to the pending queue for later processing.
func (s *payloadAttestationService) queuePendingAttestation(blockRoot common.Hash, msg *cltypes.PayloadAttestationMessage) bool {
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	if _, loaded := s.pending.jobs.Load(key); loaded {
		return true
	}
	if !s.pending.reserve() {
		if _, loaded := s.pending.jobs.Load(key); loaded {
			return true
		}
		return false
	}
	s.pending.storeReserved(key, msg)
	return true
}

func pendingPayloadAttestationKeyFor(blockRoot common.Hash, msg *cltypes.PayloadAttestationMessage) pendingPayloadAttestationKey {
	root, _ := msg.HashSSZ()
	return pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: msg.ValidatorIndex,
		messageRoot:    common.Hash(root),
	}
}

// tryProcessPendingAttestation re-runs validation via ProcessMessage once the block has arrived,
// dropping attestations that are no longer for the current slot.
func (s *payloadAttestationService) tryProcessPendingAttestation(ctx context.Context, key pendingPayloadAttestationKey, msg *cltypes.PayloadAttestationMessage) (func(), bool) {
	if !isPayloadAttestationSlotCurrent(s.ethClock, s.now(), msg.Data.Slot) {
		log.Trace("Pending payload attestation slot mismatch", "blockRoot", key.blockRoot)
		return nil, true
	}

	if _, ok := s.forkchoiceStore.GetHeader(key.blockRoot); !ok {
		return nil, false
	}

	err := s.processMessage(ctx, msg, false, nil)
	if errors.Is(err, ErrAttestationRetryable) {
		return nil, false
	}
	if err != nil {
		log.Trace("Failed to process pending payload attestation", "blockRoot", key.blockRoot, "err", err)
	}
	return nil, true
}
