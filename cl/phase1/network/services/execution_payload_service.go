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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossipmgr "github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seenEnvelopeKey tracks validated envelopes per (beaconBlockRoot, builderIndex).
type seenEnvelopeKey struct {
	beaconBlockRoot common.Hash
	builderIndex    uint64
}

// pendingEnvelopeKey keeps distinct signed envelopes for a block separate until
// validation, so a forged envelope cannot suppress a valid one.
type pendingEnvelopeKey struct {
	blockRoot    common.Hash
	envelopeHash common.Hash
}

type pendingEnvelopeJob struct {
	envelope              *cltypes.SignedExecutionPayloadEnvelope
	ownedBytes            uint64
	receivedAt            time.Time
	processing            atomic.Bool
	finishPendingEnvelope func()
}

type pendingEnvelopeLookupRetry struct {
	owner   *pendingEnvelopeJob
	retryAt int64
}

const (
	seenEnvelopeCacheSize              = 1000
	pendingEnvelopeExpiry              = 30 * time.Second
	pendingEnvelopeCheckInterval       = 100 * time.Millisecond
	pendingEnvelopeLookupRetryInterval = 5 * time.Second
	maxPendingEnvelopes                = 1024
	maxPendingEnvelopeBytes            = 4 * clparams.MaxChunkSize
)

var errEnvelopeBlockUnavailable = errors.New("execution payload envelope block unavailable")

type executionPayloadService struct {
	ctx             context.Context
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter
	gossipManager   gossipmgr.Gossip

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for block to arrive
	pending              *pendingJobQueue[pendingEnvelopeKey, *pendingEnvelopeJob]
	pendingBytes         atomic.Uint64
	pendingMu            sync.Mutex
	pendingLookupRetryAt sync.Map
	now                  func() time.Time
}

// NewExecutionPayloadService creates a new execution payload service
func NewExecutionPayloadService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	beaconCfg *clparams.BeaconChainConfig,
	emitters *beaconevents.EventEmitter,
	gossipManager gossipmgr.Gossip,
) ExecutionPayloadService {
	seenEnvelopesCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	if err != nil {
		panic(err)
	}
	s := &executionPayloadService{
		ctx:                ctx,
		forkchoiceStore:    forkchoiceStore,
		beaconCfg:          beaconCfg,
		emitters:           emitters,
		gossipManager:      gossipManager,
		seenEnvelopesCache: seenEnvelopesCache,
		now:                time.Now,
	}
	s.pending = s.newPendingQueue(ctx)
	return s
}

func (s *executionPayloadService) newPendingQueue(ctx context.Context) *pendingJobQueue[pendingEnvelopeKey, *pendingEnvelopeJob] {
	return newPendingJobQueue(ctx, pendingJobQueueOptions{
		name:          "execution_payload_envelope",
		capacity:      maxPendingEnvelopes,
		expiry:        pendingEnvelopeExpiry,
		checkInterval: pendingEnvelopeCheckInterval,
	},
		s.tryProcessPendingEnvelope,
		func(_ context.Context, _ pendingEnvelopeKey, job *pendingEnvelopeJob) {
			s.releasePendingEnvelope(job)
		},
		func(key pendingEnvelopeKey, job *pendingEnvelopeJob) {
			s.releasePendingEnvelope(job)
			if job.envelope != nil && job.envelope.Message != nil {
				seenKey := seenEnvelopeKey{key.blockRoot, job.envelope.Message.BuilderIndex}
				s.clearPendingLookupRetryIfUnused(seenKey)
			}
			log.Trace("Pending envelope expired", "blockRoot", key.blockRoot)
		})
}

func (s *executionPayloadService) clearPendingLookupRetryIfUnused(seenKey seenEnvelopeKey) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	identityPending := false
	s.pending.jobs.Range(func(_, value any) bool {
		job := value.(*pendingJob[*pendingEnvelopeJob]).msg
		if job.envelope != nil && job.envelope.Message != nil &&
			job.envelope.Message.BeaconBlockRoot == seenKey.beaconBlockRoot &&
			job.envelope.Message.BuilderIndex == seenKey.builderIndex {
			identityPending = true
			return false
		}
		return true
	})
	if identityPending {
		return
	}
	if retry, ok := s.pendingLookupRetryAt.Load(seenKey); ok {
		s.pendingLookupRetryAt.CompareAndDelete(seenKey, retry)
	}
}

func (s *executionPayloadService) Names() []string {
	return []string{gossip.TopicNameExecutionPayload}
}

func (s *executionPayloadService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameExecutionPayload
}

func (s *executionPayloadService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if err := cltypes.ValidateExecutionPayloadEnvelopeVersion(version); err != nil {
		return nil, err
	}
	obj := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelopeWithVersion(s.beaconCfg, version),
	}
	if err := obj.DecodeSSZStrict(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

// ProcessMessage processes an execution payload envelope according to the gossip spec.
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/_features/epbs/p2p-interface.md#execution_payload
// [New in Gloas:EIP7732]
func (s *executionPayloadService) ProcessMessage(ctx context.Context, _ *uint64, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	receivedAt := time.Now()
	if s.now != nil {
		receivedAt = s.now()
	}
	err := s.processMessage(ctx, signedEnvelope, receivedAt, nil)
	if errors.Is(err, errEnvelopeBlockUnavailable) || errors.Is(err, forkchoice.ErrIgnore) ||
		errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeAdmissionBusy) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrIgnore, err) //nolint:errorlint // converting, not wrapping: the forkchoice sentinels must not stay matchable
	}
	return err
}

func (s *executionPayloadService) processMessage(
	ctx context.Context,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	receivedAt time.Time,
	preclaimedAdmission *forkchoice.ExecutionPayloadEnvelopeAdmissionToken,
) error {
	var (
		admissionToken forkchoice.ExecutionPayloadEnvelopeAdmissionToken
		hasAdmission   bool
	)
	if preclaimedAdmission != nil {
		admissionToken = *preclaimedAdmission
		hasAdmission = true
	}
	defer func() {
		if hasAdmission {
			s.forkchoiceStore.FinishExecutionPayloadEnvelopeForGossip(admissionToken, false)
		}
	}()
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("nil execution payload envelope")
	}
	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot
	builderIndex := envelope.BuilderIndex
	block, blockKnown := s.forkchoiceStore.GetBlock(beaconBlockRoot)

	log.Trace("Received execution payload via gossip",
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)
	if err := validateEnvelopeLimits(s.beaconCfg, envelope); err != nil {
		if !blockKnown || block == nil {
			return fmt.Errorf("%w: invalid execution payload envelope for unknown block: %w", ErrIgnore, err)
		}
		return err
	}
	if err := signedEnvelope.ValidateForConfig(s.beaconCfg); err != nil {
		if !blockKnown || block == nil {
			return fmt.Errorf("%w: invalid execution payload envelope for unknown block: %w", ErrIgnore, err)
		}
		return fmt.Errorf("invalid execution payload envelope: %w", err)
	}
	if envelope.Payload == nil {
		return errors.New("nil execution payload")
	}
	finalizedSlot := s.forkchoiceStore.FinalizedCheckpoint().Epoch * s.beaconCfg.SlotsPerEpoch
	if envelope.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("%w: envelope slot %d < finalized slot %d", ErrIgnore, envelope.Payload.SlotNumber, finalizedSlot)
	}

	// [IGNORE] The envelope's block root has been seen (via gossip or non-gossip sources)
	// A client MAY queue payload for processing once the block is retrieved.
	if !blockKnown || block == nil {
		queued, err := s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope, receivedAt, true)
		if err != nil {
			return fmt.Errorf("%w: %w", errEnvelopeBlockUnavailable, err)
		}
		if !queued {
			return fmt.Errorf("%w: execution payload envelope already queued", errEnvelopeBlockUnavailable)
		}
		log.Trace("Queued execution payload envelope for later processing",
			"beaconBlockRoot", beaconBlockRoot,
			"builderIndex", builderIndex)
		return errEnvelopeBlockUnavailable
	}
	if block.Block == nil {
		return fmt.Errorf("%w: beacon block %v is incomplete", ErrIgnore, beaconBlockRoot)
	}
	if err := cltypes.ValidateExecutionPayloadEnvelopeBuilderIndex(block, signedEnvelope); err != nil {
		return fmt.Errorf("%w: %w", forkchoice.ErrInvalidExecutionPayloadEnvelope, err)
	}

	// [IGNORE] The node has not seen another valid SignedExecutionPayloadEnvelope
	// for this block root from this builder.
	seenKey := seenEnvelopeKey{
		beaconBlockRoot: beaconBlockRoot,
		builderIndex:    builderIndex,
	}
	if s.seenEnvelopesCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen envelope for block %v from builder %d", ErrIgnore, beaconBlockRoot, builderIndex)
	}

	var err error
	if !hasAdmission {
		admissionToken, err = s.forkchoiceStore.ClaimExecutionPayloadEnvelopeForGossip(ctx, beaconBlockRoot, builderIndex)
		if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeLookupRequired) {
			persisted, readErr := s.forkchoiceStore.ReadEnvelopeFromDisk(beaconBlockRoot)
			if readErr == nil && persisted != nil && persisted.Message != nil {
				return fmt.Errorf("%w: %w", ErrIgnore, err)
			}
			s.forkchoiceStore.ForgetExecutionPayloadEnvelopeForGossip(beaconBlockRoot, builderIndex)
			admissionToken, err = s.forkchoiceStore.ClaimExecutionPayloadEnvelopeForGossip(ctx, beaconBlockRoot, builderIndex)
		}
		if err != nil {
			if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeAdmissionBusy) {
				queued, queueErr := s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope, receivedAt, false)
				if queueErr != nil {
					return fmt.Errorf("%w: %w", ErrIgnore, queueErr)
				}
				if !queued {
					return fmt.Errorf("%w: execution payload envelope already queued", forkchoice.ErrExecutionPayloadEnvelopeAdmissionBusy)
				}
				return err
			}
			if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeLookupRequired) {
				s.forkchoiceStore.ForgetExecutionPayloadEnvelopeForGossip(beaconBlockRoot, builderIndex)
			}
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		hasAdmission = true
	}

	if err := s.forkchoiceStore.ValidateExecutionPayloadEnvelopeForGossip(signedEnvelope); err != nil {
		if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeStateUnavailable) {
			s.forkchoiceStore.QueuePendingExecutionPayloadEnvelope(signedEnvelope, receivedAt)
		}
		if errors.Is(err, forkchoice.ErrIgnore) {
			return err
		}
		return fmt.Errorf("%w: %w", forkchoice.ErrInvalidExecutionPayloadEnvelope, err)
	}
	s.forkchoiceStore.RecordExecutionPayloadArrival(beaconBlockRoot, envelope.Payload.SlotNumber, receivedAt)
	finishPendingEnvelope := s.forkchoiceStore.BeginPendingPayloadEnvelope(beaconBlockRoot)
	s.seenEnvelopesCache.Add(seenKey, struct{}{})
	handedOff := false
	defer func() {
		if !handedOff {
			s.seenEnvelopesCache.Remove(seenKey)
			finishPendingEnvelope()
		}
	}()
	// importEnvelope now owns the admission token, the seen entry and the hold.
	if preclaimedAdmission != nil {
		handedOff, hasAdmission = true, false
		if err := s.importEnvelope(signedEnvelope, receivedAt, seenKey, admissionToken, finishPendingEnvelope); err != nil {
			return err
		}
		s.emitExecutionPayloadGossip(block, envelope)
		return nil
	}
	s.emitExecutionPayloadGossip(block, envelope)
	handedOff, hasAdmission = true, false
	go func() {
		_ = s.importEnvelope(signedEnvelope, receivedAt, seenKey, admissionToken, finishPendingEnvelope)
	}()

	return nil
}

func (s *executionPayloadService) importEnvelope(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	receivedAt time.Time,
	seenKey seenEnvelopeKey,
	token forkchoice.ExecutionPayloadEnvelopeAdmissionToken,
	finishPendingEnvelope func(),
) (importErr error) {
	keepSeen := false
	defer func() {
		if r := recover(); r != nil {
			log.Warn("Recovered from panicking execution payload envelope import", "beaconBlockRoot", seenKey.beaconBlockRoot, "builderIndex", seenKey.builderIndex, "panic", r)
			importErr = fmt.Errorf("execution payload envelope import panicked: %v", r)
		}
		if !keepSeen {
			// Pubsub drops identical redeliveries, so transient failures must be unseen for a later copy to retry.
			s.seenEnvelopesCache.Remove(seenKey)
		}
		s.forkchoiceStore.FinishExecutionPayloadEnvelopeForGossip(token, keepSeen)
		finishPendingEnvelope()
	}()
	var err error
	if store, ok := s.forkchoiceStore.(interface {
		OnExecutionPayloadAt(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool, time.Time) error
	}); ok {
		err = store.OnExecutionPayloadAt(s.ctx, signedEnvelope, true, true, receivedAt)
	} else {
		err = s.forkchoiceStore.OnExecutionPayload(s.ctx, signedEnvelope, true, true)
	}
	keepSeen = err == nil ||
		errors.Is(err, forkchoice.ErrInvalidExecutionPayloadEnvelope) ||
		errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
		s.forkchoiceStore.HasEnvelope(seenKey.beaconBlockRoot)
	if err != nil {
		log.Debug("Failed to import execution payload envelope", "beaconBlockRoot", seenKey.beaconBlockRoot, "builderIndex", seenKey.builderIndex, "err", err)
	}
	if keepSeen || errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed) {
		return nil
	}
	return err
}

func validateEnvelopeLimits(cfg *clparams.BeaconChainConfig, envelope *cltypes.ExecutionPayloadEnvelope) error {
	if cfg == nil || envelope == nil || envelope.Payload == nil {
		return errors.New("missing execution payload envelope fields")
	}
	if err := validateExecutionRequestsLimits(cfg, envelope.ExecutionRequests); err != nil {
		return err
	}
	if envelope.Payload.Withdrawals == nil {
		return errors.New("missing payload withdrawals")
	}
	if uint64(envelope.Payload.Withdrawals.Len()) > cfg.MaxWithdrawalsPerPayload {
		return fmt.Errorf("payload withdrawals count %d exceeds limit %d", envelope.Payload.Withdrawals.Len(), cfg.MaxWithdrawalsPerPayload)
	}
	return nil
}

func (s *executionPayloadService) emitExecutionPayloadGossip(block *cltypes.SignedBeaconBlock, envelope *cltypes.ExecutionPayloadEnvelope) {
	s.emitters.Operation().SendExecutionPayloadGossip(&beaconevents.ExecutionPayloadGossipData{
		Slot:         block.Block.Slot,
		BuilderIndex: envelope.BuilderIndex,
		BlockHash:    envelope.Payload.BlockHash,
		BlockRoot:    envelope.BeaconBlockRoot,
	})
}

func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, receivedAt time.Time, holdPreparation bool) (bool, error) {
	envelopeHash, err := envelope.HashSSZ()
	if err != nil {
		return false, fmt.Errorf("failed to hash envelope for pending queue: %w", err)
	}

	key := pendingEnvelopeKey{
		blockRoot:    blockRoot,
		envelopeHash: envelopeHash,
	}
	ownedBytes := envelope.EncodingSizeSSZ()
	if ownedBytes <= 0 {
		return false, errors.New("invalid pending execution payload envelope size")
	}
	ownedBytes64 := uint64(ownedBytes)

	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, loaded := s.pending.jobs.Load(key); loaded {
		return false, nil
	}
	currentBytes := s.pendingBytes.Load()
	if ownedBytes64 > maxPendingEnvelopeBytes || currentBytes > maxPendingEnvelopeBytes-ownedBytes64 {
		return false, errors.New("pending execution payload envelope capacity reached")
	}
	if !s.pending.reserve() {
		return false, fmt.Errorf("pending execution payload envelope capacity reached: %w", errPendingJobQueueFull)
	}
	s.pendingBytes.Store(currentBytes + ownedBytes64)
	job := &pendingEnvelopeJob{
		envelope:   envelope,
		ownedBytes: ownedBytes64,
		receivedAt: receivedAt,
	}
	if holdPreparation {
		job.finishPendingEnvelope = s.forkchoiceStore.BeginPendingPayloadEnvelope(blockRoot)
	}
	s.pending.storeReserved(key, job)
	return true, nil
}

func (s *executionPayloadService) releasePendingEnvelope(job *pendingEnvelopeJob) {
	if job.finishPendingEnvelope != nil {
		job.finishPendingEnvelope()
	}
	s.releasePendingEnvelopeBytes(job.ownedBytes)
}

func (s *executionPayloadService) releasePendingEnvelopeBytes(ownedBytes uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	currentBytes := s.pendingBytes.Load()
	if ownedBytes >= currentBytes {
		s.pendingBytes.Store(0)
	} else {
		s.pendingBytes.Store(currentBytes - ownedBytes)
	}
}

// tryProcessPendingEnvelope retains queue ownership until validation finishes or forkchoice takes over.
func (s *executionPayloadService) tryProcessPendingEnvelope(ctx context.Context, key pendingEnvelopeKey, job *pendingEnvelopeJob) pendingJobDecision {
	block, ok := s.forkchoiceStore.GetBlock(key.blockRoot)
	if !ok || block == nil || block.Block == nil || !job.processing.CompareAndSwap(false, true) {
		return pendingJobKeep
	}
	if job.envelope == nil || job.envelope.Message == nil || job.envelope.Message.BeaconBlockRoot != key.blockRoot {
		return pendingJobRemoveThenProcess
	}
	if err := cltypes.ValidateExecutionPayloadEnvelopeBuilderIndex(block, job.envelope); err != nil {
		return pendingJobRemoveThenProcess
	}
	seenKey := seenEnvelopeKey{key.blockRoot, job.envelope.Message.BuilderIndex}
	if s.seenEnvelopesCache.Contains(seenKey) {
		s.pendingLookupRetryAt.Delete(seenKey)
		return pendingJobRemoveThenProcess
	}
	if retryAt, ok := s.pendingLookupRetryAt.Load(seenKey); ok {
		if retryAt.(pendingEnvelopeLookupRetry).retryAt > time.Now().UnixNano() && s.forkchoiceStore.HasEnvelope(key.blockRoot) {
			job.processing.Store(false)
			return pendingJobKeep
		}
		s.pendingLookupRetryAt.Delete(seenKey)
	}
	admissionToken, err := s.forkchoiceStore.TryClaimExecutionPayloadEnvelopeForGossip(
		key.blockRoot,
		job.envelope.Message.BuilderIndex,
	)
	if err != nil {
		if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeAdmissionBusy) {
			job.processing.Store(false)
			return pendingJobKeep
		}
		if errors.Is(err, forkchoice.ErrExecutionPayloadEnvelopeLookupRequired) {
			persisted, readErr := s.forkchoiceStore.ReadEnvelopeFromDisk(key.blockRoot)
			if readErr == nil && persisted != nil && persisted.Message != nil {
				s.pendingLookupRetryAt.Delete(seenKey)
				s.seenEnvelopesCache.Add(seenKey, struct{}{})
				return pendingJobRemoveThenProcess
			}
			s.forkchoiceStore.ForgetExecutionPayloadEnvelopeForGossip(key.blockRoot, job.envelope.Message.BuilderIndex)
			s.pendingLookupRetryAt.Store(seenKey, pendingEnvelopeLookupRetry{
				owner:   job,
				retryAt: time.Now().Add(pendingEnvelopeLookupRetryInterval).UnixNano(),
			})
			job.processing.Store(false)
			return pendingJobKeep
		}
		return pendingJobRemoveThenProcess
	}
	err = s.processMessage(ctx, job.envelope, job.receivedAt, &admissionToken)
	if err != nil {
		log.Trace("Failed to process pending envelope", "blockRoot", key.blockRoot, "err", err)
		if !errors.Is(err, ErrIgnore) && !errors.Is(err, forkchoice.ErrIgnore) &&
			!errors.Is(err, forkchoice.ErrInvalidExecutionPayloadEnvelope) {
			job.processing.Store(false)
			return pendingJobKeep
		}
	}
	if err == nil {
		s.publishPendingEnvelope(job.envelope)
	}
	return pendingJobRemoveThenProcess
}

// publishPendingEnvelope relays an envelope that gossip ignored, and so did not forward, while its block was unknown.
func (s *executionPayloadService) publishPendingEnvelope(envelope *cltypes.SignedExecutionPayloadEnvelope) {
	if s.gossipManager == nil {
		return
	}
	encoded, err := envelope.EncodeSSZ(nil)
	if err != nil {
		log.Debug("Failed to encode pending execution payload envelope", "beaconBlockRoot", envelope.Message.BeaconBlockRoot, "err", err)
		return
	}
	// PublishBackground logs its own admission failures.
	_ = s.gossipManager.PublishBackground(gossip.TopicNameExecutionPayload, encoded, time.Time{})
}
