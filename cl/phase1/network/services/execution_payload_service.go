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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seenEnvelopeKey tracks validated envelopes per (beaconBlockRoot, builderIndex).
type seenEnvelopeKey struct {
	beaconBlockRoot common.Hash
	builderIndex    uint64
}

// pendingEnvelopeKey retains distinct unvalidated candidates; DA-validated jobs use one entry per block root.
type pendingEnvelopeKey struct {
	blockRoot        common.Hash
	envelopeHash     common.Hash
	dataAvailability bool
}

// envelopeJob represents an envelope waiting for its block or PeerDAS data.
type envelopeJob struct {
	envelope     *cltypes.SignedExecutionPayloadEnvelope
	creationTime time.Time
	processing   bool
	recovered    atomic.Bool
	validate     atomic.Bool
	nextAttempt  time.Time
	retryDelay   time.Duration
}

const (
	seenEnvelopeCacheSize         = 1000
	pendingEnvelopeExpiry         = 30 * time.Second
	pendingDataAvailabilityExpiry = 2 * time.Minute
	pendingEnvelopeCheckInterval  = 100 * time.Millisecond
	pendingEnvelopeInitialRetry   = time.Second
	pendingEnvelopeMaxRetry       = 10 * time.Second
	maxPendingEnvelopes           = 1024
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// pendingMu keeps map membership and count changes atomic; retry timing is owned by loop.
	pendingEnvelopes sync.Map // pendingEnvelopeKey -> *envelopeJob
	pendingMu        sync.Mutex
	pendingCount     atomic.Int32
	pendingCond      *sync.Cond
}

// NewExecutionPayloadService creates a new execution payload service
func NewExecutionPayloadService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	beaconCfg *clparams.BeaconChainConfig,
	emitters *beaconevents.EventEmitter,
) ExecutionPayloadService {
	seenEnvelopesCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	if err != nil {
		panic(err)
	}
	s := &executionPayloadService{
		forkchoiceStore:    forkchoiceStore,
		beaconCfg:          beaconCfg,
		emitters:           emitters,
		seenEnvelopesCache: seenEnvelopesCache,
		pendingCond:        sync.NewCond(&sync.Mutex{}),
	}
	go s.loop(ctx)
	return s
}

func (s *executionPayloadService) Names() []string {
	return []string{gossip.TopicNameExecutionPayload}
}

func (s *executionPayloadService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameExecutionPayload
}

func (s *executionPayloadService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	obj := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(s.beaconCfg),
	}
	if err := obj.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

// ProcessMessage processes an execution payload envelope according to the gossip spec.
// Reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/_features/epbs/p2p-interface.md#execution_payload
// [New in Gloas:EIP7732]
func (s *executionPayloadService) ProcessMessage(ctx context.Context, _ *uint64, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	return s.processEnvelope(ctx, signedEnvelope, false, true, true)
}

func (s *executionPayloadService) ProcessRecoveredEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, validatePayload bool) error {
	return s.processEnvelope(ctx, signedEnvelope, true, validatePayload, true)
}

func (s *executionPayloadService) processEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, recovered, validatePayload, queueOnRetry bool) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("nil execution payload envelope")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot
	builderIndex := envelope.BuilderIndex

	log.Trace("Received execution payload via gossip",
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)

	// [IGNORE] The envelope's block root has been seen (via gossip or non-gossip sources)
	// A client MAY queue payload for processing once the block is retrieved.
	block, ok := s.forkchoiceStore.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		if queueOnRetry {
			s.queuePendingEnvelopeWithOptions(beaconBlockRoot, signedEnvelope, recovered, validatePayload, false)
		}
		if !recovered {
			s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, false, true)
		}
		log.Trace("Queued execution payload envelope for later processing",
			"beaconBlockRoot", beaconBlockRoot,
			"builderIndex", builderIndex)
		return ErrIgnore
	}
	if block.Block == nil {
		return errors.New("nil beacon block")
	}

	// [IGNORE] The node has not seen another valid SignedExecutionPayloadEnvelope
	// for this block root from this builder.
	seenKey := seenEnvelopeKey{
		beaconBlockRoot: beaconBlockRoot,
		builderIndex:    builderIndex,
	}
	if !recovered && s.seenEnvelopesCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen envelope for block %v from builder %d", ErrIgnore, beaconBlockRoot, builderIndex)
	}

	// [IGNORE] The envelope is from a slot greater than or equal to the latest finalized slot
	if !recovered {
		finalizedSlot := s.forkchoiceStore.FinalizedSlot()
		if block.Block.Slot < finalizedSlot {
			return fmt.Errorf("%w: envelope slot %d < finalized slot %d", ErrIgnore, block.Block.Slot, finalizedSlot)
		}
	}
	if !recovered && s.forkchoiceStore.HasEnvelope(beaconBlockRoot) {
		return storedEnvelopeResult(beaconBlockRoot, block, builderIndex)
	}

	// Process the execution payload through forkchoice
	// Note: bid matching and signature verification are done in OnExecutionPayload.validateEnvelopeAgainstBlock
	if err := s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, true, validatePayload); err != nil {
		if errors.Is(err, forkchoice.ErrExecutionPayloadAlreadyStored) {
			return storedEnvelopeResult(beaconBlockRoot, block, builderIndex)
		}
		if errors.Is(err, forkchoice.ErrIgnore) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			if queueOnRetry {
				s.queuePendingEnvelopeWithOptions(beaconBlockRoot, signedEnvelope, recovered, validatePayload, errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable))
			}
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		return fmt.Errorf("failed to process execution payload: %w", err)
	}

	s.markEnvelopeAvailable(seenKey, block.Block.Slot)

	log.Trace("Processed execution payload envelope",
		"slot", block.Block.Slot,
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)

	return nil
}

func (s *executionPayloadService) markEnvelopeAvailable(key seenEnvelopeKey, slot uint64) {
	if seen, _ := s.seenEnvelopesCache.ContainsOrAdd(key, struct{}{}); seen {
		return
	}
	s.emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{
		Slot:      slot,
		BlockRoot: key.beaconBlockRoot,
	})
}

func (s *executionPayloadService) accountStoredPendingEnvelope(block *cltypes.SignedBeaconBlock, key pendingEnvelopeKey, job *envelopeJob) bool {
	stored, err := s.forkchoiceStore.ReadEnvelopeFromDisk(key.blockRoot)
	if err != nil || stored == nil || stored.Message == nil || job.envelope == nil || job.envelope.Message == nil {
		return false
	}
	if stored.Message.BuilderIndex != job.envelope.Message.BuilderIndex {
		return false
	}
	s.markEnvelopeAvailable(seenEnvelopeKey{
		beaconBlockRoot: key.blockRoot,
		builderIndex:    stored.Message.BuilderIndex,
	}, block.Block.Slot)
	s.removePendingEnvelope(key, job)
	return true
}

func storedEnvelopeResult(blockRoot common.Hash, block *cltypes.SignedBeaconBlock, builderIndex uint64) error {
	if block == nil || block.Block == nil || block.Block.Body == nil {
		return fmt.Errorf("%w: stored envelope block is incomplete", ErrIgnore)
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return fmt.Errorf("%w: stored envelope block has no committed bid", ErrIgnore)
	}
	storedBuilder := bid.Message.BuilderIndex
	if storedBuilder != builderIndex {
		return fmt.Errorf("envelope builder_index %d != stored builder_index %d", builderIndex, storedBuilder)
	}
	return fmt.Errorf("%w: envelope already applied for block %v from builder %d", ErrIgnore, blockRoot, builderIndex)
}

// queuePendingEnvelope adds an envelope to the pending queue for later processing
func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) {
	s.queuePendingEnvelopeWithOptions(blockRoot, envelope, false, true, false)
}

func (s *executionPayloadService) queuePendingEnvelopeWithOptions(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, recovered, validatePayload, dataAvailability bool) {
	key := pendingEnvelopeKey{blockRoot: blockRoot, dataAvailability: dataAvailability}

	var envelopeHash common.Hash
	if !dataAvailability {
		if !recovered && s.pendingCount.Load() >= maxPendingEnvelopes {
			return
		}
		var err error
		envelopeHash, err = envelope.HashSSZ()
		if err != nil {
			log.Warn("Failed to hash envelope for pending queue", "blockRoot", blockRoot, "err", err)
			return
		}
	}
	key.envelopeHash = envelopeHash

	job := &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
	}
	job.recovered.Store(recovered)
	job.validate.Store(validatePayload)
	if dataAvailability {
		job.retryDelay = pendingEnvelopeInitialRetry
		job.nextAttempt = time.Now().Add(job.retryDelay)
	}

	s.pendingMu.Lock()
	if actual, loaded := s.pendingEnvelopes.Load(key); loaded {
		storedJob := actual.(*envelopeJob)
		upgradeEnvelopeJob(storedJob, recovered, validatePayload)
		if dataAvailability {
			storedJob.creationTime = time.Now()
		}
		s.pendingMu.Unlock()
		return
	}
	if dataAvailability && s.pendingCount.Load() >= maxPendingEnvelopes {
		s.evictUnvalidatedPendingEnvelope()
	}
	if s.pendingCount.Load() >= maxPendingEnvelopes {
		s.pendingMu.Unlock()
		return
	}
	s.pendingEnvelopes.Store(key, job)
	s.pendingCount.Add(1)
	s.pendingMu.Unlock()

	s.pendingCond.L.Lock()
	s.pendingCond.Signal()
	s.pendingCond.L.Unlock()
}

func (s *executionPayloadService) evictUnvalidatedPendingEnvelope() {
	s.pendingEnvelopes.Range(func(key, value any) bool {
		pendingKey := key.(pendingEnvelopeKey)
		job := value.(*envelopeJob)
		if pendingKey.dataAvailability || job.processing {
			return true
		}
		if s.pendingEnvelopes.CompareAndDelete(pendingKey, value) {
			s.pendingCount.Add(-1)
		}
		return false
	})
}

func (s *executionPayloadService) claimPendingEnvelope(key pendingEnvelopeKey, job *envelopeJob) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	actual, ok := s.pendingEnvelopes.Load(key)
	if !ok || actual != job || job.processing {
		return false
	}
	job.processing = true
	return true
}

func (s *executionPayloadService) releasePendingEnvelope(job *envelopeJob) {
	s.pendingMu.Lock()
	job.processing = false
	s.pendingMu.Unlock()
}

func upgradeEnvelopeJob(job *envelopeJob, recovered, validatePayload bool) {
	if validatePayload {
		job.validate.Store(true)
	}
	if recovered {
		job.recovered.Store(true)
	}
}

func (s *executionPayloadService) promoteDataAvailabilityRetry(oldKey pendingEnvelopeKey, job *envelopeJob) {
	job.retryDelay = pendingEnvelopeInitialRetry
	job.nextAttempt = time.Now().Add(job.retryDelay)
	newKey := pendingEnvelopeKey{blockRoot: oldKey.blockRoot, dataAvailability: true}
	s.pendingMu.Lock()
	if actual, ok := s.pendingEnvelopes.Load(oldKey); !ok || actual != job {
		s.pendingMu.Unlock()
		return
	}
	actual, loaded := s.pendingEnvelopes.Load(newKey)
	if loaded {
		storedJob := actual.(*envelopeJob)
		upgradeEnvelopeJob(storedJob, job.recovered.Load(), job.validate.Load())
		storedJob.creationTime = time.Now()
	} else {
		job.creationTime = time.Now()
		s.pendingEnvelopes.Store(newKey, job)
	}
	if s.pendingEnvelopes.CompareAndDelete(oldKey, job) && loaded {
		s.pendingCount.Add(-1)
	}
	s.pendingMu.Unlock()
}

func (s *executionPayloadService) removePendingEnvelope(key pendingEnvelopeKey, job *envelopeJob) {
	s.pendingMu.Lock()
	if s.pendingEnvelopes.CompareAndDelete(key, job) {
		s.pendingCount.Add(-1)
	}
	s.pendingMu.Unlock()
}

func (s *executionPayloadService) expirePendingEnvelope(key pendingEnvelopeKey, job *envelopeJob, expiry time.Duration) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	actual, ok := s.pendingEnvelopes.Load(key)
	if !ok || actual != job || time.Since(job.creationTime) <= expiry {
		return false
	}
	if s.pendingEnvelopes.CompareAndDelete(key, job) {
		s.pendingCount.Add(-1)
		return true
	}
	return false
}

// loop is the background goroutine that processes pending envelopes
func (s *executionPayloadService) loop(ctx context.Context) {
	// Wake any blocked Wait() on context cancellation to prevent deadlock.
	go func() {
		<-ctx.Done()
		s.pendingCond.L.Lock()
		s.pendingCond.Broadcast()
		s.pendingCond.L.Unlock()
	}()

	for {
		// Wait until there are pending envelopes
		s.pendingCond.L.Lock()
		for s.pendingCount.Load() == 0 {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				s.pendingCond.L.Unlock()
				return
			default:
			}
			s.pendingCond.Wait()
		}
		s.pendingCond.L.Unlock()

		// Poll until all pending envelopes are processed
		ticker := time.NewTicker(pendingEnvelopeCheckInterval)
		for s.pendingCount.Load() > 0 {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				s.processPendingEnvelopes(ctx)
			}
		}
		ticker.Stop()
	}
}

// processPendingEnvelopes checks and processes any pending envelopes whose blocks have arrived
func (s *executionPayloadService) processPendingEnvelopes(ctx context.Context) {
	s.pendingEnvelopes.Range(func(key, value any) bool {
		pendingKey := key.(pendingEnvelopeKey)
		job := value.(*envelopeJob)

		// Check expiry
		expiry := pendingEnvelopeExpiry
		if pendingKey.dataAvailability {
			expiry = pendingDataAvailabilityExpiry
		}
		if s.expirePendingEnvelope(pendingKey, job, expiry) {
			log.Trace("Pending envelope expired", "blockRoot", pendingKey.blockRoot)
			return true
		}

		// Check if block has arrived
		block, ok := s.forkchoiceStore.GetBlock(pendingKey.blockRoot)
		if !ok || block == nil {
			return true // Block still not here, keep waiting
		}
		if pendingKey.dataAvailability && time.Now().Before(job.nextAttempt) {
			return true
		}
		if !s.claimPendingEnvelope(pendingKey, job) {
			return true
		}
		defer s.releasePendingEnvelope(job)

		err := s.processEnvelope(ctx, job.envelope, job.recovered.Load(), job.validate.Load(), false)
		if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			if !pendingKey.dataAvailability {
				s.promoteDataAvailabilityRetry(pendingKey, job)
				return true
			}
			job.retryDelay = min(job.retryDelay*2, pendingEnvelopeMaxRetry)
			job.nextAttempt = time.Now().Add(job.retryDelay)
			return true
		}
		if errors.Is(err, ErrIgnore) && s.accountStoredPendingEnvelope(block, pendingKey, job) {
			return true
		}
		if errors.Is(err, forkchoice.ErrIgnore) || errors.Is(err, ErrIgnore) {
			return true
		}
		s.removePendingEnvelope(pendingKey, job)
		if err != nil {
			log.Trace("Failed to process pending envelope", "blockRoot", pendingKey.blockRoot, "err", err)
		}
		return true
	})
}
