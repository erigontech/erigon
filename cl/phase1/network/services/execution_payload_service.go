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

// pendingEnvelopeKey tracks envelopes waiting for their block or data columns.
// We use (blockRoot, envelopeHash) as key instead of just blockRoot because:
//   - Multiple envelopes (including forged ones) may arrive before the block
//   - Using only blockRoot would cause later arrivals to overwrite earlier ones
//   - If a forged envelope overwrites the valid one, we lose the valid envelope
//   - With envelopeHash, all candidates are kept and validated when block arrives
type pendingEnvelopeKey struct {
	blockRoot    common.Hash
	envelopeHash common.Hash
}

// envelopeJob represents an envelope waiting for its dependencies.
type envelopeJob struct {
	envelope     *cltypes.SignedExecutionPayloadEnvelope
	creationTime time.Time
	nextAttempt  time.Time
	blockSeen    atomic.Bool
	resolving    atomic.Bool
}

const (
	seenEnvelopeCacheSize        = 1000
	pendingEnvelopeExpiry        = 3 * time.Minute
	pendingEnvelopeCheckInterval = 100 * time.Millisecond
	pendingEnvelopeRetryInterval = time.Second
	maxPendingEnvelopes          = 1024
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for their dependencies
	pendingEnvelopes sync.Map // pendingEnvelopeKey -> *envelopeJob
	pendingCount     atomic.Int32
	pendingCond      *sync.Cond
	pendingMu        sync.Mutex
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
		// Block hasn't arrived yet, queue envelope for later processing
		s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope, false)
		// Also store in forkchoice's pendingEnvelopes so OnBlock can process it immediately
		// when the block arrives, instead of waiting for the 100ms polling loop.
		// validatePayload must be true: if the block arrives (via OnBlock) before this call
		// acquires f.mu, the envelope will be applied with validatePayload — ensuring
		// NewPayload is sent to the EL. With false, a mutex-contention race silently
		// marks the envelope as processed without ever notifying the EL, permanently
		// breaking the chain.
		s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, false, true)
		log.Trace("Queued execution payload envelope for later processing",
			"beaconBlockRoot", beaconBlockRoot,
			"builderIndex", builderIndex)
		return ErrIgnore
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

	// [IGNORE] The envelope is from a slot greater than or equal to the latest finalized slot
	finalizedSlot := s.forkchoiceStore.FinalizedSlot()
	if block.Block.Slot < finalizedSlot {
		return fmt.Errorf("%w: envelope slot %d < finalized slot %d", ErrIgnore, block.Block.Slot, finalizedSlot)
	}

	// Process the execution payload through forkchoice
	// Note: bid matching and signature verification are done in OnExecutionPayload.validateEnvelopeAgainstBlock
	if err := s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, true, true); err != nil {
		if isRetryableExecutionPayloadError(err) {
			s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope, true)
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		if errors.Is(err, forkchoice.ErrIgnore) {
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		return fmt.Errorf("failed to process execution payload: %w", err)
	}

	// Mark as seen AFTER successful validation
	// This ensures invalid envelopes (e.g., with forged signatures) don't block valid ones
	s.seenEnvelopesCache.Add(seenKey, struct{}{})

	// Emit SSE event for execution_payload_available [New in Gloas:EIP7732]
	s.emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{
		Slot:      block.Block.Slot,
		BlockRoot: beaconBlockRoot,
	})

	log.Trace("Processed execution payload via gossip",
		"slot", block.Block.Slot,
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)

	return nil
}

func isRetryableExecutionPayloadError(err error) bool {
	return errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
		errors.Is(err, forkchoice.ErrELPayloadValidationUnavailable) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// queuePendingEnvelope adds an envelope to the pending queue for later processing
func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, blockSeen bool) {
	// Compute envelope hash to allow multiple candidates per block
	envelopeHash, err := envelope.HashSSZ()
	if err != nil {
		log.Warn("Failed to hash envelope for pending queue", "blockRoot", blockRoot, "err", err)
		return
	}

	key := pendingEnvelopeKey{
		blockRoot:    blockRoot,
		envelopeHash: envelopeHash,
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if existing, loaded := s.pendingEnvelopes.Load(key); loaded {
		if blockSeen {
			existing.(*envelopeJob).blockSeen.Store(true)
		}
		return
	}
	for s.pendingCount.Load() >= maxPendingEnvelopes {
		oldestKey, found := s.oldestPendingEnvelope(false)
		if !found && blockSeen {
			oldestKey, found = s.oldestPendingEnvelope(true)
		}
		if !found {
			return
		}
		if _, loaded := s.pendingEnvelopes.LoadAndDelete(oldestKey); loaded {
			s.pendingCount.Add(-1)
		}
	}

	job := &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
	}
	job.blockSeen.Store(blockSeen)
	if _, loaded := s.pendingEnvelopes.LoadOrStore(key, job); loaded {
	} else {
		s.pendingCount.Add(1)
		s.pendingCond.L.Lock()
		s.pendingCond.Signal()
		s.pendingCond.L.Unlock()
	}
}

func (s *executionPayloadService) oldestPendingEnvelope(blockSeen bool) (pendingEnvelopeKey, bool) {
	var oldestKey pendingEnvelopeKey
	var oldestTime time.Time
	found := false
	s.pendingEnvelopes.Range(func(candidateKey, value any) bool {
		candidate := value.(*envelopeJob)
		if candidate.resolving.Load() || candidate.blockSeen.Load() != blockSeen {
			return true
		}
		if !found || candidate.creationTime.Before(oldestTime) {
			oldestKey = candidateKey.(pendingEnvelopeKey)
			oldestTime = candidate.creationTime
			found = true
		}
		return true
	})
	return oldestKey, found
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

// processPendingEnvelopes retries pending envelopes whose blocks have arrived.
func (s *executionPayloadService) processPendingEnvelopes(ctx context.Context) {
	s.pendingEnvelopes.Range(func(key, value any) bool {
		pendingKey := key.(pendingEnvelopeKey)
		job := value.(*envelopeJob)

		s.pendingMu.Lock()
		current, stillPending := s.pendingEnvelopes.Load(pendingKey)
		if !stillPending || current != job || !job.resolving.CompareAndSwap(false, true) {
			s.pendingMu.Unlock()
			return true
		}
		blockSeen := job.blockSeen.Load()
		s.pendingMu.Unlock()

		if !blockSeen {
			block, ok := s.forkchoiceStore.GetBlock(pendingKey.blockRoot)
			if !ok || block == nil {
				s.pendingMu.Lock()
				current, stillPending = s.pendingEnvelopes.Load(pendingKey)
				expired := stillPending && current == job && time.Since(job.creationTime) > pendingEnvelopeExpiry
				if stillPending && current == job {
					if expired {
						s.pendingEnvelopes.Delete(pendingKey)
						s.pendingCount.Add(-1)
					} else {
						job.resolving.Store(false)
					}
				} else {
					job.resolving.Store(false)
				}
				s.pendingMu.Unlock()
				if expired {
					log.Trace("Pending envelope expired", "blockRoot", pendingKey.blockRoot)
				}
				return true
			}
			job.blockSeen.Store(true)
		}
		if time.Now().Before(job.nextAttempt) {
			s.pendingMu.Lock()
			current, stillPending = s.pendingEnvelopes.Load(pendingKey)
			if stillPending && current == job {
				job.resolving.Store(false)
			}
			s.pendingMu.Unlock()
			return true
		}

		err := s.ProcessMessage(ctx, nil, job.envelope)
		s.finishPendingEnvelopeAttempt(pendingKey, job, err)
		if err != nil {
			log.Trace("Failed to process pending envelope", "blockRoot", pendingKey.blockRoot, "err", err)
		}
		return true
	})
}

func (s *executionPayloadService) finishPendingEnvelopeAttempt(pendingKey pendingEnvelopeKey, job *envelopeJob, err error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	current, stillPending := s.pendingEnvelopes.Load(pendingKey)
	if !stillPending || current != job {
		job.resolving.Store(false)
		return
	}
	if isRetryableExecutionPayloadError(err) {
		job.nextAttempt = time.Now().Add(pendingEnvelopeRetryInterval)
		job.resolving.Store(false)
		return
	}
	s.pendingEnvelopes.Delete(pendingKey)
	s.pendingCount.Add(-1)
}
