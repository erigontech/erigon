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

type pendingEnvelopeKey struct {
	blockRoot   common.Hash
	messageHash common.Hash
}

// envelopeJob represents an envelope waiting for its dependencies.
type envelopeJob struct {
	envelope     *cltypes.SignedExecutionPayloadEnvelope
	creationTime time.Time
	nextAttempt  time.Time
	blockSeen    atomic.Bool
	resolving    atomic.Bool
	validated    bool
}

const (
	seenEnvelopeCacheSize        = 1000
	pendingEnvelopeExpiry        = 3 * time.Minute
	pendingEnvelopeCheckInterval = 100 * time.Millisecond
	pendingEnvelopeRetryInterval = time.Second
	maxPendingEnvelopes          = 1024
	maxPendingCandidatesPerRoot  = 4
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for their dependencies
	pendingEnvelopes  sync.Map // pendingEnvelopeKey -> *envelopeJob
	pendingCount      atomic.Int32
	pendingRootCounts map[common.Hash]int
	pendingCond       *sync.Cond
	pendingMu         sync.Mutex
	resolver          *envelopeResolver
}

// NewExecutionPayloadService creates a new execution payload service
func NewExecutionPayloadService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	beaconCfg *clparams.BeaconChainConfig,
	emitters *beaconevents.EventEmitter,
	requester executionPayloadEnvelopeRequester,
) *executionPayloadService {
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
	s.resolver = newEnvelopeResolver(ctx, requester, s)
	go s.loop(ctx)
	return s
}

func (s *executionPayloadService) ResolveExecutionPayloadEnvelope(root common.Hash) {
	if s.resolver != nil {
		s.resolver.ResolveExecutionPayloadEnvelope(root)
	}
}

func (s *executionPayloadService) HasPendingExecutionPayloadEnvelope(root common.Hash) bool {
	return s.hasPendingEnvelopeRoot(root)
}

func (s *executionPayloadService) hasPendingEnvelopeRoot(root common.Hash) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return s.pendingRootCounts[root] > 0
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
	if err := requireCanonicalSSZ(data, obj); err != nil {
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
		return fmt.Errorf("%w: block %v not found", ErrIgnore, beaconBlockRoot)
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
			if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) || errors.Is(err, forkchoice.ErrELPayloadValidationUnavailable) {
				s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope, true, true)
			}
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
func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, blockSeen, validated bool) bool {
	messageHash, err := envelope.Message.HashSSZ()
	if err != nil {
		return false
	}
	key := pendingEnvelopeKey{blockRoot: blockRoot, messageHash: messageHash}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.pendingRootCounts == nil {
		s.pendingRootCounts = make(map[common.Hash]int)
	}
	if existing, loaded := s.pendingEnvelopes.Load(key); loaded {
		job := existing.(*envelopeJob)
		if blockSeen {
			job.blockSeen.Store(true)
		}
		if !validated || job.validated {
			return false
		}
		nextAttempt := job.nextAttempt
		if retryAt := time.Now().Add(pendingEnvelopeRetryInterval); nextAttempt.Before(retryAt) {
			nextAttempt = retryAt
		}
		replacement := &envelopeJob{
			envelope:     envelope,
			creationTime: job.creationTime,
			nextAttempt:  nextAttempt,
			validated:    true,
		}
		replacement.blockSeen.Store(true)
		s.pendingEnvelopes.Store(key, replacement)
		return true
	}
	if s.pendingRootCounts[blockRoot] >= maxPendingCandidatesPerRoot {
		oldestKey, found := s.oldestPendingEnvelopeForRoot(blockRoot)
		if !found {
			return false
		}
		s.deletePendingEnvelopeLocked(oldestKey, nil)
	}
	for s.pendingCount.Load() >= maxPendingEnvelopes {
		oldestKey, found := s.oldestPendingEnvelope(false)
		if !found && blockSeen {
			oldestKey, found = s.oldestPendingEnvelope(true)
		}
		if !found {
			return false
		}
		s.deletePendingEnvelopeLocked(oldestKey, nil)
	}

	job := &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
		validated:    validated,
	}
	job.blockSeen.Store(blockSeen)
	if _, loaded := s.pendingEnvelopes.LoadOrStore(key, job); loaded {
		return false
	} else {
		s.pendingCount.Add(1)
		s.pendingRootCounts[blockRoot]++
		if s.pendingCond != nil {
			s.pendingCond.L.Lock()
			s.pendingCond.Signal()
			s.pendingCond.L.Unlock()
		}
	}
	return true
}

func (s *executionPayloadService) oldestPendingEnvelopeForRoot(blockRoot common.Hash) (pendingEnvelopeKey, bool) {
	var oldestKey pendingEnvelopeKey
	var oldestTime time.Time
	found := false
	s.pendingEnvelopes.Range(func(candidateKey, value any) bool {
		key := candidateKey.(pendingEnvelopeKey)
		candidate := value.(*envelopeJob)
		if key.blockRoot != blockRoot || candidate.validated || candidate.resolving.Load() {
			return true
		}
		if !found || candidate.creationTime.Before(oldestTime) {
			oldestKey = key
			oldestTime = candidate.creationTime
			found = true
		}
		return true
	})
	return oldestKey, found
}

func (s *executionPayloadService) deletePendingEnvelopeLocked(key pendingEnvelopeKey, expected *envelopeJob) bool {
	current, loaded := s.pendingEnvelopes.Load(key)
	if !loaded || expected != nil && current != expected {
		return false
	}
	s.pendingEnvelopes.Delete(key)
	s.pendingCount.Add(-1)
	if count := s.pendingRootCounts[key.blockRoot]; count > 1 {
		s.pendingRootCounts[key.blockRoot] = count - 1
	} else {
		delete(s.pendingRootCounts, key.blockRoot)
	}
	return true
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
		if time.Since(job.creationTime) > pendingEnvelopeExpiry {
			s.pendingMu.Lock()
			if !s.deletePendingEnvelopeLocked(pendingKey, job) {
				job.resolving.Store(false)
			}
			s.pendingMu.Unlock()
			log.Trace("Pending envelope expired", "blockRoot", pendingKey.blockRoot)
			return true
		}

		if !blockSeen {
			block, ok := s.forkchoiceStore.GetBlock(pendingKey.blockRoot)
			if !ok || block == nil {
				s.pendingMu.Lock()
				current, stillPending = s.pendingEnvelopes.Load(pendingKey)
				if stillPending && current == job {
					job.resolving.Store(false)
				}
				s.pendingMu.Unlock()
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
	s.deletePendingEnvelopeLocked(pendingKey, job)
}
