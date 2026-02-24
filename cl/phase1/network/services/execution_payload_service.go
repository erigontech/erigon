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

// pendingEnvelopeKey tracks envelopes waiting for their block to arrive.
// We use (blockRoot, envelopeHash) as key instead of just blockRoot because:
//   - Multiple envelopes (including forged ones) may arrive before the block
//   - Using only blockRoot would cause later arrivals to overwrite earlier ones
//   - If a forged envelope overwrites the valid one, we lose the valid envelope
//   - With envelopeHash, all candidates are kept and validated when block arrives
type pendingEnvelopeKey struct {
	blockRoot    common.Hash
	envelopeHash common.Hash
}

// envelopeJob represents a pending envelope waiting for its block to arrive
type envelopeJob struct {
	envelope     *cltypes.SignedExecutionPayloadEnvelope
	creationTime time.Time
}

const (
	seenEnvelopeCacheSize        = 1000
	pendingEnvelopeExpiry        = 30 * time.Second
	pendingEnvelopeCheckInterval = 100 * time.Millisecond
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for block to arrive
	pendingEnvelopes sync.Map // pendingEnvelopeKey -> *envelopeJob
	pendingCount     atomic.Int32
	pendingCond      *sync.Cond
}

// NewExecutionPayloadService creates a new execution payload service
func NewExecutionPayloadService(
	ctx context.Context,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	beaconCfg *clparams.BeaconChainConfig,
) ExecutionPayloadService {
	seenEnvelopesCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	if err != nil {
		panic(err)
	}
	s := &executionPayloadService{
		forkchoiceStore:    forkchoiceStore,
		beaconCfg:          beaconCfg,
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

	log.Debug("Received execution payload via gossip",
		"slot", envelope.Slot,
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)

	// [IGNORE] The envelope's block root has been seen (via gossip or non-gossip sources)
	// A client MAY queue payload for processing once the block is retrieved.
	block, ok := s.forkchoiceStore.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		// Block hasn't arrived yet, queue envelope for later processing
		s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope)
		log.Debug("Queued execution payload envelope for later processing",
			"beaconBlockRoot", beaconBlockRoot,
			"builderIndex", builderIndex)
		return nil
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
	if envelope.Slot < finalizedSlot {
		return fmt.Errorf("%w: envelope slot %d < finalized slot %d", ErrIgnore, envelope.Slot, finalizedSlot)
	}

	// Process the execution payload through forkchoice
	// Note: bid matching and signature verification are done in OnExecutionPayload.validateEnvelopeAgainstBlock
	if err := s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, true, true); err != nil {
		return fmt.Errorf("failed to process execution payload: %w", err)
	}

	// Mark as seen AFTER successful validation
	// This ensures invalid envelopes (e.g., with forged signatures) don't block valid ones
	s.seenEnvelopesCache.Add(seenKey, struct{}{})

	log.Debug("Processed execution payload via gossip",
		"slot", envelope.Slot,
		"beaconBlockRoot", beaconBlockRoot,
		"builderIndex", builderIndex)

	return nil
}

// queuePendingEnvelope adds an envelope to the pending queue for later processing
func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) {
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

	// Only add if not already present (avoid duplicate count)
	if _, loaded := s.pendingEnvelopes.LoadOrStore(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
	}); !loaded {
		s.pendingCount.Add(1)
		s.pendingCond.Signal() // Wake up the loop
	}
}

// loop is the background goroutine that processes pending envelopes
func (s *executionPayloadService) loop(ctx context.Context) {
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
		if time.Since(job.creationTime) > pendingEnvelopeExpiry {
			s.pendingEnvelopes.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Debug("Pending envelope expired", "blockRoot", pendingKey.blockRoot)
			return true
		}

		// Check if block has arrived
		block, ok := s.forkchoiceStore.GetBlock(pendingKey.blockRoot)
		if !ok || block == nil {
			return true // Block still not here, keep waiting
		}

		// Block arrived, remove from pending and process
		s.pendingEnvelopes.Delete(pendingKey)
		s.pendingCount.Add(-1)

		// Re-run full validation via ProcessMessage
		if err := s.ProcessMessage(ctx, nil, job.envelope); err != nil {
			log.Debug("Failed to process pending envelope", "blockRoot", pendingKey.blockRoot, "err", err)
		}
		return true
	})
}
