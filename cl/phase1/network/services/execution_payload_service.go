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

const (
	seenEnvelopeCacheSize        = 1000
	pendingEnvelopeExpiry        = 30 * time.Second
	pendingEnvelopeCheckInterval = 100 * time.Millisecond
	maxPendingEnvelopes          = 1024
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for block to arrive
	pending *pendingJobQueue[pendingEnvelopeKey, *cltypes.SignedExecutionPayloadEnvelope]
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
	}
	s.pending = s.newPendingQueue()
	go s.pending.loop(ctx)
	return s
}

func (s *executionPayloadService) newPendingQueue() *pendingJobQueue[pendingEnvelopeKey, *cltypes.SignedExecutionPayloadEnvelope] {
	return newPendingJobQueue(maxPendingEnvelopes, pendingEnvelopeExpiry, pendingEnvelopeCheckInterval,
		s.tryProcessPendingEnvelope,
		func(key pendingEnvelopeKey) {
			log.Trace("Pending envelope expired", "blockRoot", key.blockRoot)
		})
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
		s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope)
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
		if errors.Is(err, forkchoice.ErrIgnore) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			return fmt.Errorf("%w: %v", ErrIgnore, err)
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

// queuePendingEnvelope adds an envelope to the pending queue for later processing
func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) {
	// Compute envelope hash to allow multiple candidates per block
	envelopeHash, err := envelope.HashSSZ()
	if err != nil {
		log.Warn("Failed to hash envelope for pending queue", "blockRoot", blockRoot, "err", err)
		return
	}

	s.pending.enqueue(pendingEnvelopeKey{
		blockRoot:    blockRoot,
		envelopeHash: envelopeHash,
	}, envelope)
}

// tryProcessPendingEnvelope re-runs full validation via ProcessMessage once the block has arrived.
func (s *executionPayloadService) tryProcessPendingEnvelope(ctx context.Context, key pendingEnvelopeKey, envelope *cltypes.SignedExecutionPayloadEnvelope) (func(), bool) {
	block, ok := s.forkchoiceStore.GetBlock(key.blockRoot)
	if !ok || block == nil {
		return nil, false // Block still not here, keep waiting
	}

	return func() {
		if err := s.ProcessMessage(ctx, nil, envelope); err != nil {
			log.Trace("Failed to process pending envelope", "blockRoot", key.blockRoot, "err", err)
		}
	}, true
}
