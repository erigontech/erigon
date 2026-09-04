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
	"github.com/erigontech/erigon/cl/phase1/core/state"
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

type pendingEnvelopeJob struct {
	envelope   *cltypes.SignedExecutionPayloadEnvelope
	ownedBytes uint64
}

const (
	seenEnvelopeCacheSize        = 1000
	pendingEnvelopeExpiry        = 30 * time.Second
	pendingEnvelopeCheckInterval = 100 * time.Millisecond
	maxPendingEnvelopes          = 1024
	maxPendingEnvelopeBytes      = 4 * clparams.MaxChunkSize
)

type executionPayloadService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	beaconCfg       *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter

	// Cache to track seen envelopes: (beaconBlockRoot, builderIndex) -> struct{}
	seenEnvelopesCache *lru.Cache[seenEnvelopeKey, struct{}]

	// Pending envelopes waiting for block to arrive
	pending      *pendingJobQueue[pendingEnvelopeKey, *pendingEnvelopeJob]
	pendingBytes atomic.Uint64
	pendingMu    sync.Mutex
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

func (s *executionPayloadService) newPendingQueue() *pendingJobQueue[pendingEnvelopeKey, *pendingEnvelopeJob] {
	return newPendingJobQueue(maxPendingEnvelopes, pendingEnvelopeExpiry, pendingEnvelopeCheckInterval,
		s.tryProcessPendingEnvelope,
		func(key pendingEnvelopeKey, job *pendingEnvelopeJob) {
			s.releasePendingEnvelopeBytes(job.ownedBytes)
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
	if err := obj.DecodeSSZStrict(data, int(version)); err != nil {
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
	block, blockKnown := s.forkchoiceStore.GetBlock(beaconBlockRoot)
	if err := validateEnvelopeLimits(s.beaconCfg, envelope); err != nil {
		if !blockKnown || block == nil {
			return fmt.Errorf("%w: invalid execution payload envelope for unknown block: %w", ErrIgnore, err)
		}
		return err
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
		queued, err := s.queuePendingEnvelope(beaconBlockRoot, signedEnvelope)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrIgnore, err)
		}
		if !queued {
			return fmt.Errorf("%w: execution payload envelope already queued", ErrIgnore)
		}
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

	// Process the execution payload through forkchoice
	// Note: bid matching and signature verification are done in OnExecutionPayload.validateEnvelopeAgainstBlock
	if err := s.forkchoiceStore.OnExecutionPayload(ctx, signedEnvelope, true, true); err != nil {
		if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			s.emitExecutionPayloadGossip(block, envelope)
		}
		if errors.Is(err, forkchoice.ErrIgnore) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %v", ErrIgnore, err) //nolint:errorlint // converting, not wrapping: the forkchoice sentinels must not stay matchable
		}
		return fmt.Errorf("failed to process execution payload: %w", err)
	}

	// Mark as seen AFTER successful validation
	// This ensures invalid envelopes (e.g., with forged signatures) don't block valid ones
	s.seenEnvelopesCache.Add(seenKey, struct{}{})

	s.emitExecutionPayloadGossip(block, envelope)
	s.emitters.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{
		Slot: block.Block.Slot, BuilderIndex: builderIndex, BlockHash: envelope.Payload.BlockHash, BlockRoot: beaconBlockRoot,
		ExecutionOptimistic: s.forkchoiceStore.IsRootOptimistic(beaconBlockRoot),
	})
	s.emitFullHeadUpdate(block, beaconBlockRoot)

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

func (s *executionPayloadService) emitFullHeadUpdate(block *cltypes.SignedBeaconBlock, blockRoot common.Hash) {
	headRoot, headSlot, err := s.forkchoiceStore.GetHead(nil)
	if err != nil || headRoot != blockRoot || s.beaconCfg.SlotsPerEpoch == 0 {
		return
	}
	var headEvent *beaconevents.HeadV2Data
	err = s.forkchoiceStore.ViewStateAtBlockRoot(blockRoot, func(headState *state.CachingBeaconState) error {
		headEvent, err = beaconevents.BuildHeadV2Data(
			s.beaconCfg,
			headState,
			headSlot,
			headRoot,
			block.Block.StateRoot,
			"full",
			s.forkchoiceStore.IsRootOptimistic(blockRoot),
		)
		return err
	})
	if err != nil || headEvent == nil {
		return
	}
	s.emitters.WithHeadEventLock(func() {
		currentHeadRoot, currentHeadSlot, err := s.forkchoiceStore.GetHead(nil)
		currentPayloadStatus := beaconevents.PayloadStatusName(s.forkchoiceStore.GetHeadPayloadStatus())
		currentOptimistic := s.forkchoiceStore.IsRootOptimistic(currentHeadRoot)
		if err != nil || currentHeadRoot != headRoot || currentHeadSlot != headSlot ||
			currentPayloadStatus != headEvent.Data.PayloadStatus || currentOptimistic != headEvent.Data.ExecutionOptimistic {
			return
		}
		s.emitters.State().SendHeadV2(headEvent)
	})
}

func (s *executionPayloadService) queuePendingEnvelope(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
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
	if s.pending.count.Load() >= maxPendingEnvelopes || ownedBytes64 > maxPendingEnvelopeBytes || currentBytes > maxPendingEnvelopeBytes-ownedBytes64 {
		return false, errors.New("pending execution payload envelope capacity reached")
	}
	if !s.pending.reserve() {
		return false, errors.New("pending execution payload envelope capacity reached")
	}
	s.pendingBytes.Store(currentBytes + ownedBytes64)
	s.pending.storeReserved(key, &pendingEnvelopeJob{envelope: envelope, ownedBytes: ownedBytes64})
	return true, nil
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

// tryProcessPendingEnvelope re-runs full validation via ProcessMessage once the block has arrived.
func (s *executionPayloadService) tryProcessPendingEnvelope(ctx context.Context, key pendingEnvelopeKey, job *pendingEnvelopeJob) (func(), bool) {
	block, ok := s.forkchoiceStore.GetBlock(key.blockRoot)
	if !ok || block == nil {
		return nil, false // Block still not here, keep waiting
	}
	return func() {
		s.releasePendingEnvelopeBytes(job.ownedBytes)
		if err := s.ProcessMessage(ctx, nil, job.envelope); err != nil {
			log.Trace("Failed to process pending envelope", "blockRoot", key.blockRoot, "err", err)
		}
	}, true
}
