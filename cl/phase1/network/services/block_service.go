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

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

var (
	ErrInvalidSignature         = errors.New("invalid signature")
	ErrPublishedBlockJobExpired = errors.New("published block integration expired")
	ErrPublishedBlockJobStopped = errors.New("block service stopped")
)

var publishedBlockJobSequence atomic.Uint64

const maxConcurrentBlockValidationContexts = 4

type proposerIndexAndSlot struct {
	proposerIndex uint64
	slot          uint64
}

type blockJob struct {
	// block is immutable; mu protects the publication lifetime and retry state.
	block            *cltypes.SignedBeaconBlock
	creationTime     time.Time
	scheduleSequence uint64

	mu                  sync.Mutex
	store               func(context.Context) error
	storeGeneration     uint64
	completedGeneration uint64
	terminal            bool
	running             bool
	attempt             *blockJobAttempt
	lastAttempt         *blockJobAttempt

	gossip                  bool
	signedRoot              common.Hash
	gossipEventSent         bool
	persisted               bool
	executionAndDataChecked bool
	processingFailureAt     time.Time
	retryAfter              time.Time
	retryDelay              time.Duration
}

func (job *blockJob) readyToRetryLocked(now time.Time) bool {
	return job.retryAfter.IsZero() || !now.Before(job.retryAfter)
}

func (job *blockJob) recordProcessingFailureLocked(now time.Time, err error) {
	job.processingFailureAt = now
	// Other dependencies stay on the fast queue interval. Retaining retryDelay
	// ensures that a later EL failure continues the existing backoff sequence.
	if !errors.Is(err, forkchoice.ErrNewPayloadNoStatus) {
		job.retryAfter = time.Time{}
		return
	}
	// Repeated newPayload calls do not help an unavailable EL recover faster.
	if job.retryDelay == 0 {
		job.retryDelay = blockELRetryInitialDelay
	} else {
		job.retryDelay = min(2*job.retryDelay, blockELRetryMaxDelay)
	}
	job.retryAfter = now.Add(job.retryDelay)
}

func (job *blockJob) processingState() (persisted, executionAndDataChecked bool) {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.persisted, job.executionAndDataChecked
}

func (job *blockJob) markPersisted() {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.persisted = true
}

// A duplicate delivery preserves completed work and the original admission
// time. The newest failure controls the next retry, while an existing EL delay
// remains the base of exponential backoff.
func mergeBlockProcessingState(existing, incoming *blockJob) {
	if existing == incoming {
		return
	}
	incoming.mu.Lock()
	incomingPersisted := incoming.persisted
	incomingExecutionAndDataChecked := incoming.executionAndDataChecked
	incomingProcessingFailureAt := incoming.processingFailureAt
	incomingRetryAfter := incoming.retryAfter
	incomingRetryDelay := incoming.retryDelay
	incomingSignedRoot := incoming.signedRoot
	incomingGossip := incoming.gossip
	incomingGossipEventSent := incoming.gossipEventSent
	incoming.mu.Unlock()

	existing.mu.Lock()
	defer existing.mu.Unlock()
	if incomingGossip && !existing.gossip {
		existing.signedRoot = incomingSignedRoot
	}
	existing.gossip = existing.gossip || incomingGossip
	existing.gossipEventSent = existing.gossipEventSent || incomingGossipEventSent
	existing.persisted = existing.persisted || incomingPersisted
	existing.executionAndDataChecked = existing.executionAndDataChecked || incomingExecutionAndDataChecked
	if incomingProcessingFailureAt.After(existing.processingFailureAt) {
		existing.processingFailureAt = incomingProcessingFailureAt
		if incomingRetryAfter.IsZero() {
			existing.retryDelay = max(existing.retryDelay, incomingRetryDelay)
			existing.retryAfter = time.Time{}
		} else {
			if existing.retryDelay == 0 {
				existing.retryDelay = incomingRetryDelay
			} else {
				existing.retryDelay = max(incomingRetryDelay, min(2*existing.retryDelay, blockELRetryMaxDelay))
			}
			existing.retryAfter = incomingProcessingFailureAt.Add(existing.retryDelay)
		}
	} else {
		existing.retryDelay = max(existing.retryDelay, incomingRetryDelay)
		if !existing.retryAfter.IsZero() {
			existing.retryAfter = existing.processingFailureAt.Add(existing.retryDelay)
		}
	}
}

type blockJobAttempt struct {
	done       chan struct{}
	generation uint64
	err        error
}

type publishedBlockJobHandle struct {
	job        *blockJob
	generation uint64
}

func (h *publishedBlockJobHandle) Wait(ctx context.Context) error {
	for {
		h.job.mu.Lock()
		if h.job.terminal && h.job.completedGeneration >= h.generation {
			err := h.job.lastAttempt.err
			h.job.mu.Unlock()
			return err
		}
		attempt := h.job.attempt
		h.job.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
		}
	}
}

func newBlockJob(block *cltypes.SignedBeaconBlock, store func(context.Context) error) *blockJob {
	generation := uint64(0)
	if store != nil {
		generation = 1
	}
	return &blockJob{
		block:            block,
		store:            store,
		storeGeneration:  generation,
		creationTime:     time.Now(),
		scheduleSequence: publishedBlockJobSequence.Add(1),
		attempt:          &blockJobAttempt{done: make(chan struct{})},
	}
}

func newFailedBlockJob(block *cltypes.SignedBeaconBlock, store func(context.Context) error, err error) *blockJob {
	job := newBlockJob(block, store)
	job.attempt.err = err
	job.attempt.generation = job.storeGeneration
	job.lastAttempt = job.attempt
	job.completedGeneration = job.storeGeneration
	job.terminal = true
	close(job.attempt.done)
	return job
}

type blockReservation struct {
	pending    chan struct{}
	root       common.Hash
	version    uint64
	validators uint64
	// Queued gossip stays reserved even if its history entry is evicted.
	queued     *blockJob
	queuedRoot common.Hash
}

type seenBlock struct {
	signedRoot    common.Hash
	replayAllowed bool
}

type blockValidationContextKey struct {
	parentRoot common.Hash
	slot       uint64
}

type blockValidationContextCall struct {
	done    chan struct{}
	context *blockValidationContext
	err     error
}

type blockValidationContext struct {
	expectedProposer                   uint64
	latestBlockHash                    common.Hash
	latestExecutionPayloadBidBlockHash common.Hash
	hasLatestExecutionPayloadBid       bool
}

type blockService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	syncedData      *synced_data.SyncedDataManager
	ethClock        eth_clock.EthereumClock
	beaconCfg       *clparams.BeaconChainConfig

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
	seenBlocksCache *lru.Cache[proposerIndexAndSlot, seenBlock]
	reservations    map[proposerIndexAndSlot]*blockReservation
	seenBlocksMu    sync.Mutex
	validationMu    sync.Mutex
	validationCache *lru.Cache[blockValidationContextKey, *blockValidationContext]
	validationCalls map[blockValidationContextKey]*blockValidationContextCall
	validationSlots chan struct{}

	emitter *beaconevents.EventEmitter
	// Blocks waiting for their slot or an import dependency.
	blocksScheduledForLaterExecution *pendingJobQueue[[32]byte, *blockJob]
	blockJobsLifecycleMu             sync.RWMutex
	blockJobsStopped                 bool
	// store the block in db
	db kv.RwDB
}

// NewBlockService creates a new block service
func NewBlockService(
	ctx context.Context,
	db kv.RwDB,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	syncedData *synced_data.SyncedDataManager,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	emitter *beaconevents.EventEmitter,
) BlockService {
	seenBlocksCache, err := lru.New[proposerIndexAndSlot, seenBlock]("seenblocks", seenBlockCacheSize)
	if err != nil {
		panic(err)
	}
	validationCache, err := lru.New[blockValidationContextKey, *blockValidationContext]("block_validation_contexts", seenBlockCacheSize)
	if err != nil {
		panic(err)
	}
	b := &blockService{
		forkchoiceStore: forkchoiceStore,
		syncedData:      syncedData,
		ethClock:        ethClock,
		beaconCfg:       beaconCfg,
		seenBlocksCache: seenBlocksCache,
		reservations:    make(map[proposerIndexAndSlot]*blockReservation),
		validationCache: validationCache,
		validationCalls: make(map[blockValidationContextKey]*blockValidationContextCall),
		validationSlots: make(chan struct{}, maxConcurrentBlockValidationContexts),
		emitter:         emitter,
		db:              db,
	}
	b.blocksScheduledForLaterExecution = b.newPendingBlockQueue(ctx)
	go b.stopPublishedBlockJobsOnContext(ctx)
	return b
}

func (b *blockService) newPendingBlockQueue(ctx context.Context) *pendingJobQueue[[32]byte, *blockJob] {
	return newPendingJobQueue(ctx, pendingJobQueueOptions{
		name:          "beacon_block",
		capacity:      maxPendingBlocks,
		expiry:        blockJobExpiry,
		checkInterval: blockJobsIntervalTick,
	}, func(ctx context.Context, root [32]byte, job *blockJob) pendingJobDecision {
		b.processScheduledBlock(ctx, root, job, time.Now())
		// Completion and refresh are serialized by the job lock; removal here
		// could discard a newer store generation after the callback returns.
		return pendingJobKeep
	}, nil, func(_ [32]byte, job *blockJob) {
		job.mu.Lock()
		defer job.mu.Unlock()
		finishBlockJobLocked(job, ErrPublishedBlockJobExpired)
		b.finishGossipJobLocked(job, false)
	})
}

func (b *blockService) Names() []string {
	return []string{gossip.TopicNameBeaconBlock}
}

func (b *blockService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameBeaconBlock
}

func (b *blockService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedBeaconBlock, error) {
	obj := cltypes.NewSignedBeaconBlock(b.beaconCfg, version)
	if err := obj.DecodeSSZStrict(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

// ProcessMessage processes a block message according to https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
func (b *blockService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.SignedBeaconBlock) error {
	if msg == nil || msg.Block == nil || msg.Block.Body == nil {
		return errors.New("missing beacon block")
	}
	log.Trace("Received block via gossip", "slot", msg.Block.Slot)

	var admissionErr error
	err := b.validateFirstGossip(ctx, msg, func() {
		root, err := msg.Block.HashSSZ()
		if err != nil {
			admissionErr = fmt.Errorf("%w: cannot hash pending block: %v", ErrIgnore, err) //nolint:errorlint // local admission failures must not reject the peer
			return
		}
		job := newBlockJob(msg, nil)
		job.gossip = true
		admissionErr = b.schedulePendingBlockWithRoot(root, job)
	}, true)
	if admissionErr != nil {
		return admissionErr
	}
	if err != nil {
		return err
	}
	root, err := msg.Block.HashSSZ()
	if err != nil {
		return err
	}
	job := newBlockJob(msg, nil)
	job.gossip = true
	if err := b.pinGossipJob(job); err != nil {
		return err
	}
	if err := b.processGossipBlock(ctx, root, job); err != nil {
		if !isPendingBlockRetryableError(err) && !errors.Is(err, ErrIgnore) {
			job.mu.Lock()
			b.finishGossipJobLocked(job, true)
			job.mu.Unlock()
			return err
		}
		if admissionErr := b.schedulePendingBlockWithRoot(root, job); admissionErr != nil {
			return admissionErr
		}
		return fmt.Errorf("%w: block queued while a processing dependency is unavailable: %v", ErrIgnore, err) //nolint:errorlint // fork-choice sentinels must not stay matchable
	}
	job.mu.Lock()
	b.finishGossipJobLocked(job, true)
	job.mu.Unlock()
	return nil
}

func (b *blockService) schedulePendingBlockWithRoot(root [32]byte, job *blockJob) error {
	if job.gossip {
		if err := b.pinGossipJob(job); err != nil {
			return err
		}
	}
	retained, _ := b.scheduleBlockJob(root, job)
	retained.mu.Lock()
	defer retained.mu.Unlock()
	if retained.terminal && retained.lastAttempt.err != nil {
		if retained != job {
			job.mu.Lock()
			b.finishGossipJobLocked(job, false)
			job.mu.Unlock()
		}
		return fmt.Errorf("%w: pending block admission failed: %v", ErrIgnore, retained.lastAttempt.err) //nolint:errorlint // local admission failures must not reject the peer
	}
	if retained != job && job.gossip {
		b.seenBlocksMu.Lock()
		reservation := b.reservations[blockGossipKey(job.block)]
		if reservation != nil && reservation.queued == job {
			reservation.queued = retained
		}
		b.seenBlocksMu.Unlock()
		if retained.terminal {
			b.finishGossipJobLocked(retained, true)
		}
	}
	return nil
}

func (b *blockService) pinGossipJob(job *blockJob) error {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.signedRoot == (common.Hash{}) {
		root, err := job.block.HashSSZ()
		if err != nil {
			return fmt.Errorf("%w: cannot hash pending block: %v", ErrIgnore, err) //nolint:errorlint // local admission failures must not reject the peer
		}
		job.signedRoot = common.Hash(root)
	}
	key := blockGossipKey(job.block)
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	if seen, ok := b.seenBlocksCache.Peek(key); ok && seen.signedRoot != job.signedRoot {
		return fmt.Errorf("%w: another block is already seen for proposer and slot", ErrIgnore)
	}
	reservation := b.reservations[key]
	if reservation == nil {
		reservation = &blockReservation{}
		b.reservations[key] = reservation
	}
	if reservation.pending != nil || (reservation.queued != nil && reservation.queuedRoot != job.signedRoot) {
		return fmt.Errorf("%w: block already reserved for proposer and slot", ErrIgnore)
	}
	if reservation.queued == nil {
		reservation.queued = job
		reservation.queuedRoot = job.signedRoot
	}
	return nil
}

// The caller holds job.mu. Only the job owning the reservation may release it,
// so a late expiry cannot clear a replacement admitted under the same key.
func (b *blockService) finishGossipJobLocked(job *blockJob, completed bool) {
	if !job.gossip {
		return
	}
	key := blockGossipKey(job.block)
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	reservation := b.reservations[key]
	if reservation == nil || reservation.queued != job {
		return
	}
	reservation.queued = nil
	if completed {
		// Keep any replay permission granted by a failed REST publication.
		if _, seen := b.seenBlocksCache.Peek(key); !seen {
			b.seenBlocksCache.Add(key, seenBlock{signedRoot: job.signedRoot})
		}
	} else if seen, ok := b.seenBlocksCache.Peek(key); ok && seen.signedRoot == job.signedRoot {
		b.seenBlocksCache.Remove(key)
	}
	b.cleanupReservationLocked(key, reservation)
}

func (b *blockService) processGossipBlock(ctx context.Context, root [32]byte, job *blockJob) error {
	if err := b.validateBlockAfterSignature(ctx, job.block); err != nil {
		return err
	}
	if b.forkchoiceStore.Slot() < job.block.Block.Slot {
		return forkchoice.ErrBlockTooEarly
	}
	if err := b.processAndStoreBlock(ctx, root, job); err != nil {
		return err
	}
	b.publishGossipJob(root, job)
	return nil
}

func (b *blockService) publishGossipJob(root [32]byte, job *blockJob) {
	job.mu.Lock()
	publish := job.gossip && !job.gossipEventSent
	job.gossipEventSent = true
	job.mu.Unlock()
	if publish {
		b.publishBlockGossipEvent(common.Hash(root), job.block.Block.Slot)
	}
}

func isPendingBlockRetryableError(err error) bool {
	return errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) ||
		errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
		errors.Is(err, forkchoice.ErrNewPayloadNoStatus) ||
		errors.Is(err, forkchoice.ErrParentEnvelopePending) ||
		errors.Is(err, forkchoice.ErrMissingSegment) ||
		errors.Is(err, forkchoice.ErrBlockTooEarly)
}

func (b *blockService) ValidateGossip(ctx context.Context, msg *cltypes.SignedBeaconBlock) error {
	if msg == nil || msg.Block == nil || msg.Block.Body == nil {
		return errors.New("missing beacon block")
	}
	root, err := msg.HashSSZ()
	if err != nil {
		return err
	}
	key := blockGossipKey(msg)
	b.seenBlocksMu.Lock()
	claimed, claimErr := b.claimGossipReplayLocked(key, common.Hash(root))
	b.seenBlocksMu.Unlock()
	if claimErr != nil {
		return claimErr
	}
	if claimed {
		return nil
	}
	if err := b.validateGossip(ctx, msg, nil); err != nil {
		return err
	}
	return b.reserveGossipKey(key, common.Hash(root))
}

func (b *blockService) CommitGossipReservation(msg *cltypes.SignedBeaconBlock) {
	if msg == nil || msg.Block == nil {
		return
	}
	b.commitGossipKey(blockGossipKey(msg))
}

func (b *blockService) ReleaseGossipReservation(msg *cltypes.SignedBeaconBlock) {
	if msg == nil || msg.Block == nil {
		return
	}
	root, err := msg.HashSSZ()
	if err != nil {
		return
	}
	b.releaseGossipKey(blockGossipKey(msg), common.Hash(root))
}

func (b *blockService) validateFirstGossip(ctx context.Context, msg *cltypes.SignedBeaconBlock, schedule func(), waitForPending bool) error {
	key := blockGossipKey(msg)
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: block validation canceled: %w", ErrIgnore, err)
		}
		b.seenBlocksMu.Lock()
		if b.seenBlocksCache.Contains(key) {
			b.seenBlocksMu.Unlock()
			return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
		}
		reservation := b.reservations[key]
		if reservation != nil && reservation.queued != nil {
			b.seenBlocksMu.Unlock()
			return fmt.Errorf("%w: block already queued for proposer and slot", ErrIgnore)
		}
		if reservation != nil && reservation.pending != nil {
			done := reservation.pending
			b.seenBlocksMu.Unlock()
			if !waitForPending {
				return fmt.Errorf("%w: block reservation pending for proposer and slot", ErrIgnore)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: block reservation pending: %w", ErrIgnore, ctx.Err())
			case <-done:
			}
			b.seenBlocksMu.Lock()
			committed := b.seenBlocksCache.Contains(key)
			b.seenBlocksMu.Unlock()
			if committed {
				return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
			}
			continue
		}
		if reservation == nil {
			reservation = &blockReservation{}
			b.reservations[key] = reservation
		}
		reservationVersion := reservation.version
		reservation.validators++
		b.seenBlocksMu.Unlock()

		validationErr := b.validateGossip(ctx, msg, schedule)
		var root [32]byte
		if validationErr == nil && ctx.Err() == nil {
			root, validationErr = msg.HashSSZ()
		}

		b.seenBlocksMu.Lock()
		reservation.validators--
		if err := ctx.Err(); err != nil {
			b.cleanupReservationLocked(key, reservation)
			b.seenBlocksMu.Unlock()
			return fmt.Errorf("%w: block validation canceled: %w", ErrIgnore, err)
		}
		if reservation.version != reservationVersion {
			b.cleanupReservationLocked(key, reservation)
			b.seenBlocksMu.Unlock()
			continue
		}
		if validationErr != nil {
			b.cleanupReservationLocked(key, reservation)
			b.seenBlocksMu.Unlock()
			return validationErr
		}
		if reservation.queued != nil {
			b.seenBlocksMu.Unlock()
			return fmt.Errorf("%w: block already queued for proposer and slot", ErrIgnore)
		}
		if b.seenBlocksCache.Contains(key) || reservation.pending != nil {
			b.cleanupReservationLocked(key, reservation)
			b.seenBlocksMu.Unlock()
			continue
		}
		b.seenBlocksCache.Add(key, seenBlock{signedRoot: common.Hash(root)})
		b.cleanupReservationLocked(key, reservation)
		b.seenBlocksMu.Unlock()
		return nil
	}
}

func (b *blockService) reserveGossipKey(key proposerIndexAndSlot, root common.Hash) error {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	reservation := b.reservations[key]
	claimed, err := b.claimGossipReplayLocked(key, root)
	if err != nil {
		return err
	}
	if claimed {
		return nil
	}
	if reservation != nil && (reservation.pending != nil || reservation.queued != nil) {
		return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
	}
	if reservation == nil {
		reservation = &blockReservation{}
		b.reservations[key] = reservation
	}
	reservation.pending = make(chan struct{})
	reservation.root = root
	reservation.version++
	return nil
}

func (b *blockService) commitGossipKey(key proposerIndexAndSlot) {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	reservation := b.reservations[key]
	if reservation == nil || reservation.pending == nil {
		return
	}
	done := reservation.pending
	reservation.pending = nil
	reservation.version++
	b.seenBlocksCache.Add(key, seenBlock{signedRoot: reservation.root})
	close(done)
	b.cleanupReservationLocked(key, reservation)
}

func (b *blockService) releaseGossipKey(key proposerIndexAndSlot, root common.Hash) {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	reservation := b.reservations[key]
	if reservation == nil || reservation.pending == nil {
		seen, ok := b.seenBlocksCache.Get(key)
		if ok && seen.signedRoot == root {
			seen.replayAllowed = true
			b.seenBlocksCache.Add(key, seen)
		}
		return
	}
	done := reservation.pending
	reservation.pending = nil
	reservation.version++
	close(done)
	b.cleanupReservationLocked(key, reservation)
}

func (b *blockService) claimGossipReplayLocked(key proposerIndexAndSlot, root common.Hash) (bool, error) {
	seen, ok := b.seenBlocksCache.Get(key)
	if !ok {
		return false, nil
	}
	if seen.signedRoot != root || !seen.replayAllowed {
		return false, fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
	}
	seen.replayAllowed = false
	b.seenBlocksCache.Add(key, seen)
	return true, nil
}

func (b *blockService) cleanupReservationLocked(key proposerIndexAndSlot, reservation *blockReservation) {
	if reservation.pending == nil && reservation.queued == nil && reservation.validators == 0 && b.reservations[key] == reservation {
		delete(b.reservations, key)
	}
}

func blockGossipKey(msg *cltypes.SignedBeaconBlock) proposerIndexAndSlot {
	return proposerIndexAndSlot{proposerIndex: msg.Block.ProposerIndex, slot: msg.Block.Slot}
}

func (b *blockService) validateGossip(ctx context.Context, msg *cltypes.SignedBeaconBlock, schedule func()) error {
	if msg == nil || msg.Block == nil || msg.Block.Body == nil {
		return errors.New("missing beacon block")
	}
	if b.syncedData.Syncing() {
		return fmt.Errorf("%w: syncing", ErrIgnore)
	}
	currentSlot := b.syncedData.HeadSlot()
	if currentSlot < msg.Block.Slot && !b.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.Block.Slot) {
		return fmt.Errorf("%w: block is not from a future slot: %d > %d", ErrIgnore, currentSlot, msg.Block.Slot)
	}
	if b.beaconCfg.SlotsPerEpoch == 0 {
		return errors.New("slots per epoch is zero")
	}
	epoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch
	blockVersion := b.beaconCfg.GetCurrentStateVersion(epoch)
	if blockVersion >= clparams.GloasVersion {
		if err := validateGloasBlockBodyLimits(b.beaconCfg, msg.Block.Body); err != nil {
			return err
		}
	}
	finalizedCheckpoint := b.forkchoiceStore.FinalizedCheckpoint()

	if err := b.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The block is from a slot greater than the latest finalized slot -- i.e. validate that signed_beacon_block.message.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
		// (a client MAY choose to validate and store such blocks for additional purposes -- e.g. slashing detection, archive nodes, etc).
		finalizedStartSlot, ok := safeMultiplyUint64(finalizedCheckpoint.Epoch, b.beaconCfg.SlotsPerEpoch)
		if !ok {
			return errors.New("finalized checkpoint slot is not representable")
		}
		if msg.Block.Slot <= finalizedStartSlot {
			return fmt.Errorf("%w: block slot %d is not after finalized slot %d", ErrIgnore, msg.Block.Slot, finalizedStartSlot)
		}
		if ok, err := eth2.VerifyBlockSignature(headState, msg); err != nil {
			return err
		} else if !ok {
			return ErrInvalidSignature
		}
		return nil
	}); err != nil {
		return err
	}
	err := b.validateBlockAfterSignature(ctx, msg)
	if errors.Is(err, ErrIgnore) && schedule != nil {
		schedule()
	}
	return err
}

// Deferred blocks repeat dependency-sensitive gossip checks before any database
// write. Their signature was already checked at admission.
func (b *blockService) validateBlockAfterSignature(ctx context.Context, msg *cltypes.SignedBeaconBlock) error {
	epoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch
	blockVersion := b.beaconCfg.GetCurrentStateVersion(epoch)
	finalizedCheckpoint := b.forkchoiceStore.FinalizedCheckpoint()

	// [IGNORE] The block's parent (defined by block.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue blocks for processing once the parent block is retrieved).
	parentHeader, ok := b.forkchoiceStore.GetHeader(msg.Block.ParentRoot)
	if !ok {
		return fmt.Errorf("%w: parent header not found: %v", ErrIgnore, msg.Block.ParentRoot)
	}
	if parentHeader.Slot >= msg.Block.Slot {
		return ErrBlockYoungerThanParent
	}
	var gloasBid *cltypes.ExecutionPayloadBid
	parentIsFull := false
	if blockVersion >= clparams.GloasVersion {
		signedBid := msg.Block.Body.GetSignedExecutionPayloadBid()
		if signedBid == nil || signedBid.Message == nil {
			return errors.New("missing signed_execution_payload_bid in GLOAS block")
		}
		gloasBid = signedBid.Message
	}
	finalizedSlot, ok := safeMultiplyUint64(finalizedCheckpoint.Epoch, b.beaconCfg.SlotsPerEpoch)
	if !ok {
		return errors.New("finalized checkpoint slot is not representable")
	}
	if anchorSlot := b.forkchoiceStore.AnchorSlot(); finalizedSlot < anchorSlot {
		finalizedSlot = anchorSlot
	}
	if b.forkchoiceStore.Ancestor(msg.Block.ParentRoot, finalizedSlot).Root != finalizedCheckpoint.Root {
		return errors.New("finalized checkpoint is not an ancestor of block")
	}
	validationContext, err := b.blockValidationContext(ctx, msg.Block.ParentRoot, msg.Block.Slot)
	if err != nil {
		return err
	}
	if blockVersion >= clparams.GloasVersion {
		var parentBidBlockHash common.Hash
		var hasParentBid bool
		parentBlock, ok := b.forkchoiceStore.GetBlock(msg.Block.ParentRoot)
		switch {
		case ok && parentBlock != nil && parentBlock.Block != nil && parentBlock.Block.Body != nil:
			parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
			if parentBid != nil && parentBid.Message != nil {
				parentBidBlockHash = parentBid.Message.BlockHash
				hasParentBid = true
			}
		case msg.Block.ParentRoot == b.forkchoiceStore.AnchorRoot():
			parentBidBlockHash = validationContext.latestExecutionPayloadBidBlockHash
			hasParentBid = validationContext.hasLatestExecutionPayloadBid
		default:
			return errors.New("parent block not found")
		}
		parentIsFull = hasParentBid && gloasBid.ParentBlockHash == parentBidBlockHash
		if parentIsFull {
			status, seen := b.forkchoiceStore.GetRecentExecutionPayloadStatusByRoot(msg.Block.ParentRoot)
			if !seen || (status != execution_client.PayloadStatusValidated && status != execution_client.PayloadStatusNotValidated) {
				return fmt.Errorf("%w: parent payload is not verified", ErrIgnore)
			}
		}
	}
	if msg.Block.ProposerIndex != validationContext.expectedProposer {
		return fmt.Errorf("block proposer index %d does not match expected proposer %d", msg.Block.ProposerIndex, validationContext.expectedProposer)
	}

	var maxBlobsPerBlock uint64
	if blockVersion >= clparams.FuluVersion {
		maxBlobsPerBlock = b.beaconCfg.GetBlobParameters(epoch).MaxBlobsPerBlock
	} else {
		maxBlobsPerBlock = b.beaconCfg.MaxBlobsPerBlockByVersion(blockVersion)
	}

	// [Modified in Gloas:EIP7732] KZG commitments and execution payload validations moved from block.body to bid
	if blockVersion >= clparams.GloasVersion {
		// GLOAS: validate using bid = signed_execution_payload_bid.message
		// [REJECT] The length of KZG commitments is less than or equal to the limitation defined in Consensus Layer
		// i.e. validate that len(bid.blob_kzg_commitments) <= get_blob_parameters(get_current_epoch(state)).max_blobs_per_block
		if gloasBid.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
			return ErrInvalidCommitmentsCount
		}

		// [REJECT] The bid's parent (defined by bid.parent_block_root) equals the block's parent (defined by block.parent_root)
		if gloasBid.ParentBlockRoot != msg.Block.ParentRoot {
			return errors.New("bid.parent_block_root does not match block.parent_root")
		}

		if !parentIsFull && gloasBid.ParentBlockHash != validationContext.latestBlockHash {
			return errors.New("bid does not build on the parent's execution head")
		}
	} else if msg.Block.Body.BlobKzgCommitments != nil && msg.Block.Body.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
		// Pre-GLOAS: [REJECT] The length of KZG commitments is less than or equal to the limitation defined in Consensus Layer
		// i.e. validate that len(body.signed_beacon_block.message.blob_kzg_commitments) <= MAX_BLOBS_PER_BLOCK
		return ErrInvalidCommitmentsCount
	}
	return nil
}

func (b *blockService) blockValidationContext(ctx context.Context, parentRoot common.Hash, slot uint64) (*blockValidationContext, error) {
	key := blockValidationContextKey{parentRoot: parentRoot, slot: slot}
	b.validationMu.Lock()
	if validationContext, ok := b.validationCache.Get(key); ok {
		b.validationMu.Unlock()
		return validationContext, nil
	}
	call := b.validationCalls[key]
	b.validationMu.Unlock()
	if call != nil {
		return waitForBlockValidationContext(ctx, call)
	}

	select {
	case b.validationSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: block validation canceled: %w", ErrIgnore, ctx.Err())
	}

	b.validationMu.Lock()
	if validationContext, ok := b.validationCache.Get(key); ok {
		b.validationMu.Unlock()
		<-b.validationSlots
		return validationContext, nil
	}
	if call = b.validationCalls[key]; call != nil {
		b.validationMu.Unlock()
		<-b.validationSlots
		return waitForBlockValidationContext(ctx, call)
	}
	call = &blockValidationContextCall{done: make(chan struct{})}
	b.validationCalls[key] = call
	b.validationMu.Unlock()

	finished := false
	defer func() {
		if !finished {
			b.finishBlockValidationContext(key, call, nil, fmt.Errorf("%w: parent state validation panicked", ErrIgnore))
		}
		<-b.validationSlots
	}()
	validationContext, err := b.computeBlockValidationContext(parentRoot, slot)
	b.finishBlockValidationContext(key, call, validationContext, err)
	finished = true
	return validationContext, err
}

func waitForBlockValidationContext(ctx context.Context, call *blockValidationContextCall) (*blockValidationContext, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: block validation canceled: %w", ErrIgnore, ctx.Err())
	case <-call.done:
		return call.context, call.err
	}
}

func (b *blockService) computeBlockValidationContext(parentRoot common.Hash, slot uint64) (*blockValidationContext, error) {
	parentState, err := b.forkchoiceStore.GetStateAtBlockRoot(parentRoot, true)
	if err != nil {
		return nil, fmt.Errorf("%w: get parent block state: %w", ErrIgnore, err)
	}
	if parentState == nil {
		return nil, fmt.Errorf("%w: parent block state not found", ErrIgnore)
	}
	validationContext := &blockValidationContext{}
	if bid := parentState.GetLatestExecutionPayloadBid(); bid != nil {
		validationContext.latestExecutionPayloadBidBlockHash = bid.BlockHash
		validationContext.hasLatestExecutionPayloadBid = true
	}
	if err := transition.DefaultMachine.ProcessSlots(parentState, slot); err != nil {
		return nil, fmt.Errorf("%w: process parent state to block slot: %w", ErrIgnore, err)
	}
	validationContext.latestBlockHash = parentState.GetLatestBlockHash()
	validationContext.expectedProposer, err = parentState.GetBeaconProposerIndexForSlot(slot)
	if err != nil {
		return nil, fmt.Errorf("%w: get expected proposer: %w", ErrIgnore, err)
	}
	return validationContext, nil
}

func (b *blockService) finishBlockValidationContext(key blockValidationContextKey, call *blockValidationContextCall, validationContext *blockValidationContext, err error) {
	b.validationMu.Lock()
	defer b.validationMu.Unlock()
	if b.validationCalls[key] != call {
		return
	}
	call.context = validationContext
	call.err = err
	delete(b.validationCalls, key)
	if err == nil {
		b.validationCache.Add(key, validationContext)
	}
	close(call.done)
}

func validateGloasBlockBodyLimits(cfg *clparams.BeaconChainConfig, body *cltypes.BeaconBody) error {
	if cfg == nil || body == nil {
		return errors.New("missing Gloas block body configuration")
	}
	if body.ProposerSlashings == nil || body.AttesterSlashings == nil || body.Attestations == nil || body.Deposits == nil ||
		body.VoluntaryExits == nil || body.ExecutionChanges == nil || body.PayloadAttestations == nil {
		return errors.New("missing Gloas block body operation list")
	}
	checks := []struct {
		name  string
		count int
		limit uint64
	}{
		{"proposer slashings", body.ProposerSlashings.Len(), cfg.MaxProposerSlashings},
		{"attester slashings", body.AttesterSlashings.Len(), cfg.MaxAttesterSlashingsElectra},
		{"attestations", body.Attestations.Len(), cfg.MaxAttestationsElectra},
		{"voluntary exits", body.VoluntaryExits.Len(), cfg.MaxVoluntaryExits},
		{"BLS to execution changes", body.ExecutionChanges.Len(), cfg.MaxBlsToExecutionChanges},
		{"payload attestations", body.PayloadAttestations.Len(), cfg.MaxPayloadAttestations},
	}
	if body.Deposits.Len() != 0 {
		return fmt.Errorf("deposits count %d exceeds Gloas limit 0", body.Deposits.Len())
	}
	for _, check := range checks {
		if uint64(check.count) > check.limit {
			return fmt.Errorf("%s count %d exceeds limit %d", check.name, check.count, check.limit)
		}
	}
	return validateExecutionRequestsLimits(cfg, body.ParentExecutionRequests)
}

func validateExecutionRequestsLimits(cfg *clparams.BeaconChainConfig, requests *cltypes.ExecutionRequests) error {
	if cfg == nil || requests == nil {
		return errors.New("missing execution requests")
	}
	if requests.Deposits == nil || requests.Withdrawals == nil || requests.Consolidations == nil ||
		requests.BuilderDeposits == nil || requests.BuilderExits == nil {
		return errors.New("missing execution request list")
	}
	checks := []struct {
		name  string
		count int
		limit uint64
	}{
		{"withdrawal requests", requests.Withdrawals.Len(), cfg.MaxWithdrawalRequestsPerPayload},
		{"consolidation requests", requests.Consolidations.Len(), cfg.MaxConsolidationRequestsPerPayload},
		{"builder deposit requests", requests.BuilderDeposits.Len(), cfg.MaxBuilderDepositRequestsPerPayload},
		{"builder exit requests", requests.BuilderExits.Len(), cfg.MaxBuilderExitRequestsPerPayload},
	}
	for _, check := range checks {
		if uint64(check.count) > check.limit {
			return fmt.Errorf("%s count %d exceeds limit %d", check.name, check.count, check.limit)
		}
	}
	return nil
}

// publishBlockGossipEvent runs after the block has passed rejection-grade validation.
func (b *blockService) publishBlockGossipEvent(root common.Hash, slot uint64) {
	if b.emitter != nil {
		b.emitter.State().SendBlockGossip(&beaconevents.BlockGossipData{Slot: slot, Block: root})
	}
}

// ScheduleBlockForLaterProcessing schedules a block for later processing.
func (b *blockService) ScheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) {
	b.scheduleBlockForLaterProcessing(block, nil)
}

func (b *blockService) SchedulePublishedBlockForLaterProcessing(block *cltypes.SignedBeaconBlock, store func(context.Context) error) PublishedBlockJob {
	job, generation := b.scheduleBlockForLaterProcessing(block, store)
	return &publishedBlockJobHandle{job: job, generation: generation}
}

func (b *blockService) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock, store func(context.Context) error) (*blockJob, uint64) {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		job := newFailedBlockJob(block, store, err)
		return job, job.storeGeneration
	}

	return b.scheduleBlockJob(blockRoot, newBlockJob(block, store))
}

func (b *blockService) scheduleBlockJob(blockRoot [32]byte, job *blockJob) (*blockJob, uint64) {
	block, store := job.block, job.store
	jobGeneration := job.storeGeneration
	b.blockJobsLifecycleMu.RLock()
	defer b.blockJobsLifecycleMu.RUnlock()
	if b.blockJobsStopped {
		job = newFailedBlockJob(block, store, ErrPublishedBlockJobStopped)
		return job, job.storeGeneration
	}
	for {
		existingJob, err := b.blocksScheduledForLaterExecution.enqueueKey(blockRoot, job)
		if err != nil {
			log.Debug("Pending block admission failed", "slot", block.Block.Slot, "error", err)
			job = newFailedBlockJob(block, store, err)
			return job, job.storeGeneration
		}
		if existingJob == job {
			log.Trace("Block scheduled for later processing", "slot", block.Block.Slot, "blockRoot", blockRoot)
			return job, jobGeneration
		}
		existing, generation := b.reuseScheduledBlockJob(blockRoot, existingJob, job, store)
		if existing != nil {
			return existing, generation
		}
	}
}

func (b *blockService) reuseScheduledBlockJob(key [32]byte, existing, job *blockJob, store func(context.Context) error) (*blockJob, uint64) {
	mergeBlockProcessingState(existing, job)
	existing.mu.Lock()
	defer existing.mu.Unlock()
	current, ok := b.blocksScheduledForLaterExecution.jobs.Load(key)
	if !ok || current.(*pendingJob[*blockJob]).msg != existing {
		return nil, 0
	}
	if store == nil {
		return existing, existing.storeGeneration
	}
	if job.scheduleSequence <= existing.scheduleSequence {
		return existing, existing.storeGeneration
	}
	// Replacing the queue entry makes an expiry sampled before this refresh
	// harmless: identity-checked removal cannot delete the new generation.
	refreshedAt := time.Now()
	refreshed := &pendingJob[*blockJob]{msg: existing, creationTime: refreshedAt}
	if !b.blocksScheduledForLaterExecution.jobs.CompareAndSwap(key, current, refreshed) {
		return nil, 0
	}
	existing.store = store
	existing.storeGeneration++
	existing.scheduleSequence = job.scheduleSequence
	existing.creationTime = refreshedAt
	if existing.terminal {
		existing.terminal = false
		existing.attempt = &blockJobAttempt{done: make(chan struct{})}
	}
	return existing, existing.storeGeneration
}

func (b *blockService) processAndStoreBlock(ctx context.Context, root [32]byte, job *blockJob) error {
	block := job.block
	persisted, executionAndDataChecked := job.processingState()
	if !persisted {
		if err := b.db.View(ctx, func(tx kv.Tx) error {
			slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
			persisted = slot != nil
			return err
		}); err != nil {
			return fmt.Errorf("%w: cannot read block storage: %v", ErrIgnore, err) //nolint:errorlint // local database errors are retryable, not invalid gossip
		}
		if !persisted {
			if err := b.db.Update(ctx, func(tx kv.RwTx) error {
				return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
			}); err != nil {
				return fmt.Errorf("%w: cannot store block: %v", ErrIgnore, err) //nolint:errorlint // local database errors are retryable, not invalid gossip
			}
		}
		job.markPersisted()
	}

	if _, exists := b.forkchoiceStore.GetHeader(root); !exists {
		if err := b.forkchoiceStore.OnBlock(ctx, block, !executionAndDataChecked, true, !executionAndDataChecked); err != nil {
			job.mu.Lock()
			// Only OnBlock's MissingSegment result guarantees that execution
			// and data checks completed; a publication callback may fail earlier.
			if errors.Is(err, forkchoice.ErrMissingSegment) {
				job.executionAndDataChecked = true
			}
			job.recordProcessingFailureLocked(time.Now(), err)
			job.mu.Unlock()
			return err
		}
		go b.importBlockOperations(block)
	}
	if err := b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, b.forkchoiceStore.FinalizedSlot())
	}); err != nil {
		// Fork choice has accepted the block; an auxiliary index failure must
		// not become a peer-level rejection.
		log.Warn("Failed to update highest finalized block after import", "slot", block.Block.Slot, "error", err)
	}
	return nil
}

// importBlockOperations imports block operations in parallel
func (b *blockService) importBlockOperations(block *cltypes.SignedBeaconBlock) {
	defer func() { // Would prefer this not to crash but rather log the error
		r := recover()
		if r != nil {
			log.Warn("recovered from panic", "err", r)
		}
	}()
	start := time.Now()
	block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
		if err := b.forkchoiceStore.OnAttestation(a, true, false); err != nil {
			log.Debug("bad attestation received", "err", err)
		}

		return true
	})
	block.Block.Body.AttesterSlashings.Range(func(idx int, a *cltypes.AttesterSlashing, total int) bool {
		if err := b.forkchoiceStore.OnAttesterSlashing(a, false); err != nil && !errors.Is(err, forkchoice.ErrIgnore) {
			log.Debug("bad attester slashing received", "err", err)
		}
		return true
	})
	log.Trace("import operations", "time", time.Since(start))
}

func (b *blockService) stopPublishedBlockJobsOnContext(ctx context.Context) {
	<-ctx.Done()
	b.stopPublishedBlockJobs()
}

func (b *blockService) stopPublishedBlockJobs() {
	b.blockJobsLifecycleMu.Lock()
	if b.blockJobsStopped {
		b.blockJobsLifecycleMu.Unlock()
		return
	}
	b.blockJobsStopped = true
	b.blockJobsLifecycleMu.Unlock()

	b.blocksScheduledForLaterExecution.jobs.Range(func(key, value any) bool {
		job := value.(*pendingJob[*blockJob]).msg
		job.mu.Lock()
		current, ok := b.blocksScheduledForLaterExecution.jobs.Load(key)
		if ok && current.(*pendingJob[*blockJob]).msg == job {
			finishBlockJobLocked(job, ErrPublishedBlockJobStopped)
			b.finishGossipJobLocked(job, false)
			b.blocksScheduledForLaterExecution.remove(key.([32]byte), current.(*pendingJob[*blockJob]))
		}
		job.mu.Unlock()
		return true
	})
}

func (b *blockService) processScheduledBlock(ctx context.Context, key [32]byte, job *blockJob, now time.Time) {
	job.mu.Lock()
	if job.running {
		job.mu.Unlock()
		return
	}
	if now.Sub(job.creationTime) > blockJobExpiry {
		finishBlockJobLocked(job, ErrPublishedBlockJobExpired)
		b.finishGossipJobLocked(job, false)
		b.removeScheduledBlockLocked(key, job)
		job.mu.Unlock()
		return
	}
	if job.terminal || !job.readyToRetryLocked(now) {
		job.mu.Unlock()
		return
	}
	job.running = true
	store := job.store
	gossip := job.gossip
	generation := job.storeGeneration
	attempt := job.attempt
	job.mu.Unlock()
	var err error
	switch {
	case store != nil:
		err = store(ctx)
	case gossip:
		err = b.processGossipBlock(ctx, key, job)
	default:
		err = b.processAndStoreBlock(ctx, key, job)
	}
	job.mu.Lock()
	job.running = false
	if job.terminal && job.completedGeneration >= generation {
		job.mu.Unlock()
		return
	}
	attempt.err = err
	attempt.generation = generation
	close(attempt.done)
	job.lastAttempt = attempt
	latest := generation == job.storeGeneration
	if latest && store != nil && err != nil {
		job.recordProcessingFailureLocked(time.Now(), err)
	}
	permanent := errors.Is(err, forkchoice.ErrBlockInvalid)
	if store == nil && gossip {
		permanent = err != nil && !isPendingBlockRetryableError(err) && !errors.Is(err, ErrIgnore)
	}
	terminal := latest && (err == nil || permanent)
	if terminal {
		job.completedGeneration = generation
		job.terminal = true
	} else {
		job.attempt = &blockJobAttempt{done: make(chan struct{})}
	}
	if terminal {
		b.finishGossipJobLocked(job, true)
		b.removeScheduledBlockLocked(key, job)
	}
	job.mu.Unlock()
	if err != nil {
		log.Trace("Failed to process and store block", "block", job.block, "error", err)
		return
	}
	if terminal {
		b.publishGossipJob(key, job)
	}
}

func finishBlockJobLocked(job *blockJob, err error) {
	if job.terminal {
		return
	}
	job.attempt.err = err
	job.attempt.generation = job.storeGeneration
	job.lastAttempt = job.attempt
	job.completedGeneration = job.storeGeneration
	job.terminal = true
	close(job.attempt.done)
}

func (b *blockService) removeScheduledBlockLocked(key [32]byte, job *blockJob) {
	entry, ok := b.blocksScheduledForLaterExecution.jobs.Load(key)
	if ok && entry.(*pendingJob[*blockJob]).msg == job {
		b.blocksScheduledForLaterExecution.remove(key, entry.(*pendingJob[*blockJob]))
	}
}
