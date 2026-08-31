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
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

var ErrInvalidSignature = errors.New("invalid signature")

type proposerIndexAndSlot struct {
	proposerIndex uint64
	slot          uint64
}

type pendingBlockJob struct {
	// mu protects the mutable retry and processing state below. block is immutable.
	mu                      sync.Mutex
	block                   *cltypes.SignedBeaconBlock
	persisted               bool
	executionAndDataChecked bool
	processingFailureAt     time.Time
	retryAfter              time.Time
	retryDelay              time.Duration
}

func (job *pendingBlockJob) readyToRetry(now time.Time) bool {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.retryAfter.IsZero() || !now.Before(job.retryAfter)
}

func (job *pendingBlockJob) recordProcessingFailure(now time.Time, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.recordProcessingFailureLocked(now, err)
}

func (job *pendingBlockJob) recordProcessingFailureLocked(now time.Time, err error) {
	job.processingFailureAt = now
	// MissingSegment is returned only after execution and data checks complete,
	// so later state retries can skip those expensive phases.
	if errors.Is(err, forkchoice.ErrMissingSegment) {
		job.executionAndDataChecked = true
	}
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

func (job *pendingBlockJob) processingState() (persisted, executionAndDataChecked bool) {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.persisted, job.executionAndDataChecked
}

func (job *pendingBlockJob) markPersisted() {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.persisted = true
}

// A duplicate delivery preserves completed work and the original admission
// time. The newest failure controls the next retry, while an existing EL delay
// remains the base of exponential backoff.
func mergePendingBlockJobs(existing, incoming *pendingBlockJob) {
	if existing == incoming {
		return
	}
	incoming.mu.Lock()
	incomingPersisted := incoming.persisted
	incomingExecutionAndDataChecked := incoming.executionAndDataChecked
	incomingProcessingFailureAt := incoming.processingFailureAt
	incomingRetryAfter := incoming.retryAfter
	incomingRetryDelay := incoming.retryDelay
	incoming.mu.Unlock()

	existing.mu.Lock()
	defer existing.mu.Unlock()
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

type blockService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	syncedData      *synced_data.SyncedDataManager
	ethClock        eth_clock.EthereumClock
	beaconCfg       *clparams.BeaconChainConfig

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
	seenBlocksMu    sync.Mutex
	seenBlocksCache *lru.Cache[proposerIndexAndSlot, common.Hash]
	// Reservations must not be evicted while a block is being validated or queued;
	// otherwise a later equivocation could replace the first validly signed block.
	seenBlockReservations map[proposerIndexAndSlot]common.Hash
	emitter               *beaconevents.EventEmitter

	// Blocks waiting for their slot or a processing dependency.
	blocksScheduledForLaterExecution *pendingJobQueue[common.Hash, *pendingBlockJob]
	db                               kv.RwDB
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
	seenBlocksCache, err := lru.New[proposerIndexAndSlot, common.Hash]("seenblocks", seenBlockCacheSize)
	if err != nil {
		panic(err)
	}
	b := &blockService{
		forkchoiceStore:       forkchoiceStore,
		syncedData:            syncedData,
		ethClock:              ethClock,
		beaconCfg:             beaconCfg,
		seenBlocksCache:       seenBlocksCache,
		seenBlockReservations: make(map[proposerIndexAndSlot]common.Hash, maxPendingBlocks),
		emitter:               emitter,
		db:                    db,
	}
	b.blocksScheduledForLaterExecution = b.newPendingBlockQueue(ctx)
	return b
}

func (b *blockService) newPendingBlockQueue(ctx context.Context) *pendingJobQueue[common.Hash, *pendingBlockJob] {
	return newPendingJobQueue(ctx, pendingJobQueueOptions{
		name:          "beacon_block",
		capacity:      maxPendingBlocks,
		expiry:        blockJobExpiry,
		checkInterval: blockJobsIntervalTick,
	},
		b.tryProcessPendingBlock,
		nil,
		func(blockRoot common.Hash, job *pendingBlockJob) {
			b.releaseSeenBlockReservation(pendingBlockSeenKey(job.block), blockRoot)
			log.Trace("Pending block expired", "blockRoot", blockRoot)
		},
		mergePendingBlockJobs)
}

func (b *blockService) Names() []string {
	return []string{gossip.TopicNameBeaconBlock}
}

func (b *blockService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameBeaconBlock
}

func (b *blockService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedBeaconBlock, error) {
	obj := cltypes.NewSignedBeaconBlock(b.beaconCfg, version)
	if err := obj.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

// ProcessMessage processes a block message according to https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
func (b *blockService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.SignedBeaconBlock) error {
	log.Trace("Received block via gossip", "slot", msg.Block.Slot)
	blockEpoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch

	if b.syncedData.Syncing() {
		return fmt.Errorf("%w: syncing", ErrIgnore)
	}

	currentSlot := b.syncedData.HeadSlot()

	// [IGNORE] The block is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. validate that
	// signed_beacon_block.message.slot <= current_slot (a client MAY queue future blocks for processing at the appropriate slot).
	if currentSlot < msg.Block.Slot && !b.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.Block.Slot) {
		return fmt.Errorf("%w: block is not from a future slot: %d > %d", ErrIgnore, currentSlot, msg.Block.Slot)
	}

	// [IGNORE] The block is the first block with valid signature received for the proposer for the slot, signed_beacon_block.message.slot.
	seenKey := pendingBlockSeenKey(msg)
	if b.hasSeenBlock(seenKey) {
		return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
	}

	if err := b.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The block is from a slot greater than the latest finalized slot -- i.e. validate that signed_beacon_block.message.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
		// (a client MAY choose to validate and store such blocks for additional purposes -- e.g. slashing detection, archive nodes, etc).
		if blockEpoch <= headState.FinalizedCheckpoint().Epoch {
			return fmt.Errorf("%w: block is not from a slot greater than the latest finalized slot: %d > %d", ErrIgnore, blockEpoch, headState.FinalizedCheckpoint().Epoch)
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
	blockRoot, err := msg.Block.HashSSZ()
	if err != nil {
		return err
	}
	root := common.Hash(blockRoot)
	if _, alreadySeen := b.loadOrReserveSeenBlock(seenKey, root); alreadySeen {
		return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
	}

	if err := b.validateBlockAfterSignature(msg); err != nil {
		if errors.Is(err, ErrIgnore) {
			if scheduleErr := b.schedulePendingBlockWithRoot(root, &pendingBlockJob{block: msg}); scheduleErr != nil {
				b.releaseSeenBlockReservation(seenKey, root)
				return scheduleErr
			}
		} else {
			b.completeSeenBlock(seenKey, root)
		}
		return err
	}
	if b.forkchoiceStore.Slot() < msg.Block.Slot {
		if err := b.schedulePendingBlockWithRoot(root, &pendingBlockJob{block: msg}); err != nil {
			b.releaseSeenBlockReservation(seenKey, root)
			return err
		}
		return fmt.Errorf("%w: block queued until fork choice reaches slot %d", ErrIgnore, msg.Block.Slot)
	}
	if _, ok := b.forkchoiceStore.GetHeader(blockRoot); ok {
		b.completeSeenBlock(seenKey, root)
		b.publishBlockGossipEvent(root, msg.Block.Slot)
		return nil
	}
	if err := b.storeBlock(ctx, msg); err != nil {
		if scheduleErr := b.schedulePendingBlockWithRoot(root, &pendingBlockJob{block: msg}); scheduleErr != nil {
			b.releaseSeenBlockReservation(seenKey, root)
			return scheduleErr
		}
		return fmt.Errorf("%w: block queued after a local storage failure: %v", ErrIgnore, err) //nolint:errorlint // converting a local failure to IGNORE
	}
	// Fork choice performs the remaining block validation.
	if err := b.processStoredBlock(ctx, msg, true); err != nil {
		if isPendingBlockRetryableError(err) {
			if scheduleErr := b.scheduleBlockAfterProcessingFailure(root, msg, err); scheduleErr != nil {
				b.releaseSeenBlockReservation(seenKey, root)
				return scheduleErr
			}
			return fmt.Errorf("%w: block queued while a processing dependency is unavailable: %v", ErrIgnore, err) //nolint:errorlint // fork-choice sentinels must not stay matchable
		}
		b.completeSeenBlock(seenKey, root)
		return err
	}
	b.completeSeenBlock(seenKey, root)
	b.publishBlockGossipEvent(root, msg.Block.Slot)
	return nil
}

// validateBlockAfterSignature keeps post-signature gossip checks identical for
// initial and deferred processing, so a missing dependency cannot bypass validation.
func (b *blockService) validateBlockAfterSignature(block *cltypes.SignedBeaconBlock) error {
	// [IGNORE] The block's parent (defined by block.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue blocks for processing once the parent block is retrieved).
	parentHeader, ok := b.forkchoiceStore.GetHeader(block.Block.ParentRoot)
	if !ok {
		return fmt.Errorf("%w: parent header not found: %v", ErrIgnore, block.Block.ParentRoot)
	}
	if parentHeader.Slot >= block.Block.Slot {
		return ErrBlockYoungerThanParent
	}

	epoch := block.Block.Slot / b.beaconCfg.SlotsPerEpoch
	blockVersion := b.beaconCfg.GetCurrentStateVersion(epoch)
	var maxBlobsPerBlock uint64
	if blockVersion >= clparams.FuluVersion {
		maxBlobsPerBlock = b.beaconCfg.GetBlobParameters(epoch).MaxBlobsPerBlock
	} else {
		maxBlobsPerBlock = b.beaconCfg.MaxBlobsPerBlockByVersion(blockVersion)
	}

	// [Modified in Gloas:EIP7732] KZG commitments and execution payload validations moved from block.body to bid
	if blockVersion >= clparams.GloasVersion {
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			return errors.New("missing signed_execution_payload_bid in GLOAS block")
		}
		if bid.Message.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
			return ErrInvalidCommitmentsCount
		}
		if bid.Message.ParentBlockRoot != block.Block.ParentRoot {
			return errors.New("bid.parent_block_root does not match block.parent_root")
		}

		parentBlockHash := bid.Message.ParentBlockHash
		// Gossip requires the parent payload to be known, not necessarily fully validated.
		// An absent status is retryable; an invalid status permanently rejects the block.
		status, seen := b.forkchoiceStore.GetRecentExecutionPayloadStatus(parentBlockHash)
		if !seen {
			return fmt.Errorf("%w: parent execution payload not seen: %v", ErrIgnore, parentBlockHash)
		}
		if status == execution_client.PayloadStatusInvalidated {
			return errors.New("parent execution payload is invalid")
		}
		return nil
	}

	if block.Block.Body.BlobKzgCommitments != nil && block.Block.Body.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
		return ErrInvalidCommitmentsCount
	}
	return nil
}

func pendingBlockSeenKey(block *cltypes.SignedBeaconBlock) proposerIndexAndSlot {
	return proposerIndexAndSlot{
		proposerIndex: block.Block.ProposerIndex,
		slot:          block.Block.Slot,
	}
}

func (b *blockService) hasSeenBlock(key proposerIndexAndSlot) bool {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	_, ok := b.seenBlockRootLocked(key)
	return ok
}

func (b *blockService) seenBlockRootLocked(key proposerIndexAndSlot) (common.Hash, bool) {
	if blockRoot, ok := b.seenBlockReservations[key]; ok {
		return blockRoot, true
	}
	return b.seenBlocksCache.Peek(key)
}

// loadOrReserveSeenBlock atomically reserves a proposer and slot for the first
// block with a valid signature. Active reservations stay outside the evicting
// history cache until validation and any deferred processing finish.
func (b *blockService) loadOrReserveSeenBlock(
	key proposerIndexAndSlot,
	blockRoot common.Hash,
) (seenRoot common.Hash, alreadySeen bool) {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	if seenRoot, ok := b.seenBlockRootLocked(key); ok {
		return seenRoot, true
	}
	b.seenBlockReservations[key] = blockRoot
	return blockRoot, false
}

// completeSeenBlock moves a matching active reservation into bounded history.
func (b *blockService) completeSeenBlock(key proposerIndexAndSlot, blockRoot common.Hash) {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	if reservedRoot, ok := b.seenBlockReservations[key]; ok {
		if reservedRoot != blockRoot {
			return
		}
		delete(b.seenBlockReservations, key)
	} else if seenRoot, ok := b.seenBlocksCache.Peek(key); ok && seenRoot != blockRoot {
		return
	}
	b.seenBlocksCache.Add(key, blockRoot)
}

// releaseSeenBlockReservation removes only the matching active reservation.
func (b *blockService) releaseSeenBlockReservation(key proposerIndexAndSlot, blockRoot common.Hash) {
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	if reservedRoot, ok := b.seenBlockReservations[key]; ok && reservedRoot == blockRoot {
		delete(b.seenBlockReservations, key)
	}
}

// publishBlockGossipEvent must run only after rejection-grade beacon_block
// topic checks have completed.
func (b *blockService) publishBlockGossipEvent(blockRoot common.Hash, slot uint64) {
	if b.emitter == nil {
		return
	}
	b.emitter.State().SendBlockGossip(&beaconevents.BlockGossipData{
		Slot:  slot,
		Block: blockRoot,
	})
}

func (b *blockService) scheduleBlockAfterProcessingFailure(
	blockRoot common.Hash,
	block *cltypes.SignedBeaconBlock,
	processingErr error,
) error {
	// Retryable processing errors are returned only after the block transaction
	// commits, so deferred attempts must not write the same block again.
	job := &pendingBlockJob{block: block, persisted: true}
	job.recordProcessingFailure(time.Now(), processingErr)
	return b.schedulePendingBlockWithRoot(blockRoot, job)
}

func (b *blockService) schedulePendingBlockWithRoot(blockRoot common.Hash, job *pendingBlockJob) error {
	result := b.blocksScheduledForLaterExecution.enqueueKey(blockRoot, job)
	return b.finishPendingBlockSchedule(job.block, result)
}

func (b *blockService) finishPendingBlockSchedule(
	block *cltypes.SignedBeaconBlock,
	result pendingJobEnqueueResult,
) error {
	// Gloas blocks do not carry an execution payload in the block body.
	var blockNum uint64
	if block.Block.Body.ExecutionPayload != nil {
		blockNum = block.Block.Body.ExecutionPayload.BlockNumber
	}
	switch result {
	case pendingJobEnqueued:
		log.Trace("Block scheduled for later processing", "slot", block.Block.Slot, "block", blockNum)
	case pendingJobQueueFull:
		log.Debug("Pending block queue full; block not scheduled", "slot", block.Block.Slot, "block", blockNum)
		return fmt.Errorf("%w: pending block queue is full", ErrIgnore)
	}
	return nil
}

func (b *blockService) storeBlock(ctx context.Context, block *cltypes.SignedBeaconBlock) error {
	return b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	})
}

func (b *blockService) processStoredBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, checkExecutionAndData bool) error {
	if err := b.forkchoiceStore.OnBlock(ctx, block, checkExecutionAndData, true, checkExecutionAndData); err != nil {
		return err
	}
	go b.importBlockOperations(block)
	if err := b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, b.forkchoiceStore.FinalizedSlot())
	}); err != nil {
		// Fork choice has already accepted the block. An auxiliary index failure
		// must not turn a valid gossip message into a peer-level rejection.
		log.Warn("Failed to update highest finalized block after import", "slot", block.Block.Slot, "error", err)
	}
	return nil
}

func (b *blockService) blockStored(ctx context.Context, blockRoot common.Hash) (bool, error) {
	var stored bool
	err := b.db.View(ctx, func(tx kv.Tx) error {
		// The slot index is committed atomically with the block and avoids
		// re-encoding and rewriting the block on every pending retry.
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			return err
		}
		stored = slot != nil
		return nil
	})
	return stored, err
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

func (b *blockService) tryProcessPendingBlock(ctx context.Context, blockRoot common.Hash, job *pendingBlockJob) pendingJobDecision {
	block := job.block
	seenKey := pendingBlockSeenKey(block)
	seenRoot, alreadySeen := b.loadOrReserveSeenBlock(seenKey, blockRoot)
	if alreadySeen && seenRoot != blockRoot {
		return pendingJobRemove
	}
	if _, ok := b.forkchoiceStore.GetHeader(blockRoot); ok {
		b.completeSeenBlock(seenKey, blockRoot)
		b.publishBlockGossipEvent(blockRoot, block.Block.Slot)
		return pendingJobRemove
	}
	if !job.readyToRetry(time.Now()) {
		return pendingJobKeep
	}
	if err := b.validateBlockAfterSignature(block); err != nil {
		if errors.Is(err, ErrIgnore) {
			return pendingJobKeep
		}
		log.Trace("Pending block failed validation", "block", block, "error", err)
		b.completeSeenBlock(seenKey, blockRoot)
		return pendingJobRemove
	}
	if b.forkchoiceStore.Slot() < block.Block.Slot {
		return pendingJobKeep
	}
	persisted, executionAndDataChecked := job.processingState()
	if !persisted {
		stored, err := b.blockStored(ctx, blockRoot)
		if err != nil {
			log.Trace("Failed to check pending block storage", "block", block, "error", err)
			return pendingJobKeep
		}
		if !stored {
			if err := b.storeBlock(ctx, block); err != nil {
				log.Trace("Failed to store pending block", "block", block, "error", err)
				return pendingJobKeep
			}
		}
		job.markPersisted()
	}
	if err := b.processStoredBlock(ctx, block, !executionAndDataChecked); err != nil {
		log.Trace("Failed to process and store block", "block", block, "error", err)
		if isPendingBlockRetryableError(err) {
			job.recordProcessingFailure(time.Now(), err)
			return pendingJobKeep
		}
		b.completeSeenBlock(seenKey, blockRoot)
		return pendingJobRemove
	}
	b.completeSeenBlock(seenKey, blockRoot)
	b.publishBlockGossipEvent(blockRoot, block.Block.Slot)
	return pendingJobRemove
}

func isPendingBlockRetryableError(err error) bool {
	return errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) ||
		errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
		errors.Is(err, forkchoice.ErrNewPayloadNoStatus) ||
		errors.Is(err, forkchoice.ErrParentEnvelopePending) ||
		errors.Is(err, forkchoice.ErrMissingSegment)
}
