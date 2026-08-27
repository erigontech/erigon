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
	block                   *cltypes.SignedBeaconBlock
	persisted               bool
	gossipEventPublished    bool
	executionAndDataChecked bool
	retryAfter              time.Time
	retryDelay              time.Duration
}

func (job *pendingBlockJob) readyToRetry(now time.Time) bool {
	return job.retryAfter.IsZero() || !now.Before(job.retryAfter)
}

func (job *pendingBlockJob) recordProcessingFailure(now time.Time, err error) {
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

type blockService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	syncedData      *synced_data.SyncedDataManager
	ethClock        eth_clock.EthereumClock
	beaconCfg       *clparams.BeaconChainConfig

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
	seenBlocksCache *lru.Cache[proposerIndexAndSlot, struct{}]
	emitter         *beaconevents.EventEmitter

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
	seenBlocksCache, err := lru.New[proposerIndexAndSlot, struct{}]("seenblocks", seenBlockCacheSize)
	if err != nil {
		panic(err)
	}
	b := &blockService{
		forkchoiceStore: forkchoiceStore,
		syncedData:      syncedData,
		ethClock:        ethClock,
		beaconCfg:       beaconCfg,
		seenBlocksCache: seenBlocksCache,
		emitter:         emitter,
		db:              db,
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
		func(blockRoot common.Hash) {
			log.Trace("Pending block expired", "blockRoot", blockRoot)
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
	seenCacheKey := proposerIndexAndSlot{
		proposerIndex: msg.Block.ProposerIndex,
		slot:          msg.Block.Slot,
	}
	if b.seenBlocksCache.Contains(seenCacheKey) {
		return nil
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

	if err := b.validateBlockAfterSignature(msg); err != nil {
		if errors.Is(err, ErrIgnore) {
			if scheduleErr := b.scheduleBlockForLaterProcessing(msg); scheduleErr != nil {
				return scheduleErr
			}
		}
		return err
	}
	b.publishBlockGossipEvent(msg)
	if b.forkchoiceStore.Slot() < msg.Block.Slot {
		if err := b.schedulePendingBlock(&pendingBlockJob{block: msg, gossipEventPublished: true}); err != nil {
			return err
		}
		return nil
	}
	// Fork choice performs the remaining block validation.
	if err := b.processAndStoreBlock(ctx, msg); err != nil {
		if isPendingBlockRetryableError(err) {
			if scheduleErr := b.scheduleBlockAfterProcessingFailure(msg, err); scheduleErr != nil {
				return scheduleErr
			}
			return nil
		}
		return err
	}
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

// publishBlockGossipEvent publishes after the block passes gossip validation.
func (b *blockService) publishBlockGossipEvent(block *cltypes.SignedBeaconBlock) {
	if b.emitter == nil {
		return
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		return
	}
	// publish block to event handler
	b.emitter.State().SendBlockGossip(&beaconevents.BlockGossipData{
		Slot:  block.Block.Slot,
		Block: common.Hash(blockRoot),
	})
}

// scheduleBlockForLaterProcessing queues a block after its gossip signature has
// been validated, while another processing dependency is unavailable.
func (b *blockService) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) error {
	return b.schedulePendingBlock(&pendingBlockJob{block: block})
}

func (b *blockService) scheduleBlockAfterProcessingFailure(block *cltypes.SignedBeaconBlock, processingErr error) error {
	// Retryable processing errors are returned only after the block transaction
	// commits, so deferred attempts must not write the same block again.
	job := &pendingBlockJob{block: block, persisted: true, gossipEventPublished: true}
	job.recordProcessingFailure(time.Now(), processingErr)
	return b.schedulePendingBlock(job)
}

func (b *blockService) schedulePendingBlock(job *pendingBlockJob) error {
	block := job.block
	result, err := b.blocksScheduledForLaterExecution.enqueueLazy(job, func() (common.Hash, error) {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return common.Hash{}, err
		}
		return common.Hash(blockRoot), nil
	})
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		return err
	}
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

// processAndStoreBlock processes and stores a block
func (b *blockService) processAndStoreBlock(ctx context.Context, block *cltypes.SignedBeaconBlock) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}

	if _, ok := b.forkchoiceStore.GetHeader(blockRoot); ok {
		return nil
	}

	if err := b.storeBlock(ctx, block); err != nil {
		return err
	}
	return b.processStoredBlock(ctx, block, true)
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
		return err
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
	if _, ok := b.forkchoiceStore.GetHeader(blockRoot); ok {
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
		return pendingJobRemove
	}
	if !job.gossipEventPublished {
		b.publishBlockGossipEvent(block)
		job.gossipEventPublished = true
	}
	if b.forkchoiceStore.Slot() < block.Block.Slot {
		return pendingJobKeep
	}
	if !job.persisted {
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
		job.persisted = true
	}
	if err := b.processStoredBlock(ctx, block, !job.executionAndDataChecked); err != nil {
		log.Trace("Failed to process and store block", "block", block, "error", err)
		if isPendingBlockRetryableError(err) {
			job.recordProcessingFailure(time.Now(), err)
			return pendingJobKeep
		}
		return pendingJobRemove
	}
	return pendingJobRemove
}

func isPendingBlockRetryableError(err error) bool {
	return errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) ||
		errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) ||
		errors.Is(err, forkchoice.ErrNewPayloadNoStatus) ||
		errors.Is(err, forkchoice.ErrParentEnvelopePending) ||
		errors.Is(err, forkchoice.ErrMissingSegment)
}
