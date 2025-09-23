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
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/db/kv"
)

var (
	ErrInvalidSignature = errors.New("invalid signature")
)

type proposerIndexAndSlot struct {
	proposerIndex uint64
	slot          uint64
}

type blockJob struct {
	block        *cltypes.SignedBeaconBlock
	creationTime time.Time
}

type blockService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	syncedData      *synced_data.SyncedDataManager
	ethClock        eth_clock.EthereumClock
	beaconCfg       *clparams.BeaconChainConfig

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
	seenBlocksCache *lru.Cache[proposerIndexAndSlot, struct{}]

	// blocks that should be scheduled for later execution (e.g missing blobs).
	emitter                          *beaconevents.EventEmitter
	blocksScheduledForLaterExecution sync.Map
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
) Service[*cltypes.SignedBeaconBlock] {
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
	go b.loop(ctx)
	return b
}

// ProcessMessage processes a block message according to https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
func (b *blockService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.SignedBeaconBlock) error {

	blockEpoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch

	if b.syncedData.Syncing() {
		return ErrIgnore
	}

	currentSlot := b.syncedData.HeadSlot()

	// [IGNORE] The block is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. validate that
	//signed_beacon_block.message.slot <= current_slot (a client MAY queue future blocks for processing at the appropriate slot).
	if currentSlot < msg.Block.Slot && !b.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(msg.Block.Slot) {
		return ErrIgnore
	}

	// [IGNORE] The block is the first block with valid signature received for the proposer for the slot, signed_beacon_block.message.slot.
	seenCacheKey := proposerIndexAndSlot{
		proposerIndex: msg.Block.ProposerIndex,
		slot:          msg.Block.Slot,
	}
	if b.seenBlocksCache.Contains(seenCacheKey) {
		return ErrIgnore
	}

	if err := b.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The block is from a slot greater than the latest finalized slot -- i.e. validate that signed_beacon_block.message.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
		// (a client MAY choose to validate and store such blocks for additional purposes -- e.g. slashing detection, archive nodes, etc).
		if blockEpoch <= headState.FinalizedCheckpoint().Epoch {
			return ErrIgnore
		}

		if ok, err := eth2.VerifyBlockSignature(headState, msg); err != nil {
			return err
		} else if !ok {
			return ErrInvalidSignature
		}
		return nil
	}); err != nil {
		if errors.Is(err, ErrIgnore) {
			b.scheduleBlockForLaterProcessing(msg)
		}
		return err
	}

	// [IGNORE] The block's parent (defined by block.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue blocks for processing once the parent block is retrieved).
	parentHeader, ok := b.forkchoiceStore.GetHeader(msg.Block.ParentRoot)
	if !ok {
		b.scheduleBlockForLaterProcessing(msg)
		return ErrIgnore
	}
	if parentHeader.Slot >= msg.Block.Slot {
		return ErrBlockYoungerThanParent
	}

	// [REJECT] The length of KZG commitments is less than or equal to the limitation defined in Consensus Layer -- i.e. validate that len(body.signed_beacon_block.message.blob_kzg_commitments) <= MAX_BLOBS_PER_BLOCK
	epoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch
	blockVersion := b.beaconCfg.GetCurrentStateVersion(epoch)
	var maxBlobsPerBlock uint64
	if blockVersion >= clparams.FuluVersion {
		maxBlobsPerBlock = b.beaconCfg.GetBlobParameters(epoch).MaxBlobsPerBlock
	} else {
		maxBlobsPerBlock = b.beaconCfg.MaxBlobsPerBlockByVersion(blockVersion)
	}
	if msg.Block.Body.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
		return ErrInvalidCommitmentsCount
	}
	b.publishBlockGossipEvent(msg)
	// the rest of the validation is done in the forkchoice store
	if err := b.processAndStoreBlock(ctx, msg); err != nil {
		if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			b.scheduleBlockForLaterProcessing(msg)
			return ErrIgnore
		}
		return err
	}
	return nil
}

// publishBlockGossipEvent publishes a block event which has not been processed yet
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

// scheduleBlockForLaterProcessing schedules a block for later processing
func (b *blockService) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) {
	log.Debug("Block scheduled for later processing", "slot", block.Block.Slot, "block", block.Block.Body.ExecutionPayload.BlockNumber)
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		return
	}

	b.blocksScheduledForLaterExecution.Store(blockRoot, &blockJob{
		block:        block,
		creationTime: time.Now(),
	})
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

	if err := b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}

	if err := b.forkchoiceStore.OnBlock(ctx, block, true, true, true); err != nil {
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
		if err := b.forkchoiceStore.OnAttesterSlashing(a, false); err != nil {
			log.Debug("bad attester slashing received", "err", err)
		}
		return true
	})
	log.Debug("import operations", "time", time.Since(start))
}

// loop is the main loop of the block service
func (b *blockService) loop(ctx context.Context) {
	ticker := time.NewTicker(blockJobsIntervalTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		b.blocksScheduledForLaterExecution.Range(func(key, value any) bool {
			blockJob := value.(*blockJob)
			// check if it has expired
			if time.Since(blockJob.creationTime) > blockJobExpiry {
				b.blocksScheduledForLaterExecution.Delete(key.([32]byte))
				return true
			}
			if err := b.processAndStoreBlock(ctx, blockJob.block); err != nil {
				log.Trace("Failed to process and store block", "block", blockJob.block, "error", err)
				return true
			}
			b.blocksScheduledForLaterExecution.Delete(key.([32]byte))
			return true
		})
	}
}
