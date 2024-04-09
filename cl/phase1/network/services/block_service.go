package services

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"

	libcomoon "github.com/ledgerwatch/erigon-lib/common"
)

type proposerIndexAndSlot struct {
	proposerIndex uint64
	slot          uint64
}

type blockJob struct {
	block     *cltypes.SignedBeaconBlock
	blockRoot libcomoon.Hash
	when      time.Time
}

type blockService struct {
	forkchoiceStore forkchoice.ForkChoiceStorage
	syncedData      *synced_data.SyncedDataManager
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig

	// reference: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
	seenBlocksCache *lru.Cache[proposerIndexAndSlot, struct{}]

	// blocks that should be scheduled for later execution (e.g missing blobs).
	emitter                          *beaconevents.Emitters
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
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	emitter *beaconevents.Emitters,
) Service[*cltypes.SignedBeaconBlock] {
	seenBlocksCache, err := lru.New[proposerIndexAndSlot, struct{}]("seenblocks", SeenBlockCacheSize)
	if err != nil {
		panic(err)
	}
	b := &blockService{
		forkchoiceStore: forkchoiceStore,
		syncedData:      syncedData,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		seenBlocksCache: seenBlocksCache,
		emitter:         emitter,
		db:              db,
	}
	go b.loop(ctx)
	return b
}

// ProcessMessage processes a block message according to https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beacon_block
func (b *blockService) ProcessMessage(ctx context.Context, msg *cltypes.SignedBeaconBlock) error {
	headState := b.syncedData.HeadState()
	if headState == nil {
		b.scheduleBlockForLaterProcessing(msg)
		log.Debug("Head state is nil")
		return ErrIgnore
	}

	blockEpoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch

	currentSlot := utils.GetCurrentSlot(b.genesisCfg.GenesisTime, b.beaconCfg.SecondsPerSlot)
	// [IGNORE] The block is not from a future slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. validate that
	//signed_beacon_block.message.slot <= current_slot (a client MAY queue future blocks for processing at the appropriate slot).
	if currentSlot < msg.Block.Slot && !utils.IsCurrentSlotWithMaximumClockDisparity(b.genesisCfg.GenesisTime, b.beaconCfg.SecondsPerSlot, msg.Block.Slot) {
		log.Debug("Block is from a future slot", "currentSlot", currentSlot, "blockSlot", msg.Block.Slot)
		return ErrIgnore
	}
	// [IGNORE] The block is from a slot greater than the latest finalized slot -- i.e. validate that signed_beacon_block.message.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
	// (a client MAY choose to validate and store such blocks for additional purposes -- e.g. slashing detection, archive nodes, etc).
	if blockEpoch <= headState.FinalizedCheckpoint().Epoch() {
		log.Debug("Block is from a slot less than or equal to the latest finalized slot", "blockEpoch", blockEpoch, "finalizedEpoch", headState.FinalizedCheckpoint().Epoch())
		return ErrIgnore
	}

	// [IGNORE] The block is the first block with valid signature received for the proposer for the slot, signed_beacon_block.message.slot.
	seenCacheKey := proposerIndexAndSlot{
		proposerIndex: msg.Block.ProposerIndex,
		slot:          msg.Block.Slot,
	}
	if b.seenBlocksCache.Contains(seenCacheKey) {
		log.Debug("Block is the first block with valid signature received for the proposer for the slot", "proposerIndex", msg.Block.ProposerIndex, "slot", msg.Block.Slot)
		return ErrIgnore
	}

	// [IGNORE] The block's parent (defined by block.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue blocks for processing once the parent block is retrieved).
	parentHeader, ok := b.forkchoiceStore.GetHeader(msg.Block.ParentRoot)
	if !ok {
		log.Debug("Parent header not found", "parentRoot", msg.Block.ParentRoot)
		return ErrIgnore
	}
	if parentHeader.Slot >= msg.Block.Slot {
		log.Debug("Parent header is greater than or equal to the block slot", "parentSlot", parentHeader.Slot, "blockSlot", msg.Block.Slot)
		return ErrBlockYoungerThanParent
	}

	// [REJECT] The length of KZG commitments is less than or equal to the limitation defined in Consensus Layer -- i.e. validate that len(body.signed_beacon_block.message.blob_kzg_commitments) <= MAX_BLOBS_PER_BLOCK
	if msg.Block.Body.BlobKzgCommitments.Len() > int(b.beaconCfg.MaxBlobsPerBlock) {
		log.Debug("Invalid commitments count", "commitmentsCount", msg.Block.Body.BlobKzgCommitments.Len(), "maxCommitments", b.beaconCfg.MaxBlobsPerBlock)
		return ErrInvalidCommitmentsCount
	}
	b.publishBlockEvent(msg)

	// the rest of the validation is done in the forkchoice store
	if err := b.processAndStoreBlock(ctx, msg); err != nil {
		if err == forkchoice.ErrEIP4844DataNotAvailable {
			b.scheduleBlockForLaterProcessing(msg)
			log.Info("Block scheduled for later processing", "block", msg.Block.Slot)
			return ErrIgnore
		}
		return err
	}
	return nil
}

// publishBlockEvent publishes a block event
func (b *blockService) publishBlockEvent(block *cltypes.SignedBeaconBlock) {
	if b.emitter == nil {
		return
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		return
	}
	// publish block to event handler
	b.emitter.Publish("block", map[string]any{
		"slot":                 strconv.Itoa(int(block.Block.Slot)),
		"block":                libcomoon.Hash(blockRoot),
		"execution_optimistic": false,
	})
}

// scheduleBlockForLaterProcessing schedules a block for later processing
func (b *blockService) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		return
	}

	b.blocksScheduledForLaterExecution.Store(&blockJob{
		block:     block,
		blockRoot: blockRoot,
		when:      time.Now(),
	}, struct{}{})
}

// processAndStoreBlock processes and stores a block
func (b *blockService) processAndStoreBlock(ctx context.Context, block *cltypes.SignedBeaconBlock) error {
	start := time.Now()
	if err := b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}
	if err := b.forkchoiceStore.OnBlock(ctx, block, true, true, true); err != nil {
		return err
	}
	go b.importBlockAttestations(block)
	fmt.Println("Block stored in", time.Since(start))
	return b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, b.forkchoiceStore.FinalizedSlot())
	})

}

// importBlockAttestationsInParallel imports block attestations in parallel
func (b *blockService) importBlockAttestations(block *cltypes.SignedBeaconBlock) {
	defer func() {
		r := recover()
		if r != nil {
			log.Warn("recovered from panic", "err", r)
		}
	}()
	block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
		// emit attestation
		b.emitter.Publish("attestation", a)
		if err := b.forkchoiceStore.OnAttestation(a, true, false); err != nil {
			log.Debug("bad attestation received", "err", err)
		}

		return true
	})
}

// loop is the main loop of the block service
func (b *blockService) loop(ctx context.Context) {
	ticker := time.NewTicker(jobsIntervalTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		b.blocksScheduledForLaterExecution.Range(func(key, _ any) bool {
			blockJob := key.(*blockJob)
			// check if it has expired
			if time.Since(blockJob.when) > blockJobExpiry {
				b.blocksScheduledForLaterExecution.Delete(key)
				return true
			}
			if err := b.processAndStoreBlock(ctx, blockJob.block); err != nil {
				log.Trace("Failed to process and store block", "block", blockJob.block, "error", err)
				return true
			}
			b.blocksScheduledForLaterExecution.Delete(key)
			return true
		})
	}
}
