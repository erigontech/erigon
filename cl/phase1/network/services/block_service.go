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
	"github.com/erigontech/erigon/cl/transition"
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
	seenBlocksMu    sync.Mutex

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
	go b.loop(ctx)
	return b
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
	if msg == nil || msg.Block == nil || msg.Block.Body == nil {
		return errors.New("missing beacon block")
	}
	log.Trace("Received block via gossip", "slot", msg.Block.Slot)

	// [IGNORE] The block is the first block with valid signature received for the proposer for the slot, signed_beacon_block.message.slot.
	if err := b.validateFirstGossip(ctx, msg, func() { b.scheduleBlockForLaterProcessing(msg) }); err != nil {
		return err
	}
	b.publishBlockGossipEvent(msg)
	// the rest of the validation is done in the forkchoice store
	if err := b.processAndStoreBlock(ctx, msg); err != nil {
		if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) || errors.Is(err, forkchoice.ErrParentEnvelopePending) {
			b.scheduleBlockForLaterProcessing(msg)
			return nil
		}
		return err
	}
	return nil
}

func (b *blockService) ValidateGossip(ctx context.Context, msg *cltypes.SignedBeaconBlock) error {
	return b.validateFirstGossip(ctx, msg, nil)
}

func (b *blockService) validateFirstGossip(ctx context.Context, msg *cltypes.SignedBeaconBlock, schedule func()) error {
	if err := b.validateGossip(ctx, msg, schedule); err != nil {
		return err
	}
	key := proposerIndexAndSlot{proposerIndex: msg.Block.ProposerIndex, slot: msg.Block.Slot}
	b.seenBlocksMu.Lock()
	defer b.seenBlocksMu.Unlock()
	if b.seenBlocksCache.Contains(key) {
		return fmt.Errorf("%w: block already seen for proposer and slot", ErrIgnore)
	}
	b.seenBlocksCache.Add(key, struct{}{})
	return nil
}

func (b *blockService) validateGossip(_ context.Context, msg *cltypes.SignedBeaconBlock, schedule func()) error {
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
	var finalizedCheckpoint solid.Checkpoint

	if err := b.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] The block is from a slot greater than the latest finalized slot -- i.e. validate that signed_beacon_block.message.slot > compute_start_slot_at_epoch(store.finalized_checkpoint.epoch)
		// (a client MAY choose to validate and store such blocks for additional purposes -- e.g. slashing detection, archive nodes, etc).
		finalizedStartSlot, ok := safeMultiplyUint64(headState.FinalizedCheckpoint().Epoch, b.beaconCfg.SlotsPerEpoch)
		if !ok {
			return errors.New("finalized checkpoint slot is not representable")
		}
		if msg.Block.Slot <= finalizedStartSlot {
			return fmt.Errorf("%w: block slot %d is not after finalized slot %d", ErrIgnore, msg.Block.Slot, finalizedStartSlot)
		}
		finalizedCheckpoint = headState.FinalizedCheckpoint()

		if ok, err := eth2.VerifyBlockSignature(headState, msg); err != nil {
			return err
		} else if !ok {
			return ErrInvalidSignature
		}
		return nil
	}); err != nil {
		if errors.Is(err, ErrIgnore) && schedule != nil {
			schedule()
		}
		return err
	}

	// [IGNORE] The block's parent (defined by block.parent_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue blocks for processing once the parent block is retrieved).
	parentHeader, ok := b.forkchoiceStore.GetHeader(msg.Block.ParentRoot)
	if !ok {
		if schedule != nil {
			schedule()
		}
		return fmt.Errorf("%w: parent header not found: %v", ErrIgnore, msg.Block.ParentRoot)
	}
	if parentHeader.Slot >= msg.Block.Slot {
		return ErrBlockYoungerThanParent
	}
	epoch := msg.Block.Slot / b.beaconCfg.SlotsPerEpoch
	blockVersion := b.beaconCfg.GetCurrentStateVersion(epoch)
	var gloasBid *cltypes.ExecutionPayloadBid
	parentIsFull := false
	if blockVersion >= clparams.GloasVersion {
		signedBid := msg.Block.Body.GetSignedExecutionPayloadBid()
		if signedBid == nil || signedBid.Message == nil {
			return errors.New("missing signed_execution_payload_bid in GLOAS block")
		}
		gloasBid = signedBid.Message
		parentBlock, ok := b.forkchoiceStore.GetBlock(msg.Block.ParentRoot)
		if !ok || parentBlock == nil || parentBlock.Block == nil || parentBlock.Block.Body == nil {
			return errors.New("parent block not found")
		}
		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		parentIsFull = parentBid != nil && parentBid.Message != nil && gloasBid.ParentBlockHash == parentBid.Message.BlockHash
		if parentIsFull {
			status, seen := b.forkchoiceStore.GetRecentExecutionPayloadStatusByRoot(msg.Block.ParentRoot)
			if status == execution_client.PayloadStatusInvalidated {
				return errors.New("parent execution payload is invalid")
			}
			if !seen || status != execution_client.PayloadStatusValidated {
				if schedule != nil {
					schedule()
				}
				return fmt.Errorf("%w: parent payload is not verified", ErrIgnore)
			}
		}
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
	parentState, err := b.forkchoiceStore.GetStateAtBlockRoot(msg.Block.ParentRoot, true)
	if err != nil {
		return fmt.Errorf("get parent block state: %w", err)
	}
	if parentState == nil {
		return errors.New("parent block state not found")
	}
	if err := transition.DefaultMachine.ProcessSlots(parentState, msg.Block.Slot); err != nil {
		return fmt.Errorf("process parent state to block slot: %w", err)
	}
	expectedProposer, err := parentState.GetBeaconProposerIndexForSlot(msg.Block.Slot)
	if err != nil {
		return fmt.Errorf("get expected proposer: %w", err)
	}
	if msg.Block.ProposerIndex != expectedProposer {
		return fmt.Errorf("block proposer index %d does not match expected proposer %d", msg.Block.ProposerIndex, expectedProposer)
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

		if !parentIsFull && gloasBid.ParentBlockHash != parentState.GetLatestBlockHash() {
			return errors.New("bid does not build on the parent's execution head")
		}
	} else if msg.Block.Body.BlobKzgCommitments != nil && msg.Block.Body.BlobKzgCommitments.Len() > int(maxBlobsPerBlock) {
		// Pre-GLOAS: [REJECT] The length of KZG commitments is less than or equal to the limitation defined in Consensus Layer
		// i.e. validate that len(body.signed_beacon_block.message.blob_kzg_commitments) <= MAX_BLOBS_PER_BLOCK
		return ErrInvalidCommitmentsCount
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
	// [Modified in Gloas:EIP7732] ExecutionPayload is not in block.body for GLOAS
	var blockNum uint64
	if block.Block.Body.ExecutionPayload != nil {
		blockNum = block.Block.Body.ExecutionPayload.BlockNumber
	}
	log.Trace("Block scheduled for later processing", "slot", block.Block.Slot, "block", blockNum)
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
		if err := b.forkchoiceStore.OnAttesterSlashing(a, false); err != nil && !errors.Is(err, forkchoice.ErrIgnore) {
			log.Debug("bad attester slashing received", "err", err)
		}
		return true
	})
	log.Trace("import operations", "time", time.Since(start))
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
