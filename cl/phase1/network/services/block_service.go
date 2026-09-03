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

var ErrInvalidSignature = errors.New("invalid signature")
var ErrPublishedBlockJobExpired = errors.New("published block integration expired")
var ErrPublishedBlockJobStopped = errors.New("block service stopped")

var publishedBlockJobSequence atomic.Uint64

type proposerIndexAndSlot struct {
	proposerIndex uint64
	slot          uint64
}

type blockJob struct {
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
}

type seenBlock struct {
	signedRoot    common.Hash
	replayAllowed bool
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

	// blocks that should be scheduled for later execution (e.g missing blobs).
	emitter                          *beaconevents.EventEmitter
	blocksScheduledForLaterExecution sync.Map
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
	b := &blockService{
		forkchoiceStore: forkchoiceStore,
		syncedData:      syncedData,
		ethClock:        ethClock,
		beaconCfg:       beaconCfg,
		seenBlocksCache: seenBlocksCache,
		reservations:    make(map[proposerIndexAndSlot]*blockReservation),
		emitter:         emitter,
		db:              db,
	}
	go b.stopPublishedBlockJobsOnContext(ctx)
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

	// [IGNORE] The block is the first block with valid signature received for the proposer for the slot, signed_beacon_block.message.slot.
	if err := b.validateFirstGossip(ctx, msg, func() { b.ScheduleBlockForLaterProcessing(msg) }, true); err != nil {
		return err
	}
	b.publishBlockGossipEvent(msg)
	if err := b.processAndStoreBlock(ctx, msg); err != nil {
		if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) || errors.Is(err, forkchoice.ErrParentEnvelopePending) {
			b.ScheduleBlockForLaterProcessing(msg)
			return nil
		}
		return err
	}
	return nil
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
	if reservation != nil && reservation.pending != nil {
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
	if reservation.pending == nil && reservation.validators == 0 && b.reservations[key] == reservation {
		delete(b.reservations, key)
	}
}

func blockGossipKey(msg *cltypes.SignedBeaconBlock) proposerIndexAndSlot {
	return proposerIndexAndSlot{proposerIndex: msg.Block.ProposerIndex, slot: msg.Block.Slot}
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
	parentState, err := b.forkchoiceStore.GetStateAtBlockRoot(msg.Block.ParentRoot, true)
	if err != nil {
		if schedule != nil {
			schedule()
		}
		return fmt.Errorf("%w: get parent block state: %w", ErrIgnore, err)
	}
	if parentState == nil {
		if schedule != nil {
			schedule()
		}
		return fmt.Errorf("%w: parent block state not found", ErrIgnore)
	}
	if blockVersion >= clparams.GloasVersion {
		var parentBid *cltypes.SignedExecutionPayloadBid
		parentBlock, ok := b.forkchoiceStore.GetBlock(msg.Block.ParentRoot)
		switch {
		case ok && parentBlock != nil && parentBlock.Block != nil && parentBlock.Block.Body != nil:
			parentBid = parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		case msg.Block.ParentRoot == b.forkchoiceStore.AnchorRoot():
			if bid := parentState.GetLatestExecutionPayloadBid(); bid != nil {
				parentBid = &cltypes.SignedExecutionPayloadBid{Message: bid}
			}
		default:
			return errors.New("parent block not found")
		}
		parentIsFull = parentBid != nil && parentBid.Message != nil && gloasBid.ParentBlockHash == parentBid.Message.BlockHash
		if parentIsFull {
			status, seen := b.forkchoiceStore.GetRecentExecutionPayloadStatusByRoot(msg.Block.ParentRoot)
			if !seen || status != execution_client.PayloadStatusValidated {
				if schedule != nil {
					schedule()
				}
				return fmt.Errorf("%w: parent payload is not verified", ErrIgnore)
			}
		}
	}
	if err := transition.DefaultMachine.ProcessSlots(parentState, msg.Block.Slot); err != nil {
		if schedule != nil {
			schedule()
		}
		return fmt.Errorf("%w: process parent state to block slot: %w", ErrIgnore, err)
	}
	expectedProposer, err := parentState.GetBeaconProposerIndexForSlot(msg.Block.Slot)
	if err != nil {
		if schedule != nil {
			schedule()
		}
		return fmt.Errorf("%w: get expected proposer: %w", ErrIgnore, err)
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

// ScheduleBlockForLaterProcessing schedules a block for later processing.
func (b *blockService) ScheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) {
	b.scheduleBlockForLaterProcessing(block, nil)
}

func (b *blockService) SchedulePublishedBlockForLaterProcessing(block *cltypes.SignedBeaconBlock, store func(context.Context) error) PublishedBlockJob {
	job, generation := b.scheduleBlockForLaterProcessing(block, store)
	return &publishedBlockJobHandle{job: job, generation: generation}
}

func (b *blockService) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock, store func(context.Context) error) (*blockJob, uint64) {
	// [Modified in Gloas:EIP7732] ExecutionPayload is not in block.body for GLOAS
	var blockNum uint64
	if block.Block.Body.ExecutionPayload != nil {
		blockNum = block.Block.Body.ExecutionPayload.BlockNumber
	}
	log.Trace("Block scheduled for later processing", "slot", block.Block.Slot, "block", blockNum)
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Debug("Failed to hash block", "block", block, "error", err)
		job := newFailedBlockJob(block, store, err)
		return job, job.storeGeneration
	}

	job := newBlockJob(block, store)
	jobGeneration := job.storeGeneration
	b.blockJobsLifecycleMu.RLock()
	defer b.blockJobsLifecycleMu.RUnlock()
	if b.blockJobsStopped {
		job = newFailedBlockJob(block, store, ErrPublishedBlockJobStopped)
		return job, job.storeGeneration
	}
	for {
		existingValue, loaded := b.blocksScheduledForLaterExecution.LoadOrStore(blockRoot, job)
		if !loaded {
			return job, jobGeneration
		}
		existing, generation := b.reuseScheduledBlockJob(blockRoot, existingValue.(*blockJob), job, store)
		if existing != nil {
			return existing, generation
		}
	}
}

func (b *blockService) reuseScheduledBlockJob(key [32]byte, existing, job *blockJob, store func(context.Context) error) (*blockJob, uint64) {
	existing.mu.Lock()
	defer existing.mu.Unlock()
	current, ok := b.blocksScheduledForLaterExecution.Load(key)
	if !ok || current != existing {
		return nil, 0
	}
	if store == nil {
		return existing, existing.storeGeneration
	}
	if job.scheduleSequence <= existing.scheduleSequence {
		return existing, existing.storeGeneration
	}
	existing.store = store
	existing.storeGeneration++
	existing.scheduleSequence = job.scheduleSequence
	existing.creationTime = time.Now()
	if existing.terminal {
		existing.terminal = false
		existing.attempt = &blockJobAttempt{done: make(chan struct{})}
	}
	return existing, existing.storeGeneration
}

// processAndStoreBlock processes and stores a block
func (b *blockService) processAndStoreBlock(ctx context.Context, block *cltypes.SignedBeaconBlock) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}

	_, headerExists := b.forkchoiceStore.GetHeader(blockRoot)

	if err := b.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}

	if !headerExists {
		if err := b.forkchoiceStore.OnBlock(ctx, block, true, true, true); err != nil {
			return err
		}
		go b.importBlockOperations(block)
	}
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
			b.stopPublishedBlockJobs()
			return
		case <-ticker.C:
		}
		b.blocksScheduledForLaterExecution.Range(func(key, value any) bool {
			b.processScheduledBlock(ctx, key.([32]byte), value.(*blockJob), time.Now())
			return true
		})
	}
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

	b.blocksScheduledForLaterExecution.Range(func(key, value any) bool {
		job := value.(*blockJob)
		job.mu.Lock()
		current, ok := b.blocksScheduledForLaterExecution.Load(key)
		if ok && current == job {
			if !job.terminal {
				job.attempt.err = ErrPublishedBlockJobStopped
				job.attempt.generation = job.storeGeneration
				job.lastAttempt = job.attempt
				job.completedGeneration = job.storeGeneration
				job.terminal = true
				close(job.attempt.done)
			}
			b.blocksScheduledForLaterExecution.CompareAndDelete(key, job)
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
		if !job.terminal {
			job.attempt.err = ErrPublishedBlockJobExpired
			job.attempt.generation = job.storeGeneration
			job.lastAttempt = job.attempt
			job.completedGeneration = job.storeGeneration
			job.terminal = true
			close(job.attempt.done)
		}
		b.blocksScheduledForLaterExecution.CompareAndDelete(key, job)
		job.mu.Unlock()
		return
	}
	if job.terminal {
		job.mu.Unlock()
		return
	}
	job.running = true
	store := job.store
	generation := job.storeGeneration
	attempt := job.attempt
	job.mu.Unlock()
	if store == nil {
		store = func(ctx context.Context) error { return b.processAndStoreBlock(ctx, job.block) }
	}
	err := store(ctx)
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
	terminal := latest && (err == nil || errors.Is(err, forkchoice.ErrBlockInvalid))
	if terminal {
		job.completedGeneration = generation
		job.terminal = true
	} else {
		job.attempt = &blockJobAttempt{done: make(chan struct{})}
	}
	if terminal {
		b.blocksScheduledForLaterExecution.CompareAndDelete(key, job)
	}
	job.mu.Unlock()
	if err != nil {
		log.Trace("Failed to process and store block", "block", job.block, "error", err)
		return
	}
}
