// Copyright 2026 The Erigon Authors
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
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seenBidKey tracks seen bids per (builderIndex, slot).
// Spec: [IGNORE] The signed_execution_payload_bid is the first valid bid from this builder for this slot.
type seenBidKey struct {
	builderIndex uint64
	slot         uint64
}

// pendingBidKey tracks bids waiting for proposer preferences.
type pendingBidKey struct {
	builderIndex    uint64
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
	blockHash       common.Hash
	signature       common.Bytes96
}

// pendingBidJob represents a pending bid waiting for proposer preferences to arrive.
type pendingBidJob struct {
	msg          *cltypes.SignedExecutionPayloadBid
	creationTime time.Time
}

var errBidDependencyUnavailable = fmt.Errorf("%w: bid dependency unavailable", ErrIgnore)

const (
	// seenBidCacheSize: multiple builders can bid per slot.
	// With clock disparity we may see bids for ~2 slots.
	// 256 builders * 2 slots = 512 provides safety margin.
	seenBidCacheSize        = 512
	pendingBidExpiry        = 12 * time.Second // 1 slot
	pendingBidCheckInterval = 100 * time.Millisecond
	maxPendingBids          = 1024
)

type executionPayloadBidService struct {
	syncedDataManager synced_data.SyncedData
	forkchoiceStore   forkchoice.ForkChoiceStorageReader
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool
	emitters          *beaconevents.EventEmitter

	seenCache *lru.Cache[seenBidKey, struct{}]

	// Pending bids waiting for proposer preferences
	pendingBids  sync.Map // pendingBidKey -> *pendingBidJob
	pendingCount atomic.Int32
	pendingCond  *sync.Cond
}

// NewExecutionPayloadBidService creates a new execution payload bid gossip service.
// [New in Gloas:EIP7732]
func NewExecutionPayloadBidService(
	ctx context.Context,
	syncedDataManager synced_data.SyncedData,
	forkchoiceStore forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	epbsPool *pool.EpbsPool,
	emitters *beaconevents.EventEmitter,
) ExecutionPayloadBidService {
	seenCache, err := lru.New[seenBidKey, struct{}]("seen_execution_payload_bids", seenBidCacheSize)
	if err != nil {
		panic(err)
	}
	s := &executionPayloadBidService{
		syncedDataManager: syncedDataManager,
		forkchoiceStore:   forkchoiceStore,
		ethClock:          ethClock,
		beaconCfg:         beaconCfg,
		epbsPool:          epbsPool,
		emitters:          emitters,
		seenCache:         seenCache,
		pendingCond:       sync.NewCond(&sync.Mutex{}),
	}
	go s.loop(ctx)
	return s
}

func (s *executionPayloadBidService) Names() []string {
	return []string{gossip.TopicNameExecutionPayloadBid}
}

func (s *executionPayloadBidService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedExecutionPayloadBid, error) {
	msg := &cltypes.SignedExecutionPayloadBid{}
	if err := msg.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return msg, nil
}

// ProcessMessage processes a signed execution payload bid according to the gossip spec.
// Reference: consensus-specs/specs/gloas/p2p-interface.md#execution_payload_bid
// [New in Gloas:EIP7732]
func (s *executionPayloadBidService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.SignedExecutionPayloadBid) error {
	if msg == nil || msg.Message == nil {
		return fmt.Errorf("nil execution payload bid message")
	}

	bid := msg.Message
	slot := bid.Slot
	builderIndex := bid.BuilderIndex

	log.Trace("Received execution payload bid via gossip",
		"slot", slot,
		"builderIndex", builderIndex,
		"value", bid.Value,
		"parentBlockHash", bid.ParentBlockHash)

	// [IGNORE] bid.slot is the current or next slot
	currentSlot := s.ethClock.GetCurrentSlot()
	if slot != currentSlot && slot != currentSlot+1 {
		return fmt.Errorf("%w: bid slot %d is not current (%d) or next slot", ErrIgnore, slot, currentSlot)
	}

	seenKey := seenBidKey{builderIndex: builderIndex, slot: slot}
	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen bid from builder %d for slot %d",
			ErrIgnore, builderIndex, slot)
	}
	if err := s.validateBidStateless(bid); err != nil {
		return err
	}

	preferences, ok, err := s.matchingProposerPreferences(msg)
	if err != nil {
		if errors.Is(err, errBidDependencyUnavailable) {
			s.queuePendingBid(msg)
			log.Trace("Queued execution payload bid waiting for dependencies",
				"slot", slot, "builderIndex", builderIndex, "err", err)
			return nil
		}
		return err
	}
	if !ok {
		// Queue as pending — preferences may arrive later
		s.queuePendingBid(msg)
		log.Trace("Queued execution payload bid waiting for proposer preferences",
			"slot", slot, "builderIndex", builderIndex)
		return nil
	}

	return s.validateAndStoreBid(msg, preferences)
}

func (s *executionPayloadBidService) matchingProposerPreferences(msg *cltypes.SignedExecutionPayloadBid) (*cltypes.SignedProposerPreferences, bool, error) {
	bid := msg.Message
	parentState, err := s.forkchoiceStore.GetStateAtBlockRoot(bid.ParentBlockRoot, false)
	if err != nil || parentState == nil {
		return nil, false, fmt.Errorf("%w: state for parent_block_root %v not available", errBidDependencyUnavailable, bid.ParentBlockRoot)
	}
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	dependentRoot := s.shufflingDependentRoot(bid.ParentBlockRoot, proposalEpoch)
	if proposalEpoch > s.beaconCfg.MinSeedLookahead && dependentRoot == (common.Hash{}) {
		return nil, false, fmt.Errorf("%w: failed to compute proposer dependent root", ErrIgnore)
	}
	preferences, ok := s.epbsPool.GetPreference(bid.Slot, dependentRoot)
	return preferences, ok, nil
}

func (s *executionPayloadBidService) shufflingDependentRoot(root common.Hash, epoch uint64) common.Hash {
	if epoch <= s.beaconCfg.MinSeedLookahead {
		return common.Hash{}
	}
	dependentSlot := (epoch-s.beaconCfg.MinSeedLookahead)*s.beaconCfg.SlotsPerEpoch - 1
	return s.forkchoiceStore.Ancestor(root, dependentSlot).Root
}

func (s *executionPayloadBidService) validateBidStateless(bid *cltypes.ExecutionPayloadBid) error {
	if bid.ExecutionPayment != 0 {
		return fmt.Errorf("bid execution_payment must be 0, got %d", bid.ExecutionPayment)
	}
	epoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	maxBlobsPerBlock := int(s.beaconCfg.GetBlobParameters(epoch).MaxBlobsPerBlock)
	if bid.BlobKzgCommitments.Len() > maxBlobsPerBlock {
		return fmt.Errorf("bid has too many blob_kzg_commitments: %d > %d",
			bid.BlobKzgCommitments.Len(), maxBlobsPerBlock)
	}
	return nil
}

// validateAndStoreBid performs all remaining validation checks after preferences are confirmed.
func (s *executionPayloadBidService) validateAndStoreBid(
	msg *cltypes.SignedExecutionPayloadBid,
	preferences *cltypes.SignedProposerPreferences,
) error {
	bid := msg.Message
	slot := bid.Slot
	builderIndex := bid.BuilderIndex
	prefs := preferences.Message

	// [REJECT] fee_recipient must match proposer preferences
	if bid.FeeRecipient != prefs.FeeRecipient {
		return fmt.Errorf("bid fee_recipient %v does not match proposer preferences %v",
			bid.FeeRecipient, prefs.FeeRecipient)
	}

	// [IGNORE] parent_block_root is known in fork choice
	parentHeader, ok := s.forkchoiceStore.GetHeader(bid.ParentBlockRoot)
	if !ok {
		return fmt.Errorf("%w: parent_block_root %v not known in fork choice",
			ErrIgnore, bid.ParentBlockRoot)
	}
	if slot <= parentHeader.Slot {
		return fmt.Errorf("bid slot %d is not greater than parent block slot %d", slot, parentHeader.Slot)
	}
	parentState, err := s.forkchoiceStore.GetStateAtBlockRoot(bid.ParentBlockRoot, false)
	if err != nil || parentState == nil {
		return fmt.Errorf("%w: state for parent_block_root %v not available", errBidDependencyUnavailable, bid.ParentBlockRoot)
	}
	validationState, err := s.bidValidationState(parentState, slot)
	if err != nil {
		return fmt.Errorf("bid validation failed: %w", err)
	}
	if bid.PrevRandao != validationState.GetRandaoMixes(state.Epoch(validationState)) {
		return fmt.Errorf("bid prev_randao does not match parent state randao mix")
	}

	if err := s.validateBuilderBid(msg, validationState); err != nil {
		return fmt.Errorf("bid validation failed: %w", err)
	}

	// [IGNORE] parent_block_hash is known in fork choice
	if _, ok := s.forkchoiceStore.GetRecentExecutionPayloadStatus(bid.ParentBlockHash); !ok {
		return fmt.Errorf("%w: parent_block_hash %v not known in fork choice",
			ErrIgnore, bid.ParentBlockHash)
	}

	// [IGNORE] gas_limit compatibility check — skipped (not rejected) when parent is absent from the LRU.
	if parentGasLimit, ok := s.forkchoiceStore.GetExecutionPayloadGasLimit(bid.ParentBlockHash); ok {
		if !IsGasLimitTargetCompatible(parentGasLimit, bid.GasLimit, prefs.TargetGasLimit) {
			return fmt.Errorf("%w: bid gas_limit %d not compatible with target %d (parent %d)",
				ErrIgnore, bid.GasLimit, prefs.TargetGasLimit, parentGasLimit)
		}
	}

	// [IGNORE] Highest bid check: only accept if this is the highest value bid for (slot, parentBlockHash, parentBlockRoot)
	bidKey := pool.HighestBidKey{Slot: slot, ParentBlockHash: bid.ParentBlockHash, ParentBlockRoot: bid.ParentBlockRoot}
	if existing, found := s.epbsPool.HighestBids.Get(bidKey); found {
		if bid.Value <= existing.Message.Value {
			return fmt.Errorf("%w: bid value %d is not higher than existing %d for slot %d",
				ErrIgnore, bid.Value, existing.Message.Value, slot)
		}
	}

	// All checks passed — mark as seen and store
	seenKey := seenBidKey{builderIndex: builderIndex, slot: slot}
	s.seenCache.Add(seenKey, struct{}{})
	s.epbsPool.HighestBids.Add(bidKey, msg)

	// Emit SSE event for execution_payload_bid [New in Gloas:EIP7732]
	s.emitters.Operation().SendExecutionPayloadBid(msg)

	log.Trace("Processed execution payload bid via gossip",
		"slot", slot,
		"builderIndex", builderIndex,
		"value", bid.Value,
		"parentBlockHash", bid.ParentBlockHash)

	return nil
}

func (s *executionPayloadBidService) bidValidationState(parentState *state.CachingBeaconState, bidSlot uint64) (*state.CachingBeaconState, error) {
	if parentState.Slot() > bidSlot {
		return nil, fmt.Errorf("parent state slot %d is after bid slot %d", parentState.Slot(), bidSlot)
	}
	validationState, err := parentState.Copy()
	if err != nil {
		return nil, err
	}
	if parentState.Slot() == bidSlot {
		return validationState, nil
	}
	if err := transition.DefaultMachine.ProcessSlots(validationState, bidSlot); err != nil {
		return nil, err
	}
	return validationState, nil
}

func (s *executionPayloadBidService) validateBuilderBid(msg *cltypes.SignedExecutionPayloadBid, validationState *state.CachingBeaconState) error {
	bid := msg.Message
	builderIndex := bid.BuilderIndex
	if !state.IsActiveBuilder(validationState, builderIndex) {
		return fmt.Errorf("builder %d is not active", builderIndex)
	}
	if !state.CanBuilderCoverBid(validationState, builderIndex, bid.Value) {
		return fmt.Errorf("%w: builder %d cannot cover bid value %d", ErrIgnore, builderIndex, bid.Value)
	}

	builders := validationState.GetBuilders()
	if builders == nil {
		return fmt.Errorf("builders list not available")
	}
	if builderIndex >= uint64(builders.Len()) {
		return fmt.Errorf("builder index %d out of range (max: %d)", builderIndex, builders.Len())
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		return fmt.Errorf("builder %d not found", builderIndex)
	}
	if builder.Version != s.beaconCfg.PayloadBuilderVersion {
		return fmt.Errorf("builder %d has unsupported version %d", builderIndex, builder.Version)
	}

	epoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	domain, err := validationState.GetDomain(s.beaconCfg.DomainBeaconBuilder, epoch)
	if err != nil {
		return fmt.Errorf("failed to get domain: %w", err)
	}
	signingRoot, err := computeSigningRoot(bid, domain)
	if err != nil {
		return fmt.Errorf("failed to compute signing root: %w", err)
	}
	valid, err := blsVerify(msg.Signature[:], signingRoot[:], builder.Pubkey[:])
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid builder signature")
	}
	return nil
}

// queuePendingBid adds a bid to the pending queue for later processing when preferences arrive.
func (s *executionPayloadBidService) queuePendingBid(msg *cltypes.SignedExecutionPayloadBid) {
	if s.pendingCount.Add(1) > maxPendingBids {
		s.pendingCount.Add(-1)
		return
	}

	key := pendingBidKeyFor(msg)

	if _, loaded := s.pendingBids.LoadOrStore(key, &pendingBidJob{
		msg:          msg,
		creationTime: time.Now(),
	}); loaded {
		s.pendingCount.Add(-1)
	} else {
		s.pendingCond.L.Lock()
		s.pendingCond.Signal()
		s.pendingCond.L.Unlock()
	}
}

func pendingBidKeyFor(msg *cltypes.SignedExecutionPayloadBid) pendingBidKey {
	return pendingBidKey{
		builderIndex:    msg.Message.BuilderIndex,
		slot:            msg.Message.Slot,
		parentBlockHash: msg.Message.ParentBlockHash,
		parentBlockRoot: msg.Message.ParentBlockRoot,
		blockHash:       msg.Message.BlockHash,
		signature:       msg.Signature,
	}
}

// loop is the background goroutine that processes pending bids.
func (s *executionPayloadBidService) loop(ctx context.Context) {
	// Wake any blocked Wait() on context cancellation to prevent deadlock.
	go func() {
		<-ctx.Done()
		s.pendingCond.L.Lock()
		s.pendingCond.Broadcast()
		s.pendingCond.L.Unlock()
	}()

	for {
		// Wait until there are pending bids
		s.pendingCond.L.Lock()
		for s.pendingCount.Load() == 0 {
			select {
			case <-ctx.Done():
				s.pendingCond.L.Unlock()
				return
			default:
			}
			s.pendingCond.Wait()
		}
		s.pendingCond.L.Unlock()

		// Poll until all pending bids are processed
		ticker := time.NewTicker(pendingBidCheckInterval)
		for s.pendingCount.Load() > 0 {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				s.processPendingBids()
			}
		}
		ticker.Stop()
	}
}

// processPendingBids checks pending bids whose proposer preferences may have arrived.
func (s *executionPayloadBidService) processPendingBids() {
	s.pendingBids.Range(func(key, value any) bool {
		pendingKey := key.(pendingBidKey)
		job := value.(*pendingBidJob)

		// Check expiry
		if time.Since(job.creationTime) > pendingBidExpiry {
			s.pendingBids.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Trace("Pending execution payload bid expired",
				"slot", pendingKey.slot, "builderIndex", pendingKey.builderIndex)
			return true
		}

		// Check if bid slot is still valid
		currentSlot := s.ethClock.GetCurrentSlot()
		if pendingKey.slot != currentSlot && pendingKey.slot != currentSlot+1 {
			s.pendingBids.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Trace("Pending execution payload bid slot expired",
				"slot", pendingKey.slot, "builderIndex", pendingKey.builderIndex)
			return true
		}

		if s.seenCache.Contains(seenBidKey{builderIndex: pendingKey.builderIndex, slot: pendingKey.slot}) {
			s.pendingBids.Delete(pendingKey)
			s.pendingCount.Add(-1)
			return true
		}

		preferences, ok, err := s.matchingProposerPreferences(job.msg)
		if err != nil {
			if errors.Is(err, errBidDependencyUnavailable) {
				return true
			}
			s.pendingBids.Delete(pendingKey)
			s.pendingCount.Add(-1)
			log.Trace("Failed to match pending execution payload bid",
				"slot", pendingKey.slot,
				"builderIndex", pendingKey.builderIndex,
				"err", err)
			return true
		}
		if !ok {
			return true // Preferences still not here, keep waiting
		}

		// Preferences arrived, remove from pending and process.
		s.pendingBids.Delete(pendingKey)
		s.pendingCount.Add(-1)

		if err := s.validateAndStoreBid(job.msg, preferences); err != nil {
			log.Trace("Failed to process pending execution payload bid",
				"slot", pendingKey.slot,
				"builderIndex", pendingKey.builderIndex,
				"err", err)
		}
		return true
	})
}
