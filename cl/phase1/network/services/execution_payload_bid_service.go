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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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
	builderIndex uint64
	slot         uint64
}

// pendingBidJob represents a pending bid waiting for proposer preferences to arrive.
type pendingBidJob struct {
	msg          *cltypes.SignedExecutionPayloadBid
	creationTime time.Time
}

const (
	// seenBidCacheSize: multiple builders can bid per slot.
	// With clock disparity we may see bids for ~2 slots.
	// 256 builders * 2 slots = 512 provides safety margin.
	seenBidCacheSize        = 512
	pendingBidExpiry        = 12 * time.Second // 1 slot
	pendingBidCheckInterval = 100 * time.Millisecond
)

type executionPayloadBidService struct {
	syncedDataManager synced_data.SyncedData
	forkchoiceStore   forkchoice.ForkChoiceStorageReader
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool

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

	// [REJECT] bid.execution_payment is zero
	if bid.ExecutionPayment != 0 {
		return fmt.Errorf("bid execution_payment must be zero, got %d", bid.ExecutionPayment)
	}

	// [IGNORE] SignedProposerPreferences for bid.slot has been seen
	preferences, preferencesFound := s.epbsPool.ProposerPreferences.Get(slot)
	if !preferencesFound {
		// Queue as pending — preferences may arrive later
		s.queuePendingBid(msg)
		log.Trace("Queued execution payload bid waiting for proposer preferences",
			"slot", slot, "builderIndex", builderIndex)
		return nil
	}

	return s.validateAndStoreBid(msg, preferences)
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

	// [REJECT] bid.fee_recipient matches preferences
	if bid.FeeRecipient != prefs.FeeRecipient {
		return fmt.Errorf("bid fee_recipient %v does not match preferences %v",
			bid.FeeRecipient, prefs.FeeRecipient)
	}

	// [REJECT] bid.gas_limit matches preferences
	if bid.GasLimit != prefs.GasLimit {
		return fmt.Errorf("bid gas_limit %d does not match preferences %d",
			bid.GasLimit, prefs.GasLimit)
	}

	// [IGNORE] First valid bid from this builder for this slot
	seenKey := seenBidKey{builderIndex: builderIndex, slot: slot}
	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen bid from builder %d for slot %d",
			ErrIgnore, builderIndex, slot)
	}

	// State-dependent checks: IsActiveBuilder, CanBuilderCoverBid, BLS signature
	if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [REJECT] builder is active
		if !state.IsActiveBuilder(headState, builderIndex) {
			return fmt.Errorf("builder %d is not active", builderIndex)
		}

		// [IGNORE] builder can cover the bid
		if !state.CanBuilderCoverBid(headState, builderIndex, bid.Value) {
			return fmt.Errorf("%w: builder %d cannot cover bid value %d",
				ErrIgnore, builderIndex, bid.Value)
		}

		// [REJECT] BLS signature verification
		// Get builder public key from builders list
		builders := headState.GetBuilders()
		if builders == nil {
			return fmt.Errorf("builders list not available")
		}
		if int(builderIndex) >= builders.Len() {
			return fmt.Errorf("builder index %d out of range (max: %d)", builderIndex, builders.Len())
		}
		builder := builders.Get(int(builderIndex))
		if builder == nil {
			return fmt.Errorf("builder %d not found", builderIndex)
		}
		pk := builder.Pubkey

		epoch := s.ethClock.GetEpochAtSlot(slot)
		domain, err := headState.GetDomain(s.beaconCfg.DomainBeaconBuilder, epoch)
		if err != nil {
			return fmt.Errorf("failed to get domain: %w", err)
		}
		signingRoot, err := computeSigningRoot(bid, domain)
		if err != nil {
			return fmt.Errorf("failed to compute signing root: %w", err)
		}
		valid, err := blsVerify(msg.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return fmt.Errorf("signature verification error: %w", err)
		}
		if !valid {
			return fmt.Errorf("invalid builder signature")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("bid validation failed: %w", err)
	}

	// [IGNORE] parent_block_hash is known in fork choice
	if _, ok := s.forkchoiceStore.GetRecentExecutionPayloadStatus(bid.ParentBlockHash); !ok {
		return fmt.Errorf("%w: parent_block_hash %v not known in fork choice",
			ErrIgnore, bid.ParentBlockHash)
	}

	// [IGNORE] parent_block_root is known in fork choice
	if _, ok := s.forkchoiceStore.GetHeader(bid.ParentBlockRoot); !ok {
		return fmt.Errorf("%w: parent_block_root %v not known in fork choice",
			ErrIgnore, bid.ParentBlockRoot)
	}

	// [IGNORE] Highest bid check: only accept if this is the highest value bid for (slot, parentBlockHash)
	bidKey := pool.HighestBidKey{Slot: slot, ParentBlockHash: bid.ParentBlockHash}
	if existing, found := s.epbsPool.HighestBids.Get(bidKey); found {
		if bid.Value <= existing.Message.Value {
			return fmt.Errorf("%w: bid value %d is not higher than existing %d for slot %d",
				ErrIgnore, bid.Value, existing.Message.Value, slot)
		}
	}

	// All checks passed — mark as seen and store
	s.seenCache.Add(seenKey, struct{}{})
	s.epbsPool.HighestBids.Add(bidKey, msg)

	log.Trace("Processed execution payload bid via gossip",
		"slot", slot,
		"builderIndex", builderIndex,
		"value", bid.Value,
		"parentBlockHash", bid.ParentBlockHash,
		"feeRecipient", bid.FeeRecipient)

	return nil
}

// queuePendingBid adds a bid to the pending queue for later processing when preferences arrive.
func (s *executionPayloadBidService) queuePendingBid(msg *cltypes.SignedExecutionPayloadBid) {
	key := pendingBidKey{
		builderIndex: msg.Message.BuilderIndex,
		slot:         msg.Message.Slot,
	}

	if _, loaded := s.pendingBids.LoadOrStore(key, &pendingBidJob{
		msg:          msg,
		creationTime: time.Now(),
	}); !loaded {
		s.pendingCount.Add(1)
		s.pendingCond.Signal()
	}
}

// loop is the background goroutine that processes pending bids.
func (s *executionPayloadBidService) loop(ctx context.Context) {
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

		// Check if preferences have arrived
		preferences, found := s.epbsPool.ProposerPreferences.Get(pendingKey.slot)
		if !found {
			return true // Preferences still not here, keep waiting
		}

		// Preferences arrived, remove from pending and process
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
