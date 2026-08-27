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
	"math"
	"sync"
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

type seenBidKey struct {
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
	builderIndex    uint64
}

type seenBidStore struct {
	mu     sync.RWMutex
	bySlot map[uint64]map[seenBidKey]struct{}
}

func newSeenBidStore() *seenBidStore {
	return &seenBidStore{bySlot: make(map[uint64]map[seenBidKey]struct{})}
}

func (s *seenBidStore) Contains(key seenBidKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.bySlot[key.slot][key]
	return ok
}

func (s *seenBidStore) Add(key seenBidKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bySlot[key.slot] == nil {
		s.bySlot[key.slot] = make(map[seenBidKey]struct{})
	}
	s.bySlot[key.slot][key] = struct{}{}
}

func (s *seenBidStore) PruneExpired(clock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for slot := range s.bySlot {
		if isPastBidWindow(clock, beaconCfg, now, slot) {
			delete(s.bySlot, slot)
		}
	}
}

func newSeenBidKey(bid *cltypes.ExecutionPayloadBid) seenBidKey {
	return seenBidKey{
		slot:            bid.Slot,
		parentBlockHash: bid.ParentBlockHash,
		parentBlockRoot: bid.ParentBlockRoot,
		builderIndex:    bid.BuilderIndex,
	}
}

type bidValidationStateKey struct {
	parentBlockRoot common.Hash
	slot            uint64
}

type bidValidationStateEntry struct {
	mu           sync.Mutex
	state        *state.CachingBeaconState
	parentSlot   uint64
	parentRandao common.Hash
}

var errBidDependencyUnavailable = fmt.Errorf("%w: bid dependency unavailable", ErrIgnore)

const (
	bidValidationStateCacheSize = 4
	bidValidationStateTTLSlots  = 2
	gloasMaximumClockDisparity  = 500 * time.Millisecond
)

type executionPayloadBidService struct {
	syncedDataManager synced_data.SyncedData
	forkchoiceStore   forkchoice.ForkChoiceStorageReader
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool
	emitters          *beaconevents.EventEmitter
	now               func() time.Time

	seenCache            *seenBidStore
	bidStoreMu           sync.Mutex
	validationStateMu    sync.Mutex
	validationStateCache *lru.CacheWithTTL[bidValidationStateKey, *bidValidationStateEntry]
}

// NewExecutionPayloadBidService creates a new execution payload bid gossip service.
// [New in Gloas:EIP7732]
func NewExecutionPayloadBidService(
	_ context.Context,
	syncedDataManager synced_data.SyncedData,
	forkchoiceStore forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	epbsPool *pool.EpbsPool,
	emitters *beaconevents.EventEmitter,
) ExecutionPayloadBidService {
	validationStateCache := lru.NewWithTTL[bidValidationStateKey, *bidValidationStateEntry](
		"execution_payload_bid_validation_states",
		bidValidationStateCacheSize,
		bidValidationStateCacheTTL(beaconCfg),
	)
	s := &executionPayloadBidService{
		syncedDataManager:    syncedDataManager,
		forkchoiceStore:      forkchoiceStore,
		ethClock:             ethClock,
		beaconCfg:            beaconCfg,
		epbsPool:             epbsPool,
		emitters:             emitters,
		now:                  time.Now,
		seenCache:            newSeenBidStore(),
		validationStateCache: validationStateCache,
	}
	return s
}

func (s *executionPayloadBidService) Names() []string {
	return []string{gossip.TopicNameExecutionPayloadBid}
}

func (s *executionPayloadBidService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedExecutionPayloadBid, error) {
	msg := &cltypes.SignedExecutionPayloadBid{}
	if err := msg.DecodeSSZStrict(data, int(version)); err != nil {
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

	now := s.now()
	// [IGNORE] bid.slot is the current or next slot
	if !isCurrentOrNextSlot(s.ethClock, s.beaconCfg, now, slot, gloasMaximumClockDisparity) {
		return fmt.Errorf("%w: bid slot %d is not current or next slot", ErrIgnore, slot)
	}
	s.epbsPool.HighestBids.PruneSlots(func(entrySlot uint64) bool {
		return isPastBidWindow(s.ethClock, s.beaconCfg, now, entrySlot)
	})
	s.epbsPool.ProposerPreferences.PruneSlots(func(entrySlot uint64) bool {
		return isPastBidWindow(s.ethClock, s.beaconCfg, now, entrySlot)
	})

	seenKey := newSeenBidKey(bid)
	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen bid from builder %d for slot %d",
			ErrIgnore, builderIndex, slot)
	}
	if err := s.validateHighestBid(bid); err != nil {
		return err
	}
	parentHeader, ok := s.forkchoiceStore.GetHeader(bid.ParentBlockRoot)
	if ok && slot <= parentHeader.Slot {
		return fmt.Errorf("bid slot %d is not greater than parent block slot %d", slot, parentHeader.Slot)
	}
	if err := s.validateBidStateless(bid); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: parent_block_root %v not known in fork choice", ErrIgnore, bid.ParentBlockRoot)
	}
	preferences, ok, err := s.matchingProposerPreferences(msg)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: proposer preferences not available", ErrIgnore)
	}

	prefs := preferences.Message
	if prefs == nil {
		return fmt.Errorf("%w: proposer preferences not available", ErrIgnore)
	}
	if bid.FeeRecipient != prefs.FeeRecipient {
		return fmt.Errorf("%w: bid fee_recipient %v does not match proposer preferences %v",
			ErrIgnore, bid.FeeRecipient, prefs.FeeRecipient)
	}
	if _, ok := s.forkchoiceStore.GetRecentExecutionPayloadStatus(bid.ParentBlockHash); !ok {
		return fmt.Errorf("%w: parent_block_hash %v not known in fork choice", ErrIgnore, bid.ParentBlockHash)
	}
	parentGasLimit, ok := s.forkchoiceStore.GetExecutionPayloadGasLimit(bid.ParentBlockHash)
	if !ok {
		return fmt.Errorf("%w: gas limit for parent_block_hash %v not available", ErrIgnore, bid.ParentBlockHash)
	}
	if !IsGasLimitTargetCompatible(parentGasLimit, bid.GasLimit, prefs.TargetGasLimit) {
		return fmt.Errorf("%w: bid gas_limit %d not compatible with target %d (parent %d)",
			ErrIgnore, bid.GasLimit, prefs.TargetGasLimit, parentGasLimit)
	}
	compatible, err := s.isBidCompatibleWithHead(bid)
	if err != nil {
		return err
	}
	if !compatible {
		return fmt.Errorf("%w: bid is not compatible with the current head branch", ErrIgnore)
	}
	validationStateEntry, err := s.bidValidationState(bid.ParentBlockRoot, bid.Slot)
	if err != nil {
		return err
	}
	if err := s.validateBidAuthentication(msg, validationStateEntry); err != nil {
		return err
	}
	if err := s.storeValidBidAt(msg, now); err != nil {
		return err
	}
	s.emitters.Operation().SendExecutionPayloadBid(msg)
	log.Trace("Processed execution payload bid via gossip", "slot", slot, "builderIndex", builderIndex,
		"value", bid.Value, "parentBlockHash", bid.ParentBlockHash)
	return nil
}

func (s *executionPayloadBidService) ValidateBid(_ context.Context, msg *cltypes.SignedExecutionPayloadBid) error {
	if msg == nil || msg.Message == nil {
		return errors.New("nil execution payload bid message")
	}
	bid := msg.Message
	if !isCurrentOrNextSlot(s.ethClock, s.beaconCfg, s.now(), bid.Slot, gloasMaximumClockDisparity) {
		return fmt.Errorf("%w: bid slot %d is not current or next slot", ErrIgnore, bid.Slot)
	}
	parentHeader, ok := s.forkchoiceStore.GetHeader(bid.ParentBlockRoot)
	if !ok {
		return fmt.Errorf("%w: parent_block_root %v not known in fork choice", ErrIgnore, bid.ParentBlockRoot)
	}
	if bid.Slot <= parentHeader.Slot {
		return fmt.Errorf("bid slot %d is not greater than parent block slot %d", bid.Slot, parentHeader.Slot)
	}
	if err := s.validateBidBlobLimit(bid); err != nil {
		return err
	}
	if _, ok := s.forkchoiceStore.GetRecentExecutionPayloadStatus(bid.ParentBlockHash); !ok {
		return fmt.Errorf("%w: parent_block_hash %v not known in fork choice", ErrIgnore, bid.ParentBlockHash)
	}
	validationStateEntry, err := s.bidValidationState(bid.ParentBlockRoot, bid.Slot)
	if err != nil {
		return err
	}
	return s.validateBidAuthentication(msg, validationStateEntry)
}

func isCurrentOrNextSlot(clock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, now time.Time, slot uint64, disparity time.Duration) bool {
	if slot == ^uint64(0) {
		return false
	}
	lowerSlot := slot
	if slot > 0 {
		lowerSlot--
	}
	lowerSlotTime, ok := safeSlotTime(clock, beaconCfg, lowerSlot)
	if !ok {
		return false
	}
	upperSlotTime, ok := safeSlotTime(clock, beaconCfg, slot+1)
	if !ok {
		return false
	}
	lowerBound := lowerSlotTime.Add(-disparity)
	upperBound := upperSlotTime.Add(disparity)
	return !now.Before(lowerBound) && !now.After(upperBound)
}

func safeSlotTime(clock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, slot uint64) (time.Time, bool) {
	if beaconCfg == nil || beaconCfg.SecondsPerSlot == 0 {
		return time.Time{}, false
	}
	genesisTime := clock.GenesisTime()
	if slot > (math.MaxUint64-genesisTime)/beaconCfg.SecondsPerSlot {
		return time.Time{}, false
	}
	slotTime := genesisTime + slot*beaconCfg.SecondsPerSlot
	if slotTime > math.MaxInt64 {
		return time.Time{}, false
	}
	return time.Unix(int64(slotTime), 0), true
}

func safeMultiplyUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func (s *executionPayloadBidService) matchingProposerPreferences(msg *cltypes.SignedExecutionPayloadBid) (*cltypes.SignedProposerPreferences, bool, error) {
	bid := msg.Message
	if _, ok := s.forkchoiceStore.GetHeader(bid.ParentBlockRoot); !ok {
		return nil, false, fmt.Errorf("%w: parent_block_root %v not available", errBidDependencyUnavailable, bid.ParentBlockRoot)
	}
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	dependentRoot, err := s.shufflingDependentRoot(bid.ParentBlockRoot, proposalEpoch)
	if err != nil {
		return nil, false, err
	}
	if dependentRoot == (common.Hash{}) {
		return nil, false, fmt.Errorf("%w: failed to compute proposer dependent root", ErrIgnore)
	}
	preferences, ok := s.epbsPool.GetPreference(bid.Slot, dependentRoot)
	return preferences, ok, nil
}

func (s *executionPayloadBidService) shufflingDependentRoot(root common.Hash, epoch uint64) (common.Hash, error) {
	dependentSlot := uint64(0)
	if epoch > s.beaconCfg.MinSeedLookahead {
		dependentSlot = (epoch-s.beaconCfg.MinSeedLookahead)*s.beaconCfg.SlotsPerEpoch - 1
	}
	return s.forkchoiceStore.Ancestor(root, dependentSlot).Root, nil
}

func (s *executionPayloadBidService) validateBidStateless(bid *cltypes.ExecutionPayloadBid) error {
	if bid.ExecutionPayment != 0 {
		return fmt.Errorf("bid execution_payment must be 0, got %d", bid.ExecutionPayment)
	}
	return s.validateBidBlobLimit(bid)
}

func (s *executionPayloadBidService) validateBidBlobLimit(bid *cltypes.ExecutionPayloadBid) error {
	epoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	maxBlobsPerBlock := int(s.beaconCfg.GetBlobParameters(epoch).MaxBlobsPerBlock)
	if bid.BlobKzgCommitments.Len() > maxBlobsPerBlock {
		return fmt.Errorf("bid has too many blob_kzg_commitments: %d > %d",
			bid.BlobKzgCommitments.Len(), maxBlobsPerBlock)
	}
	return nil
}

func bidValidationStateCacheTTL(beaconCfg *clparams.BeaconChainConfig) time.Duration {
	secondsPerSlot := uint64(12)
	if beaconCfg != nil && beaconCfg.SecondsPerSlot != 0 {
		secondsPerSlot = beaconCfg.SecondsPerSlot
	}
	return time.Duration(secondsPerSlot*bidValidationStateTTLSlots) * time.Second
}

func (s *executionPayloadBidService) validateBidAuthentication(msg *cltypes.SignedExecutionPayloadBid, validationStateEntry *bidValidationStateEntry) error {
	bid := msg.Message
	if bid.Slot <= validationStateEntry.parentSlot {
		return fmt.Errorf("bid slot %d is not greater than parent block slot %d", bid.Slot, validationStateEntry.parentSlot)
	}
	validationStateEntry.mu.Lock()
	if bid.PrevRandao != validationStateEntry.parentRandao {
		validationStateEntry.mu.Unlock()
		return fmt.Errorf("bid prev_randao does not match parent state randao mix")
	}
	validationState, err := validationStateEntry.state.Copy()
	validationStateEntry.mu.Unlock()
	if err != nil {
		return fmt.Errorf("bid validation failed: failed to copy parent state: %w", err)
	}
	if validationState.Slot() != bid.Slot {
		if err := transition.DefaultMachine.ProcessSlots(validationState, bid.Slot); err != nil {
			return fmt.Errorf("bid validation failed: failed to advance parent state: %w", err)
		}
	}

	builder, err := s.validateBuilderAvailability(bid, validationState)
	if err != nil {
		return fmt.Errorf("bid validation failed: %w", err)
	}
	builderPubkey := builder.Pubkey
	epoch := state.GetEpochAtSlot(s.beaconCfg, bid.Slot)
	domain, err := validationState.GetDomain(s.beaconCfg.DomainBeaconBuilder, epoch)
	if err != nil {
		return fmt.Errorf("bid validation failed: failed to get domain: %w", err)
	}
	if err := validateBuilderBidSignature(msg, domain, builderPubkey); err != nil {
		return fmt.Errorf("bid validation failed: %w", err)
	}

	return nil
}

func (s *executionPayloadBidService) storeValidBidAt(msg *cltypes.SignedExecutionPayloadBid, now time.Time) error {
	bid := msg.Message
	seenKey := newSeenBidKey(bid)

	s.bidStoreMu.Lock()
	defer s.bidStoreMu.Unlock()
	if !now.IsZero() {
		s.seenCache.PruneExpired(s.ethClock, s.beaconCfg, now)
		s.epbsPool.HighestBids.PruneSlots(func(entrySlot uint64) bool {
			return isPastBidWindow(s.ethClock, s.beaconCfg, now, entrySlot)
		})
	}

	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen bid from builder %d for slot %d",
			ErrIgnore, bid.BuilderIndex, bid.Slot)
	}
	if err := s.validateHighestBid(bid); err != nil {
		return err
	}
	s.seenCache.Add(seenKey)
	bidKey := pool.HighestBidKey{Slot: bid.Slot, ParentBlockHash: bid.ParentBlockHash, ParentBlockRoot: bid.ParentBlockRoot}
	s.epbsPool.StoreHighestBid(bidKey, msg)
	return nil
}

func (s *executionPayloadBidService) isBidCompatibleWithHead(bid *cltypes.ExecutionPayloadBid) (bool, error) {
	headNode, err := s.forkchoiceStore.GetHeadNode()
	if err != nil {
		return false, fmt.Errorf("%w: head unavailable: %w", errBidDependencyUnavailable, err)
	}
	headRoot := headNode.Root
	headHeader, ok := s.forkchoiceStore.GetHeader(headRoot)
	if !ok {
		return false, fmt.Errorf("%w: head block header unavailable", errBidDependencyUnavailable)
	}
	headBlock, hasBlock := s.forkchoiceStore.GetBlock(headRoot)
	if hasBlock && headBlock != nil && headBlock.Block != nil && headBlock.Block.Body != nil {
		signedHeadBid := headBlock.Block.Body.GetSignedExecutionPayloadBid()
		if signedHeadBid != nil && signedHeadBid.Message != nil {
			return bidCompatibleWithHead(bid, headRoot, headHeader, signedHeadBid.Message, s.forkchoiceStore.ShouldBuildOnFull(headNode, bid.Slot)), nil
		}
		if headPayload := headBlock.Block.Body.ExecutionPayload; headPayload != nil {
			return bid.ParentBlockRoot == headRoot && bid.ParentBlockHash == headPayload.BlockHash, nil
		}
	}
	headState, err := s.forkchoiceStore.GetStateAtBlockRoot(headRoot, true)
	if err != nil || headState == nil {
		return false, fmt.Errorf("%w: head state unavailable", errBidDependencyUnavailable)
	}
	if headState.Version() >= clparams.GloasVersion {
		headBid := headState.GetLatestExecutionPayloadBid()
		if headBid == nil {
			return false, fmt.Errorf("%w: head bid unavailable", errBidDependencyUnavailable)
		}
		return bidCompatibleWithHead(bid, headRoot, headHeader, headBid, s.forkchoiceStore.ShouldBuildOnFull(headNode, bid.Slot)), nil
	}
	headPayload := headState.LatestExecutionPayloadHeader()
	if headPayload == nil {
		return false, fmt.Errorf("%w: head execution payload unavailable", errBidDependencyUnavailable)
	}
	return bid.ParentBlockRoot == headRoot && bid.ParentBlockHash == headPayload.BlockHash, nil
}

func bidCompatibleWithHead(bid *cltypes.ExecutionPayloadBid, headRoot common.Hash, headHeader *cltypes.BeaconBlockHeader, headBid *cltypes.ExecutionPayloadBid, buildOnFull bool) bool {
	buildsOnParentBlock := bid.ParentBlockRoot == headHeader.ParentRoot
	buildsOnParentPayload := bid.ParentBlockHash == headBid.ParentBlockHash
	if buildsOnParentBlock && buildsOnParentPayload {
		return true
	}
	if bid.ParentBlockRoot != headRoot {
		return false
	}
	if buildOnFull {
		return bid.ParentBlockHash == headBid.BlockHash
	}
	return buildsOnParentPayload
}

func (s *executionPayloadBidService) validateHighestBid(bid *cltypes.ExecutionPayloadBid) error {
	bidKey := pool.HighestBidKey{Slot: bid.Slot, ParentBlockHash: bid.ParentBlockHash, ParentBlockRoot: bid.ParentBlockRoot}
	existing, found := s.epbsPool.GetHighestBid(bidKey)
	if !found || existing == nil || existing.Message == nil {
		return nil
	}
	if bid.Value <= existing.Message.Value {
		return fmt.Errorf("%w: bid value %d is not higher than existing %d for slot %d",
			ErrIgnore, bid.Value, existing.Message.Value, bid.Slot)
	}
	return nil
}

func (s *executionPayloadBidService) bidValidationState(parentBlockRoot common.Hash, bidSlot uint64) (*bidValidationStateEntry, error) {
	cacheKey := bidValidationStateKey{parentBlockRoot: parentBlockRoot, slot: bidSlot}
	s.validationStateMu.Lock()
	entry, ok := s.validationStateCache.Get(cacheKey)
	if !ok {
		entry = &bidValidationStateEntry{}
		s.validationStateCache.Add(cacheKey, entry)
	}
	s.validationStateMu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.state != nil {
		return entry, nil
	}

	// Fetch the parent state only on a cache miss; holding entry.mu also
	// dedups concurrent fetches for the same (parent, slot).
	parentState, err := s.forkchoiceStore.GetStateAtBlockRoot(parentBlockRoot, true)
	if err != nil || parentState == nil {
		s.removeBidValidationState(cacheKey, entry)
		return nil, fmt.Errorf("%w: state for parent_block_root %v not available", errBidDependencyUnavailable, parentBlockRoot)
	}
	if parentState.Slot() > bidSlot {
		s.removeBidValidationState(cacheKey, entry)
		return nil, fmt.Errorf("parent state slot %d is after bid slot %d", parentState.Slot(), bidSlot)
	}
	entry.parentSlot = parentState.Slot()
	entry.parentRandao = parentState.GetRandaoMixes(state.Epoch(parentState))
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, bidSlot)
	if proposalEpoch > state.Epoch(parentState)+s.beaconCfg.MinSeedLookahead {
		s.removeBidValidationState(cacheKey, entry)
		return nil, fmt.Errorf("%w: bid slot is past the parent's proposer lookahead", ErrIgnore)
	}
	entry.state = parentState
	return entry, nil
}

func (s *executionPayloadBidService) removeBidValidationState(cacheKey bidValidationStateKey, entry *bidValidationStateEntry) {
	s.validationStateMu.Lock()
	defer s.validationStateMu.Unlock()
	current, ok := s.validationStateCache.Get(cacheKey)
	if ok && current == entry {
		s.validationStateCache.Remove(cacheKey)
	}
}

func validateBuilderBidSignature(msg *cltypes.SignedExecutionPayloadBid, domain []byte, builderPubkey common.Bytes48) error {
	signingRoot, err := computeSigningRoot(msg.Message, domain)
	if err != nil {
		return fmt.Errorf("failed to compute signing root: %w", err)
	}
	valid, err := blsVerify(msg.Signature[:], signingRoot[:], builderPubkey[:])
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid builder signature")
	}
	return nil
}

func (s *executionPayloadBidService) validateBuilderAvailability(
	bid *cltypes.ExecutionPayloadBid,
	validationState *state.CachingBeaconState,
) (*cltypes.Builder, error) {
	builderIndex := bid.BuilderIndex
	builders := validationState.GetBuilders()
	if builders == nil {
		return nil, fmt.Errorf("builders list not available")
	}
	if builderIndex >= uint64(builders.Len()) {
		return nil, fmt.Errorf("builder index %d out of range (max: %d)", builderIndex, builders.Len())
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		return nil, fmt.Errorf("builder %d not found", builderIndex)
	}
	if !state.CanBuilderCoverBid(validationState, builderIndex, bid.Value) {
		return nil, fmt.Errorf("%w: builder %d cannot cover bid value %d", ErrIgnore, builderIndex, bid.Value)
	}
	if !state.IsActiveBuilder(validationState, builderIndex) {
		return nil, fmt.Errorf("builder %d is not active", builderIndex)
	}
	if builder.Version != s.beaconCfg.PayloadBuilderVersion {
		return nil, fmt.Errorf("builder %d has unsupported version %d", builderIndex, builder.Version)
	}
	return builder, nil
}
