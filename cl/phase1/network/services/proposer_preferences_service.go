package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/sync/singleflight"
)

type seenProposerPreferencesKey struct {
	slot          uint64
	dependentRoot common.Hash
}

func newSeenProposerPreferencesKey(preferences *cltypes.ProposerPreferences) seenProposerPreferencesKey {
	return seenProposerPreferencesKey{slot: preferences.ProposalSlot, dependentRoot: preferences.DependentRoot}
}

type proposerPreferencesService struct {
	syncedDataManager synced_data.SyncedData
	forkchoiceStore   forkchoice.ForkChoiceStorageReader
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool
	now               func() time.Time
	emitters          *beaconevents.EventEmitter

	storeMu             sync.Mutex
	validationViews     *lru.Cache[proposerPreferencesViewKey, *proposerPreferencesValidationView]
	validationViewLoads singleflight.Group
}

// NewProposerPreferencesService creates a new proposer preferences gossip service.
// [New in Gloas:EIP7732]
func NewProposerPreferencesService(
	syncedDataManager synced_data.SyncedData,
	forkchoiceStore forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	epbsPool *pool.EpbsPool,
	emitters *beaconevents.EventEmitter,
) ProposerPreferencesService {
	return &proposerPreferencesService{
		syncedDataManager: syncedDataManager,
		forkchoiceStore:   forkchoiceStore,
		ethClock:          ethClock,
		beaconCfg:         beaconCfg,
		epbsPool:          epbsPool,
		now:               time.Now,
		emitters:          emitters,
		validationViews:   newProposerPreferencesViewCache(),
	}
}

func (s *proposerPreferencesService) Names() []string {
	return []string{gossip.TopicNameProposerPreferences}
}

func (s *proposerPreferencesService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedProposerPreferences, error) {
	msg := &cltypes.SignedProposerPreferences{}
	if err := msg.DecodeSSZStrict(data, int(version)); err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *proposerPreferencesService) ProcessMessage(ctx context.Context, _ *uint64, msg *cltypes.SignedProposerPreferences) error {
	if msg == nil || msg.Message == nil {
		return errors.New("nil proposer preferences message")
	}

	preferences := msg.Message
	proposalSlot := preferences.ProposalSlot
	validatorIndex := preferences.ValidatorIndex

	log.Trace("Received proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex)

	now := s.now()
	past, validTime := isPastSlot(s.ethClock, s.beaconCfg, now, proposalSlot, maximumGossipClockDisparity)
	if !validTime {
		return fmt.Errorf("%w: proposal slot %d has no representable time", ErrIgnore, proposalSlot)
	}
	if past {
		return fmt.Errorf("%w: proposal slot %d has already passed", ErrIgnore, proposalSlot)
	}
	if s.beaconCfg.SlotsPerEpoch == 0 {
		return fmt.Errorf("%w: slots per epoch is zero", ErrIgnore)
	}
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, proposalSlot)
	if proposalEpoch < s.beaconCfg.GloasForkEpoch {
		return fmt.Errorf("%w: proposal epoch %d is pre-Gloas", ErrIgnore, proposalEpoch)
	}
	lookaheadEpoch := s.shufflingDependentEpoch(proposalEpoch)
	lookaheadEpochStartSlot, ok := safeMultiplyUint64(lookaheadEpoch, s.beaconCfg.SlotsPerEpoch)
	if !ok {
		return fmt.Errorf("%w: proposer lookahead slot is not representable", ErrIgnore)
	}
	lookaheadEpochStartTime, ok := safeSlotTime(s.ethClock, s.beaconCfg, lookaheadEpochStartSlot)
	if !ok {
		return fmt.Errorf("%w: proposer lookahead slot %d has no representable time", ErrIgnore, lookaheadEpochStartSlot)
	}
	if now.Add(maximumGossipClockDisparity).Before(lookaheadEpochStartTime) {
		return fmt.Errorf("%w: proposer for proposal slot %d is not yet known", ErrIgnore, proposalSlot)
	}
	s.epbsPool.ProposerPreferences.PruneSlots(func(entrySlot uint64) bool {
		return isPastBidWindow(s.ethClock, s.beaconCfg, now, entrySlot)
	})

	// [IGNORE] First valid message for this dependent root and proposal slot.
	seenKey := newSeenProposerPreferencesKey(preferences)
	if s.hasSeenPreference(seenKey) {
		return fmt.Errorf("%w: %w from validator %d for slot %d with dependent root %v",
			ErrIgnore, ErrProposerPreferenceAlreadySeen, validatorIndex, proposalSlot, preferences.DependentRoot)
	}
	dependentHeader, ok := s.forkchoiceStore.GetHeader(preferences.DependentRoot)
	if !ok {
		return fmt.Errorf("%w: dependent block %v has not been seen", ErrIgnore, preferences.DependentRoot)
	}
	dependentSlot := lookaheadEpochStartSlot
	if dependentSlot > 0 {
		dependentSlot--
	}
	// The fork graph stores a header only after the block's state transition succeeded, so the
	// spec's missing-state IGNORE cannot apply here and the state is not loaded.
	if dependentHeader.Slot > dependentSlot {
		return fmt.Errorf("dependent root slot %d is after shuffling dependent slot %d", dependentHeader.Slot, dependentSlot)
	}
	// Ignored with or without the dependent state, so it is checked before loading it.
	if !s.isValidDependentRoot(preferences.DependentRoot, lookaheadEpochStartSlot) {
		return fmt.Errorf("%w: dependent root is not a possible dependent block", ErrIgnore)
	}

	view, err := s.validationView(preferences.DependentRoot, proposalEpoch)
	if err != nil {
		return err
	}
	if err := s.validateProposerPreferences(msg, view); err != nil {
		return fmt.Errorf("proposer preferences validation failed: %w", err)
	}

	s.storeMu.Lock()
	if s.hasSeenPreference(seenKey) {
		s.storeMu.Unlock()
		return fmt.Errorf("%w: %w from validator %d for slot %d with dependent root %v",
			ErrIgnore, ErrProposerPreferenceAlreadySeen, validatorIndex, proposalSlot, preferences.DependentRoot)
	}
	s.epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot:          proposalSlot,
		DependentRoot: preferences.DependentRoot,
	}, msg)
	s.storeMu.Unlock()
	if s.emitters != nil {
		s.emitters.Operation().SendProposerPreferences(&beaconevents.VersionedSignedProposerPreferences{Version: clparams.GloasVersion.String(), Data: msg})
	}

	log.Trace("Processed proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex,
		"feeRecipient", preferences.FeeRecipient,
		"targetGasLimit", preferences.TargetGasLimit)

	return nil
}

// ValidateProposerPreferenceSlot checks that the proposal is in the gossip lookahead and has not passed.
func ValidateProposerPreferenceSlot(
	clock eth_clock.EthereumClock,
	cfg *clparams.BeaconChainConfig,
	proposalSlot uint64,
) (uint64, error) {
	currentEpoch := clock.GetCurrentEpoch()
	proposalEpoch := state.GetEpochAtSlot(cfg, proposalSlot)
	if proposalEpoch < currentEpoch || proposalEpoch > currentEpoch+cfg.MinSeedLookahead {
		return 0, fmt.Errorf("%w: proposal slot %d is in epoch %d, expected epoch in [%d, %d]",
			ErrIgnore, proposalSlot, proposalEpoch, currentEpoch, currentEpoch+cfg.MinSeedLookahead)
	}
	currentSlot := clock.GetCurrentSlot()
	if proposalSlot <= currentSlot {
		return 0, fmt.Errorf("%w: proposal slot %d has already passed (current slot %d)",
			ErrIgnore, proposalSlot, currentSlot)
	}
	return proposalEpoch, nil
}

func isPastSlot(clock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, now time.Time, slot uint64, disparity time.Duration) (bool, bool) {
	slotTime, ok := safeSlotTime(clock, beaconCfg, slot)
	if !ok {
		return false, false
	}
	return now.After(slotTime.Add(disparity)), true
}

func (s *proposerPreferencesService) hasSeenPreference(key seenProposerPreferencesKey) bool {
	_, ok := s.epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: key.slot, DependentRoot: key.dependentRoot})
	return ok
}

func isPastBidWindow(clock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, now time.Time, slot uint64) bool {
	if slot == ^uint64(0) {
		return true
	}
	nextSlotTime, ok := safeSlotTime(clock, beaconCfg, slot+1)
	return !ok || now.After(nextSlotTime.Add(maximumGossipClockDisparity))
}

func (s *proposerPreferencesService) isValidDependentRoot(root common.Hash, epochStartSlot uint64) bool {
	if s.forkchoiceStore.HasBlockChildAtOrAfter(root, epochStartSlot) {
		return true
	}
	headRoot, _, err := s.forkchoiceStore.GetHead(nil)
	return err == nil && root == headRoot
}

func (s *proposerPreferencesService) shufflingDependentEpoch(proposalEpoch uint64) uint64 {
	if proposalEpoch <= s.beaconCfg.MinSeedLookahead {
		return 0
	}
	return proposalEpoch - s.beaconCfg.MinSeedLookahead
}

func (s *proposerPreferencesService) proposerPreferencesValidationState(depState *state.CachingBeaconState, proposalEpoch uint64) (*state.CachingBeaconState, error) {
	dependentEpoch := s.shufflingDependentEpoch(proposalEpoch)
	validationSlot, ok := safeMultiplyUint64(dependentEpoch, s.beaconCfg.SlotsPerEpoch)
	if !ok {
		return nil, errors.New("dependent validation slot is not representable")
	}
	if depState.Slot() >= validationSlot {
		return depState, nil
	}
	if err := transition.DefaultMachine.ProcessSlots(depState, validationSlot); err != nil {
		return nil, err
	}
	return depState, nil
}

// proposerPreferencesValidationView is the part of a dependent state that preference validation
// reads. It is shared by every preference with the same dependent root and epoch, so it must not
// be mutated after construction.
type proposerPreferencesValidationView struct {
	epoch                 uint64
	proposerLookahead     []uint64
	proposerPubkeys       map[uint64]common.Bytes48
	genesisValidatorsRoot common.Hash
}

type proposerPreferencesViewKey struct {
	dependentRoot common.Hash
	epoch         uint64
}

const proposerPreferencesViewCacheSize = 8

func newProposerPreferencesViewCache() *lru.Cache[proposerPreferencesViewKey, *proposerPreferencesValidationView] {
	cache, err := lru.New[proposerPreferencesViewKey, *proposerPreferencesValidationView]("proposerPreferencesValidationViews", proposerPreferencesViewCacheSize)
	if err != nil {
		panic(err)
	}
	return cache
}

func newProposerPreferencesValidationView(validationState *state.CachingBeaconState) (*proposerPreferencesValidationView, error) {
	lookahead := validationState.GetProposerLookahead()
	if lookahead == nil {
		return nil, errors.New("proposer lookahead not available")
	}
	view := &proposerPreferencesValidationView{
		epoch:                 state.GetEpochAtSlot(validationState.BeaconConfig(), validationState.Slot()),
		proposerLookahead:     make([]uint64, lookahead.Length()),
		proposerPubkeys:       make(map[uint64]common.Bytes48, lookahead.Length()),
		genesisValidatorsRoot: validationState.GenesisValidatorsRoot(),
	}
	for i := range view.proposerLookahead {
		validatorIndex := lookahead.Get(i)
		view.proposerLookahead[i] = validatorIndex
		if val, err := validationState.ValidatorForValidatorIndex(int(validatorIndex)); err == nil {
			view.proposerPubkeys[validatorIndex] = val.PublicKey()
		}
	}
	return view, nil
}

// validationView shares one dependent-state load per dependent root and epoch between concurrent
// messages and reuses its result while cached, because loading it can rebuild a full beacon state.
func (s *proposerPreferencesService) validationView(dependentRoot common.Hash, proposalEpoch uint64) (*proposerPreferencesValidationView, error) {
	key := proposerPreferencesViewKey{dependentRoot: dependentRoot, epoch: s.shufflingDependentEpoch(proposalEpoch)}
	if view, ok := s.validationViews.Get(key); ok {
		return view, nil
	}
	view, err, _ := s.validationViewLoads.Do(fmt.Sprintf("%x/%d", key.dependentRoot, key.epoch), func() (any, error) {
		if view, ok := s.validationViews.Get(key); ok {
			return view, nil
		}
		depState, err := s.forkchoiceStore.GetStateAtBlockRoot(dependentRoot)
		if err != nil || depState == nil {
			return nil, fmt.Errorf("%w: state for dependent_root %v not available", ErrIgnore, dependentRoot)
		}
		validationState, err := s.proposerPreferencesValidationState(depState, proposalEpoch)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to prepare dependent state: %w", ErrIgnore, err)
		}
		view, err := newProposerPreferencesValidationView(validationState)
		if err != nil {
			return nil, fmt.Errorf("proposer preferences validation failed: %w", err)
		}
		s.validationViews.Add(key, view)
		return view, nil
	})
	if err != nil {
		return nil, err
	}
	return view.(*proposerPreferencesValidationView), nil
}

func (s *proposerPreferencesService) validateProposerPreferences(msg *cltypes.SignedProposerPreferences, view *proposerPreferencesValidationView) error {
	preferences := msg.Message
	proposalSlot := preferences.ProposalSlot
	validatorIndex := preferences.ValidatorIndex
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, proposalSlot)
	if proposalEpoch < view.epoch || proposalEpoch > view.epoch+s.beaconCfg.MinSeedLookahead {
		return fmt.Errorf("proposal slot %d is outside dependent state lookahead", proposalSlot)
	}
	lookaheadIndex := (proposalEpoch-view.epoch)*s.beaconCfg.SlotsPerEpoch + proposalSlot%s.beaconCfg.SlotsPerEpoch
	if lookaheadIndex >= uint64(len(view.proposerLookahead)) {
		return fmt.Errorf("proposer lookahead index %d out of range (length: %d)", lookaheadIndex, len(view.proposerLookahead))
	}
	if view.proposerLookahead[lookaheadIndex] != validatorIndex {
		return fmt.Errorf("validator %d is not the proposer for slot %d (expected %d)",
			validatorIndex, proposalSlot, view.proposerLookahead[lookaheadIndex])
	}

	pk, ok := view.proposerPubkeys[validatorIndex]
	if !ok {
		return fmt.Errorf("validator index %d not found: %w", validatorIndex, raw.ErrInvalidValidatorIndex)
	}
	return VerifyProposerPreferencesSignature(s.beaconCfg, msg, pk, view.genesisValidatorsRoot)
}

func VerifyProposerPreferencesSignature(
	cfg *clparams.BeaconChainConfig,
	msg *cltypes.SignedProposerPreferences,
	pubkey common.Bytes48,
	genesisValidatorsRoot common.Hash,
) error {
	proposalEpoch := state.GetEpochAtSlot(cfg, msg.Message.ProposalSlot)
	domain, err := fork.ComputeDomainAtEpoch(cfg, cfg.DomainProposerPreferences, proposalEpoch, genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("failed to get domain: %w", err)
	}
	signingRoot, err := computeSigningRoot(msg.Message, domain)
	if err != nil {
		return fmt.Errorf("failed to compute signing root: %w", err)
	}
	valid, err := blsVerify(msg.Signature[:], signingRoot[:], pubkey[:])
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return errors.New("invalid proposer preferences signature")
	}
	return nil
}
