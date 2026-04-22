package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
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

	storeMu sync.Mutex
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
		return fmt.Errorf("nil proposer preferences message")
	}

	preferences := msg.Message
	proposalSlot := preferences.ProposalSlot
	validatorIndex := preferences.ValidatorIndex

	log.Trace("Received proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex)

	now := s.now()
	past, validTime := isPastSlot(s.ethClock, s.beaconCfg, now, proposalSlot, gloasMaximumClockDisparity)
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
	if proposalEpoch < s.beaconCfg.MinSeedLookahead {
		return fmt.Errorf("%w: proposal epoch %d before min seed lookahead %d", ErrIgnore, proposalEpoch, s.beaconCfg.MinSeedLookahead)
	}
	lookaheadEpoch := proposalEpoch - s.beaconCfg.MinSeedLookahead
	lookaheadEpochStartSlot, ok := safeMultiplyUint64(lookaheadEpoch, s.beaconCfg.SlotsPerEpoch)
	if !ok {
		return fmt.Errorf("%w: proposer lookahead slot is not representable", ErrIgnore)
	}
	lookaheadEpochStartTime, ok := safeSlotTime(s.ethClock, s.beaconCfg, lookaheadEpochStartSlot)
	if !ok {
		return fmt.Errorf("%w: proposer lookahead slot %d has no representable time", ErrIgnore, lookaheadEpochStartSlot)
	}
	if now.Add(gloasMaximumClockDisparity).Before(lookaheadEpochStartTime) {
		return fmt.Errorf("%w: proposer for proposal slot %d is not yet known", ErrIgnore, proposalSlot)
	}
	s.epbsPool.ProposerPreferences.PruneSlots(func(entrySlot uint64) bool {
		return isPastBidWindow(s.ethClock, s.beaconCfg, now, entrySlot)
	})

	// [IGNORE] First valid message for this dependent root and proposal slot.
	seenKey := newSeenProposerPreferencesKey(preferences)
	if s.hasSeenPreference(seenKey) {
		return fmt.Errorf("%w: already seen proposer preferences from validator %d for slot %d with dependent root %v",
			ErrIgnore, validatorIndex, proposalSlot, preferences.DependentRoot)
	}
	dependentHeader, ok := s.forkchoiceStore.GetHeader(preferences.DependentRoot)
	if !ok {
		return fmt.Errorf("%w: dependent block %v has not been seen", ErrIgnore, preferences.DependentRoot)
	}
	depState, err := s.forkchoiceStore.GetStateAtBlockRoot(preferences.DependentRoot, true)
	if err != nil || depState == nil {
		return fmt.Errorf("%w: state for dependent_root %v not available", ErrIgnore, preferences.DependentRoot)
	}
	if dependentHeader.Slot >= lookaheadEpochStartSlot {
		return fmt.Errorf("dependent root slot %d is not before proposer lookahead slot %d", dependentHeader.Slot, lookaheadEpochStartSlot)
	}
	if !s.isValidDependentRoot(preferences.DependentRoot, lookaheadEpochStartSlot) {
		return fmt.Errorf("%w: dependent root is not a possible dependent block", ErrIgnore)
	}

	validationState, err := s.proposerPreferencesValidationState(depState, proposalEpoch)
	if err != nil {
		return fmt.Errorf("%w: failed to prepare dependent state: %w", ErrIgnore, err)
	}
	if err := s.validateProposerPreferencesWithState(msg, validationState); err != nil {
		return fmt.Errorf("proposer preferences validation failed: %w", err)
	}

	s.storeMu.Lock()
	if s.hasSeenPreference(seenKey) {
		s.storeMu.Unlock()
		return fmt.Errorf("%w: already seen proposer preferences from validator %d for slot %d with dependent root %v",
			ErrIgnore, validatorIndex, proposalSlot, preferences.DependentRoot)
	}
	s.epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot:          proposalSlot,
		DependentRoot: preferences.DependentRoot,
	}, msg)
	s.storeMu.Unlock()
	if s.emitters != nil {
		s.emitters.Operation().SendProposerPreferences(&beaconevents.VersionedSignedProposerPreferences{Version: clparams.GloasVersion.String(), Data: msg})
	}

	// Notify builder (if wired) so it can wake up without polling.
	if cb := s.epbsPool.OnPreferencesReceived; cb != nil {
		cb(proposalSlot, msg)
	}

	log.Trace("Processed proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex,
		"feeRecipient", preferences.FeeRecipient,
		"targetGasLimit", preferences.TargetGasLimit)

	return nil
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
	return !ok || now.After(nextSlotTime.Add(gloasMaximumClockDisparity))
}

func (s *proposerPreferencesService) isValidDependentRoot(root common.Hash, epochStartSlot uint64) bool {
	if s.forkchoiceStore.HasBlockChildAtOrAfter(root, epochStartSlot) {
		return true
	}
	headRoot, _, err := s.forkchoiceStore.GetHead(nil)
	return err == nil && root == headRoot
}

func (s *proposerPreferencesService) proposerPreferencesValidationState(depState *state.CachingBeaconState, proposalEpoch uint64) (*state.CachingBeaconState, error) {
	if proposalEpoch < s.beaconCfg.MinSeedLookahead {
		return nil, fmt.Errorf("proposal epoch %d before min seed lookahead %d", proposalEpoch, s.beaconCfg.MinSeedLookahead)
	}
	dependentEpoch := proposalEpoch - s.beaconCfg.MinSeedLookahead
	validationSlot, ok := safeMultiplyUint64(dependentEpoch, s.beaconCfg.SlotsPerEpoch)
	if !ok {
		return nil, fmt.Errorf("dependent validation slot is not representable")
	}
	if depState.Slot() >= validationSlot {
		return depState, nil
	}
	if err := transition.DefaultMachine.ProcessSlots(depState, validationSlot); err != nil {
		return nil, err
	}
	return depState, nil
}

func (s *proposerPreferencesService) validateProposerPreferencesWithState(msg *cltypes.SignedProposerPreferences, depState *state.CachingBeaconState) error {
	preferences := msg.Message
	proposalSlot := preferences.ProposalSlot
	validatorIndex := preferences.ValidatorIndex
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, proposalSlot)
	stateEpoch := state.GetEpochAtSlot(depState.BeaconConfig(), depState.Slot())
	if proposalEpoch < stateEpoch || proposalEpoch > stateEpoch+s.beaconCfg.MinSeedLookahead {
		return fmt.Errorf("proposal slot %d is outside dependent state lookahead", proposalSlot)
	}
	lookahead := depState.GetProposerLookahead()
	if lookahead == nil {
		return fmt.Errorf("proposer lookahead not available")
	}
	lookaheadIndex := (proposalEpoch-stateEpoch)*s.beaconCfg.SlotsPerEpoch + proposalSlot%s.beaconCfg.SlotsPerEpoch
	if int(lookaheadIndex) >= lookahead.Length() {
		return fmt.Errorf("proposer lookahead index %d out of range (length: %d)", lookaheadIndex, lookahead.Length())
	}
	if lookahead.Get(int(lookaheadIndex)) != validatorIndex {
		return fmt.Errorf("validator %d is not the proposer for slot %d (expected %d)",
			validatorIndex, proposalSlot, lookahead.Get(int(lookaheadIndex)))
	}

	val, err := depState.ValidatorForValidatorIndex(int(validatorIndex))
	if err != nil {
		return fmt.Errorf("validator index %d not found: %w", validatorIndex, err)
	}
	pk := val.PublicKey()
	domain, err := depState.GetDomain(s.beaconCfg.DomainProposerPreferences, proposalEpoch)
	if err != nil {
		return fmt.Errorf("failed to get domain: %w", err)
	}
	signingRoot, err := computeSigningRoot(preferences, domain)
	if err != nil {
		return fmt.Errorf("failed to compute signing root: %w", err)
	}
	valid, err := blsVerify(msg.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid proposer preferences signature")
	}
	return nil
}
