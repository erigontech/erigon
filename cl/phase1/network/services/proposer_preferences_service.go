package services

import (
	"context"
	"fmt"

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

type seenProposerPreferencesKey struct {
	validatorIndex uint64
	slot           uint64
	dependentRoot  common.Hash
}

const seenProposerPreferencesCacheSize = 128 // ~2 epochs of slots * some buffer

type proposerPreferencesService struct {
	syncedDataManager synced_data.SyncedData
	forkchoiceStore   forkchoice.ForkChoiceStorageReader
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool

	seenCache *lru.Cache[seenProposerPreferencesKey, struct{}]
}

// NewProposerPreferencesService creates a new proposer preferences gossip service.
// [New in Gloas:EIP7732]
func NewProposerPreferencesService(
	syncedDataManager synced_data.SyncedData,
	forkchoiceStore forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	beaconCfg *clparams.BeaconChainConfig,
	epbsPool *pool.EpbsPool,
) ProposerPreferencesService {
	seenCache, err := lru.New[seenProposerPreferencesKey, struct{}]("seen_proposer_preferences", seenProposerPreferencesCacheSize)
	if err != nil {
		panic(err)
	}
	return &proposerPreferencesService{
		syncedDataManager: syncedDataManager,
		forkchoiceStore:   forkchoiceStore,
		ethClock:          ethClock,
		beaconCfg:         beaconCfg,
		epbsPool:          epbsPool,
		seenCache:         seenCache,
	}
}

func (s *proposerPreferencesService) Names() []string {
	return []string{gossip.TopicNameProposerPreferences}
}

func (s *proposerPreferencesService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.SignedProposerPreferences, error) {
	msg := &cltypes.SignedProposerPreferences{}
	if err := msg.DecodeSSZ(data, int(version)); err != nil {
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

	// [IGNORE] compute_epoch_at_slot(preferences.proposal_slot) in range(current_epoch, current_epoch + MIN_SEED_LOOKAHEAD + 1)
	currentEpoch := s.ethClock.GetCurrentEpoch()
	proposalEpoch := state.GetEpochAtSlot(s.beaconCfg, proposalSlot)
	if proposalEpoch < currentEpoch || proposalEpoch > currentEpoch+s.beaconCfg.MinSeedLookahead {
		return fmt.Errorf("%w: proposal slot %d is in epoch %d, expected epoch in [%d, %d]",
			ErrIgnore, proposalSlot, proposalEpoch, currentEpoch, currentEpoch+s.beaconCfg.MinSeedLookahead)
	}

	// [IGNORE] The proposal slot has not already passed (proposal_slot > current_slot)
	currentSlot := s.ethClock.GetCurrentSlot()
	if proposalSlot <= currentSlot {
		return fmt.Errorf("%w: proposal slot %d has already passed (current slot %d)",
			ErrIgnore, proposalSlot, currentSlot)
	}

	// [IGNORE] First valid message from this (validator_index, proposal_slot, dependent_root)
	seenKey := seenProposerPreferencesKey{
		validatorIndex: validatorIndex,
		slot:           proposalSlot,
		dependentRoot:  preferences.DependentRoot,
	}
	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen proposer preferences from validator %d for slot %d with dependent root %v",
			ErrIgnore, validatorIndex, proposalSlot, preferences.DependentRoot)
	}

	depState, err := s.forkchoiceStore.GetStateAtBlockRoot(preferences.DependentRoot, false)
	if err != nil || depState == nil {
		return fmt.Errorf("%w: state for dependent_root %v not available", ErrIgnore, preferences.DependentRoot)
	}
	validationState, err := s.proposerPreferencesValidationState(depState, proposalEpoch)
	if err != nil {
		return fmt.Errorf("%w: failed to prepare dependent state: %v", ErrIgnore, err)
	}
	if err := s.validateProposerPreferencesWithState(msg, validationState); err != nil {
		return fmt.Errorf("proposer preferences validation failed: %w", err)
	}

	// All checks passed — mark as seen and store in pool
	s.seenCache.Add(seenKey, struct{}{})
	s.epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot:          proposalSlot,
		DependentRoot: preferences.DependentRoot,
	}, msg)

	log.Trace("Processed proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex,
		"feeRecipient", preferences.FeeRecipient,
		"targetGasLimit", preferences.TargetGasLimit)

	return nil
}

func (s *proposerPreferencesService) proposerPreferencesValidationState(depState *state.CachingBeaconState, proposalEpoch uint64) (*state.CachingBeaconState, error) {
	if proposalEpoch < s.beaconCfg.MinSeedLookahead {
		return nil, fmt.Errorf("proposal epoch %d before min seed lookahead %d", proposalEpoch, s.beaconCfg.MinSeedLookahead)
	}
	dependentEpoch := proposalEpoch - s.beaconCfg.MinSeedLookahead
	validationSlot := dependentEpoch * s.beaconCfg.SlotsPerEpoch
	if depState.Slot() >= validationSlot {
		return depState, nil
	}
	validationState, err := depState.Copy()
	if err != nil {
		return nil, err
	}
	if err := transition.DefaultMachine.ProcessSlots(validationState, validationSlot); err != nil {
		return nil, err
	}
	return validationState, nil
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
