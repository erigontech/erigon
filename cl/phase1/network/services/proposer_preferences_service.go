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
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

type seenProposerPreferencesKey struct {
	validatorIndex uint64
	slot           uint64
}

const seenProposerPreferencesCacheSize = 128 // ~2 epochs of slots * some buffer

type proposerPreferencesService struct {
	syncedDataManager synced_data.SyncedData
	ethClock          eth_clock.EthereumClock
	beaconCfg         *clparams.BeaconChainConfig
	epbsPool          *pool.EpbsPool

	seenCache *lru.Cache[seenProposerPreferencesKey, struct{}]
}

// NewProposerPreferencesService creates a new proposer preferences gossip service.
// [New in Gloas:EIP7732]
func NewProposerPreferencesService(
	syncedDataManager synced_data.SyncedData,
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

	// [IGNORE] preferences.proposal_slot is in the next epoch
	// i.e. compute_epoch_at_slot(preferences.proposal_slot) == get_current_epoch(state) + 1
	currentEpoch := s.ethClock.GetCurrentEpoch()
	proposalEpoch := s.ethClock.GetEpochAtSlot(proposalSlot)
	if proposalEpoch != currentEpoch+1 {
		return fmt.Errorf("%w: proposal slot %d is in epoch %d, expected next epoch %d",
			ErrIgnore, proposalSlot, proposalEpoch, currentEpoch+1)
	}

	// [IGNORE] First valid message from this validator+slot
	seenKey := seenProposerPreferencesKey{
		validatorIndex: validatorIndex,
		slot:           proposalSlot,
	}
	if s.seenCache.Contains(seenKey) {
		return fmt.Errorf("%w: already seen proposer preferences from validator %d for slot %d",
			ErrIgnore, validatorIndex, proposalSlot)
	}

	// [REJECT] is_valid_proposal_slot + [REJECT] BLS signature verification
	// Both need head state access, so do them together inside a single ViewHeadState call.
	if err := s.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// is_valid_proposal_slot check:
		// index = SLOTS_PER_EPOCH + preferences.proposal_slot % SLOTS_PER_EPOCH
		// return state.proposer_lookahead[index] == preferences.validator_index
		lookahead := headState.GetProposerLookahead()
		if lookahead == nil {
			return fmt.Errorf("proposer lookahead not available")
		}
		lookaheadIndex := s.beaconCfg.SlotsPerEpoch + proposalSlot%s.beaconCfg.SlotsPerEpoch
		if int(lookaheadIndex) >= lookahead.Length() {
			return fmt.Errorf("proposer lookahead index %d out of range (length: %d)", lookaheadIndex, lookahead.Length())
		}
		if lookahead.Get(int(lookaheadIndex)) != validatorIndex {
			return fmt.Errorf("validator %d is not the proposer for slot %d (expected %d)",
				validatorIndex, proposalSlot, lookahead.Get(int(lookaheadIndex)))
		}

		// Get validator public key
		val, err := headState.ValidatorForValidatorIndex(int(validatorIndex))
		if err != nil {
			return fmt.Errorf("validator index %d not found: %w", validatorIndex, err)
		}
		pk := val.PublicKey()

		// BLS signature verification
		epoch := s.ethClock.GetEpochAtSlot(proposalSlot)
		domain, err := headState.GetDomain(s.beaconCfg.DomainProposerPreferences, epoch)
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
	}); err != nil {
		return fmt.Errorf("proposer preferences validation failed: %w", err)
	}

	// All checks passed — mark as seen and store in pool
	s.seenCache.Add(seenKey, struct{}{})
	s.epbsPool.ProposerPreferences.Add(proposalSlot, msg)

	log.Trace("Processed proposer preferences via gossip",
		"proposalSlot", proposalSlot,
		"validatorIndex", validatorIndex,
		"feeRecipient", preferences.FeeRecipient,
		"gasLimit", preferences.GasLimit)

	return nil
}
