package services

import (
	"context"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	st "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
)

type proposerSlashingService struct {
	operationsPool    pool.OperationsPool
	syncedDataManager *synced_data.SyncedDataManager
	beaconCfg         *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	cache             *lru.Cache[uint64, struct{}]
}

func NewProposerSlashingService(
	operationsPool pool.OperationsPool,
	syncedDataManager *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
) *proposerSlashingService {
	cache, err := lru.New[uint64, struct{}]("proposer_slashing", proposerSlashingCacheSize)
	if err != nil {
		panic(err)
	}
	return &proposerSlashingService{
		operationsPool:    operationsPool,
		syncedDataManager: syncedDataManager,
		beaconCfg:         beaconCfg,
		ethClock:          ethClock,
		cache:             cache,
	}
}

func (s *proposerSlashingService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.ProposerSlashing) error {
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#proposer_slashing

	// [IGNORE] The proposer slashing is the first valid proposer slashing received for the proposer with index proposer_slashing.signed_header_1.message.proposer_index
	pIndex := msg.Header1.Header.ProposerIndex
	if _, ok := s.cache.Get(pIndex); ok {
		return ErrIgnore
	}

	if s.operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(msg)) {
		return ErrIgnore
	}
	h1 := msg.Header1.Header
	h2 := msg.Header2.Header

	// Verify header slots match
	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	// Verify header proposer indices match
	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf("non-matching proposer indices proposer slashing: %d != %d", h1.ProposerIndex, h2.ProposerIndex)
	}

	// Verify the headers are different
	if *h1 == *h2 {
		return fmt.Errorf("proposee slashing headers are the same")
	}

	// Verify the proposer is slashable
	state := s.syncedDataManager.HeadState()
	if state == nil {
		return ErrIgnore
	}
	proposer, err := state.ValidatorForValidatorIndex(int(h1.ProposerIndex))
	if err != nil {
		return fmt.Errorf("unable to retrieve state: %v", err)
	}
	if !proposer.IsSlashable(s.ethClock.GetCurrentEpoch()) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}

	// Verify signatures for both headers
	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{msg.Header1, msg.Header2} {
		domain, err := state.GetDomain(state.BeaconConfig().DomainBeaconProposer, st.GetEpochAtSlot(state.BeaconConfig(), signedHeader.Header.Slot))
		if err != nil {
			return fmt.Errorf("unable to get domain: %v", err)
		}
		pk := proposer.PublicKey()
		signingRoot, err := fork.ComputeSigningRoot(signedHeader, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %v", err)
		}
		valid, err := bls.Verify(signedHeader.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return fmt.Errorf("unable to verify signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", signedHeader.Signature[:], signingRoot[:], pk)
		}
	}

	s.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(msg), msg)
	s.cache.Add(pIndex, struct{}{})
	return nil
}
