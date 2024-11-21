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

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	st "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

type proposerSlashingService struct {
	operationsPool    pool.OperationsPool
	syncedDataManager synced_data.SyncedData
	beaconCfg         *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	emitters          *beaconevents.EventEmitter
	cache             *lru.Cache[uint64, struct{}]
}

func NewProposerSlashingService(
	operationsPool pool.OperationsPool,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	emitters *beaconevents.EventEmitter,
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
		emitters:          emitters,
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
		return errors.New("proposee slashing headers are the same")
	}

	return s.syncedDataManager.ViewHeadState(func(state *st.CachingBeaconState) error {
		proposer, err := state.ValidatorForValidatorIndex(int(h1.ProposerIndex))
		if err != nil {
			return fmt.Errorf("unable to retrieve state: %v", err)
		}
		if !proposer.IsSlashable(s.ethClock.GetCurrentEpoch()) {
			return fmt.Errorf("proposer is not slashable: %v", proposer)
		}

		// Verify signatures for both headers
		for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{msg.Header1, msg.Header2} {
			domain, err := state.GetDomain(s.beaconCfg.DomainBeaconProposer, st.GetEpochAtSlot(s.beaconCfg, signedHeader.Header.Slot))
			if err != nil {
				return fmt.Errorf("unable to get domain: %v", err)
			}
			pk := proposer.PublicKey()
			signingRoot, err := computeSigningRoot(signedHeader, domain)
			if err != nil {
				return fmt.Errorf("unable to compute signing root: %v", err)
			}
			valid, err := blsVerify(signedHeader.Signature[:], signingRoot[:], pk[:])
			if err != nil {
				return fmt.Errorf("unable to verify signature: %v", err)
			}
			if !valid {
				return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", signedHeader.Signature[:], signingRoot[:], pk)
			}
		}

		s.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(msg), msg)
		s.cache.Add(pIndex, struct{}{})
		s.emitters.Operation().SendProposerSlashing(msg)
		return nil
	})
}
