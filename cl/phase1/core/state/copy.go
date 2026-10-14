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

package state

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/maphash"
)

func (b *CachingBeaconState) CopyInto(bs *CachingBeaconState) (err error) {
	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}

	err = bs.reinitCaches()
	if err != nil {
		return err
	}
	// Preserve the previousStateRoot so that transitionSlot can fill
	// latestBlockHeader.Root with the correct (block-attested) state root
	// instead of recomputing HashSSZ(), which may diverge when the
	// incremental hashing cache has been dirtied by fork-choice operations.
	bs.previousStateRoot = b.previousStateRoot
	return nil
}

func (b *CachingBeaconState) reinitCaches() error {
	if b.Version() == clparams.Phase0Version {
		return b.InitBeaconState()
	}

	if b.publicKeyIndicies == nil {
		b.publicKeyIndicies = maphash.NewNonConcurrentMap[uint64]()
	} else {
		b.publicKeyIndicies.Clear()
	}

	b.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		b.publicKeyIndicies.Set(v.PublicKeyBytes(), uint64(idx))
		return true
	})

	b.totalActiveBalanceCache = nil
	b._refreshActiveBalancesIfNeeded()
	b.previousStateRoot = common.Hash{}
	if err := b.initCaches(); err != nil {
		return err
	}
	if err := b._updateProposerIndex(); err != nil {
		return err
	}
	if b.Version() >= clparams.Phase0Version {
		return b._initializeValidatorsPhase0()
	}

	return nil
}

func (b *CachingBeaconState) Copy() (bs *CachingBeaconState, err error) {
	copied := New(b.BeaconConfig())
	err = b.CopyInto(copied)
	if err != nil {
		return nil, err
	}
	return copied, nil
}
