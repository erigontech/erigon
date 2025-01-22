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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
)

func (b *CachingBeaconState) CopyInto(bs *CachingBeaconState) (err error) {
	if bs.BeaconState == nil {
		bs.BeaconState = raw.New(b.BeaconConfig())
	}
	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}
	err = bs.reinitCaches()
	if err != nil {
		return err
	}
	return nil
}

func (bs *CachingBeaconState) reinitCaches() error {
	if bs.Version() == clparams.Phase0Version {
		return bs.InitBeaconState()
	}
	if bs.publicKeyIndicies == nil {
		bs.publicKeyIndicies = make(map[[48]byte]uint64)
	}

	// We regenerate public keys from the copied state to avoid concurrency issues.
	for k, idx := range bs.publicKeyIndicies {
		if idx >= uint64(bs.ValidatorSet().Length()) {
			delete(bs.publicKeyIndicies, k)
		}
		pk := bs.ValidatorSet().Get(int(idx)).PublicKey()
		if pk != k {
			delete(bs.publicKeyIndicies, k)
		}
	}
	bs.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		pk := v.PublicKey()
		if _, ok := bs.publicKeyIndicies[pk]; ok {
			return true
		}
		bs.publicKeyIndicies[pk] = uint64(idx)
		return true
	})

	bs.totalActiveBalanceCache = nil
	bs._refreshActiveBalancesIfNeeded()
	bs.previousStateRoot = common.Hash{}
	bs.initCaches()
	if err := bs._updateProposerIndex(); err != nil {
		return err
	}
	if bs.Version() >= clparams.Phase0Version {
		return bs._initializeValidatorsPhase0()
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
