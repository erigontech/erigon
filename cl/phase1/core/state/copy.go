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
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/common"
	"golang.org/x/exp/maps"
)

func (b *CachingBeaconState) CopyInto(bs *CachingBeaconState) (err error) {
	// If we are copying into our parent state, we can optimize some stuff
	// aka we can unwind less of the public key indices cache
	var fixedCachesUnwind bool
	if bs.BeaconState == nil {
		bs.BeaconState = raw.New(b.BeaconConfig())
	}

	// if the state we are copying into is the parent state, we can optimize some stuff
	blockRoot, _ := bs.BlockRoot()
	fixedCachesUnwind = blockRoot == b.LatestBlockHeader().ParentRoot

	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}

	err = bs.reinitCaches(fixedCachesUnwind)
	if err != nil {
		return err
	}
	return nil
}

func (bs *CachingBeaconState) reinitCaches(fixedCachesUnwind bool) error {
	if bs.Version() == clparams.Phase0Version {
		return bs.InitBeaconState()
	}

	if bs.publicKeyIndicies != nil && fixedCachesUnwind {
		const opsCount = 256
		startIdx := len(bs.publicKeyIndicies) - opsCount
		if startIdx < 0 {
			startIdx = 0
		}
		for i := startIdx; i < bs.ValidatorLength(); i++ {
			v := bs.Validators().Get(i)
			bs.publicKeyIndicies[v.PublicKey()] = uint64(i)
		}
	} else {
		if bs.publicKeyIndicies == nil {
			bs.publicKeyIndicies = make(map[[48]byte]uint64)
		} else {
			maps.Clear(bs.publicKeyIndicies)
		}

		bs.ForEachValidator(func(v solid.Validator, idx, total int) bool {
			bs.publicKeyIndicies[v.PublicKey()] = uint64(idx)
			return true
		})
	}

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
