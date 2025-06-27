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
	"golang.org/x/exp/maps"
)

func (b *CachingBeaconState) CopyInto(bs *CachingBeaconState) (err error) {
	if bs.BeaconState == nil {
		bs.BeaconState = raw.New(b.BeaconConfig())
	}
	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}
	err = bs.reinitCaches(bs)
	if err != nil {
		return err
	}
	return nil
}

func (bs *CachingBeaconState) reinitPublicKeysRegistry(other *CachingBeaconState) error {
	if other != nil {
		blockRoot, err := bs.BlockRoot()
		if err != nil {
			return err
		}
		haveBlockRoot, err := other.GetBlockRootAtSlot(bs.Slot())
		if err != nil {
			return err
		}
		if haveBlockRoot == blockRoot {
			// if it is an ancestor, you can update the registry until you find a matching public key.
			for i := other.ValidatorLength() - 1; i >= 0; i-- {
				pk, err := other.ValidatorPublicKey(int(i))
				if err != nil {
					return err
				}
				if bs.publicKeyIndicies[pk] == uint64(i) {
					// found a matching public key, no need to reinitialize the registry.
					return nil
				}
				// otherwise, remove the public key from the registry.
				bs.publicKeyIndicies[pk] = uint64(i)
			}

		}
	}

	if bs.publicKeyIndicies == nil {
		bs.publicKeyIndicies = make(map[[48]byte]uint64)
	} else {
		maps.Clear(bs.publicKeyIndicies)
	}

	bs.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		bs.publicKeyIndicies[v.PublicKey()] = uint64(idx)
		return true
	})

	return nil
}

func (bs *CachingBeaconState) reinitCaches(other *CachingBeaconState) error {
	if bs.Version() == clparams.Phase0Version {
		return bs.InitBeaconState()
	}

	if err := bs.reinitPublicKeysRegistry(other); err != nil {
		return err
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
