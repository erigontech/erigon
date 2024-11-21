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
	err = b.copyCachesInto(bs)
	if err != nil {
		return err
	}
	return nil
}

func (b *CachingBeaconState) copyCachesInto(bs *CachingBeaconState) error {
	if b.Version() == clparams.Phase0Version {
		return bs.InitBeaconState()
	}
	if bs.publicKeyIndicies == nil {
		bs.publicKeyIndicies = make(map[[48]byte]uint64)
	}
	for k, idx := range bs.publicKeyIndicies {
		if otherIdx, ok := b.publicKeyIndicies[k]; ok {
			if idx != otherIdx {
				delete(bs.publicKeyIndicies, k)
			}
			continue
		}
		delete(bs.publicKeyIndicies, k)
	}
	for pk, index := range b.publicKeyIndicies {
		bs.publicKeyIndicies[pk] = index
	}
	// Sync caches
	bs.activeValidatorsCache = copyLRU(bs.activeValidatorsCache, b.activeValidatorsCache)
	bs.shuffledSetsCache = copyLRU(bs.shuffledSetsCache, b.shuffledSetsCache)

	if b.totalActiveBalanceCache != nil {
		bs.totalActiveBalanceCache = new(uint64)
		*bs.totalActiveBalanceCache = *b.totalActiveBalanceCache
		bs.totalActiveBalanceRootCache = b.totalActiveBalanceRootCache
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
