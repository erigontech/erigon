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
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var pubkeyIndexDeltaSize = metrics.GetOrCreateGauge("beacon_state_pubkey_index_delta_size")

func (b *CachingBeaconState) CopyInto(bs *CachingBeaconState) (err error) {
	return b.copyInto(bs, nil)
}

func (b *CachingBeaconState) copyInto(bs *CachingBeaconState, totalActiveBalance *uint64) (err error) {
	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}

	bs.publicKeyIndices = pubkeyIndexFromBase(b.publicKeyIndices.base, bs.ValidatorSet())
	pubkeyIndexDeltaSize.SetInt(len(bs.publicKeyIndices.delta))
	err = bs.reinitCaches(totalActiveBalance)
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

func (b *CachingBeaconState) Copy() (bs *CachingBeaconState, err error) {
	return b.copy(nil)
}

func (b *CachingBeaconState) CopyWithTotalActiveBalance(total uint64) (*CachingBeaconState, error) {
	return b.copy(&total)
}

func (b *CachingBeaconState) copy(totalActiveBalance *uint64) (bs *CachingBeaconState, err error) {
	copied := New(b.BeaconConfig())
	err = b.copyInto(copied, totalActiveBalance)
	if err != nil {
		return nil, err
	}
	return copied, nil
}
