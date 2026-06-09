// Copyright 2026 The Erigon Authors
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

package forkchoice

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func newIndexedWeightStoreTestStore() *ForkChoiceStore {
	f := &ForkChoiceStore{
		beaconCfg:      &clparams.MainnetBeaconConfig,
		latestMessages: newLatestMessagesStore(16),
	}
	f.justifiedCheckpoint.Store(solid.Checkpoint{})
	f.indexedWeightStore = NewIndexedWeightStore(f)
	return f
}

// Pre-GLOAS fork choice computes head with the non-indexed weightStore, so the
// GLOAS-only indexedWeightStore must not be maintained on the pre-GLOAS vote
// path. Maintaining it there is wasted work (per-vote allocation and balance
// lookups under the fork-choice lock) whose result is never read.
func TestPreGloasDoesNotMaintainIndexedWeightStore(t *testing.T) {
	f := newIndexedWeightStoreTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesPreGloas(att, []uint64{1, 2, 3})

	msg, has := f.getLatestMessage(2)
	require.True(t, has, "pre-GLOAS path must still record the latest message")
	require.Equal(t, att.Data.BeaconBlockRoot, msg.Root)

	require.Empty(t, f.indexedWeightStore.directVotes,
		"pre-GLOAS must not populate the GLOAS indexed weight store")
}

// The GLOAS vote path must keep maintaining the indexed weight store.
func TestGloasMaintainsIndexedWeightStore(t *testing.T) {
	f := newIndexedWeightStoreTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesGloas(att, []uint64{1, 2, 3})

	require.Len(t, f.indexedWeightStore.directVotes[att.Data.BeaconBlockRoot], 3,
		"GLOAS path must index votes for the voted root")
}
