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

// RemoveVote compacts the target validator out of the root's vote list in place
// (no per-call allocation), keeps the other voters, and drops the root once empty.
func TestRemoveVoteCompactsInPlace(t *testing.T) {
	w := newIndexedWeightStoreTestStore().indexedWeightStore
	root := common.HexToHash("0xabc")
	w.IndexVote(1, LatestMessage{Root: root})
	w.IndexVote(2, LatestMessage{Root: root})
	w.IndexVote(3, LatestMessage{Root: root})
	require.Len(t, w.directVotes[root], 3)

	w.RemoveVote(2, root)
	got := w.directVotes[root]
	require.Len(t, got, 2)
	require.ElementsMatch(t, []uint64{1, 3}, []uint64{got[0].ValidatorIndex, got[1].ValidatorIndex})

	w.RemoveVote(1, root)
	w.RemoveVote(3, root)
	_, ok := w.directVotes[root]
	require.False(t, ok, "root entry must be deleted once its last vote is removed")
}

// seedFromLatestMessages imports every latestMessage once and is a no-op
// thereafter, so repeated head computations cannot double-count a vote.
func TestSeedFromLatestMessagesIsIdempotent(t *testing.T) {
	f := newIndexedWeightStoreTestStore()
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.indexedWeightStore
	root := common.HexToHash("0xabc")
	f.latestMessages.set(0, LatestMessage{Root: root, Slot: 1})
	f.latestMessages.set(1, LatestMessage{Root: root, Slot: 2})
	require.Empty(t, w.directVotes, "index starts cold")

	w.seedFromLatestMessages()
	require.Len(t, w.directVotes[root], 2, "seed mirrors latestMessages")

	w.seedFromLatestMessages()
	require.Len(t, w.directVotes[root], 2, "second seed must be a no-op")
}
