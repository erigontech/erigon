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

package consensus_tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func TestGossipMessageTimeOverflow(t *testing.T) {
	_, ok := gossipMessageTime(^uint64(0), 1)
	require.False(t, ok)

	messageTime, ok := gossipMessageTime(12_000, 500)
	require.True(t, ok)
	require.Equal(t, uint64(12_500), messageTime)
}

func TestGossipCurrentSlotRejectsInvalidTimeConfig(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	beaconState := state.New(&config)
	beaconState.SetGenesisTime(^uint64(0))
	_, ok := gossipCurrentSlot(beaconState, 0)
	require.False(t, ok)

	config.SecondsPerSlot = 0
	beaconState = state.New(&config)
	_, ok = gossipCurrentSlot(beaconState, 0)
	require.False(t, ok)
}

func TestGossipSyncSubcommitteePublicKeysRejectsInvalidBounds(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	config.SyncCommitteeSubnetCount = 0
	_, ok := gossipSyncSubcommitteePublicKeys(state.New(&config), 0)
	require.False(t, ok)

	config = clparams.MainnetBeaconConfig
	_, ok = gossipSyncSubcommitteePublicKeys(state.New(&config), config.SyncCommitteeSubnetCount)
	require.False(t, ok)
}

func TestGossipBitsSuperset(t *testing.T) {
	require.True(t, gossipBitsSuperset([]byte{0b1110}, []byte{0b0110}))
	require.True(t, gossipBitsSuperset([]byte{0b0110}, []byte{0b0110}))
	require.False(t, gossipBitsSuperset([]byte{0b0010}, []byte{0b0110}))
	require.False(t, gossipBitsSuperset([]byte{0b0110}, []byte{0b0110, 0}))
}
