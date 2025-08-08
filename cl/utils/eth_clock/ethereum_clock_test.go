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

package eth_clock

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestSlotOperations(t *testing.T) {
	clock := NewEthereumClock(0, common.Hash{}, &clparams.BeaconChainConfig{SecondsPerSlot: 12, SlotsPerEpoch: 32})
	slot := clock.GetCurrentSlot()
	epoch := clock.GetCurrentEpoch()
	require.Equal(t, slot/32, epoch)
	require.True(t, clock.IsSlotCurrentSlotWithMaximumClockDisparity(slot))
	require.False(t, clock.IsSlotCurrentSlotWithMaximumClockDisparity(slot-1000))
}

func TestGetForkDigests(t *testing.T) {
	clock := NewEthereumClock(0, common.Hash{}, &clparams.MainnetBeaconConfig)
	currDigest, err := clock.CurrentForkDigest()
	require.NoError(t, err)
	require.Equal(t, common.Bytes4{0xc8, 0xb9, 0xe6, 0xac}, currDigest)
	nextDigest, err := clock.NextForkDigest()
	require.NoError(t, err)
	lastFork, err := clock.LastFork()
	require.NoError(t, err)
	require.Equal(t, lastFork, nextDigest)
}
