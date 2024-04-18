package eth_clock

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
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
	require.Equal(t, common.Bytes4{0xf5, 0xa5, 0xfd, 0x42}, currDigest)
	nextDigest, err := clock.NextForkDigest()
	require.NoError(t, err)
	lastFork, err := clock.LastFork()
	require.NoError(t, err)
	require.Equal(t, lastFork, nextDigest)
	expectedForkId := make([]byte, 16)
	copy(expectedForkId, currDigest[:])
	forkId, err := clock.ForkId()
	require.NoError(t, err)
	require.Equal(t, expectedForkId, forkId)
}
