package eth_clock

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// ENR eth2 field: with no future fork scheduled the spec requires
// next_fork_version == current_fork_version, so a fork left at FAR_FUTURE_EPOCH
// must not contribute its version.
func TestForkIdNextForkVersionWithoutScheduledFork(t *testing.T) {
	for _, tc := range []struct {
		name           string
		chainID        clparams.NetworkType
		currentVersion uint32
	}{
		{"chiado", chainspec.ChiadoChainID, 0x0600006f},
		{"gnosis", chainspec.GnosisChainID, 0x06000064},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, beaconCfg := clparams.GetConfigsByNetwork(tc.chainID)
			clock := NewEthereumClock(beaconCfg.MinGenesisTime, common.Hash{}, beaconCfg)

			forkID, err := clock.ForkId()
			require.NoError(t, err)
			require.Len(t, forkID, 16)

			nextForkEpoch := binary.BigEndian.Uint64(forkID[8:])
			require.Equal(t, beaconCfg.FarFutureEpoch, nextForkEpoch,
				"precondition: no fork is scheduled after the current one")

			require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(tc.currentVersion)), common.Bytes4(forkID[4:8]),
				"next_fork_version must fall back to the current fork version")
		})
	}
}
