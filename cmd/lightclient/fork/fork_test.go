package fork

import (
	"testing"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/stretchr/testify/require"
)

func TestMainnetFork(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	full, err := ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, digest, [4]byte{74, 38, 197, 139})
	require.Equal(t, full, []byte{0x4a, 0x26, 0xc5, 0x8b, 0x2, 0x0, 0x0, 0x0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
}
