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
	require.Equal(t, digest, [4]byte{74, 38, 197, 139})
}
