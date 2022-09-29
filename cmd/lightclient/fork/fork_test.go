package fork

import (
	"testing"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/stretchr/testify/require"
)

func TestMainnetFork(t *testing.T) {
	digest, err := ComputeForkDigest(clparams.BeaconConfigs[clparams.MainnetNetwork], clparams.GenesisConfigs[clparams.MainnetNetwork])
	require.NoError(t, err)
	require.Equal(t, digest, [4]byte{74, 38, 197, 139})
}
