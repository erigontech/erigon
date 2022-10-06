package clparams

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T, n NetworkType) {
	genesis, network, beacon := GetConfigsByNetwork(MainnetNetwork)

	require.Equal(t, *genesis, GenesisConfigs[MainnetNetwork])
	require.Equal(t, *network, NetworkConfigs[MainnetNetwork])
	require.Equal(t, *beacon, BeaconConfigs[MainnetNetwork])
}

func TestGetConfigsByNetwork(t *testing.T) {
	testConfig(t, MainnetNetwork)
	testConfig(t, SepoliaNetwork)
	testConfig(t, GoerliNetwork)
}
