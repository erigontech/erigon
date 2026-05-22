package handlers

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/stretchr/testify/require"
)

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}
