package clparams_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestMaxGossipMessageSizeFollowsSpec(t *testing.T) {
	mainnet := clparams.NetworkConfigs[chainspec.MainnetChainID]
	require.Equal(t, uint64(10485760), mainnet.GossipMaxSize)
	require.Equal(t, uint64(12234442), mainnet.MaxGossipMessageSize())

	small := clparams.NetworkConfig{GossipMaxSize: 1024}
	require.Equal(t, uint64(1<<20), small.MaxGossipMessageSize())
}
