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

package sentinel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// newDetachedTestSentinel builds a started Sentinel with no Caplin runtime
// fork choice attached, over a p2p manager that outlives it.
func newDetachedTestSentinel(t *testing.T, peerDasStateReader peerdasstate.PeerDasStateReader) (*Sentinel, p2p.P2PManager, *clparams.BeaconChainConfig) {
	t.Helper()
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	st, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(st.GenesisTime(), st.GenesisValidatorsRoot(), beaconConfig)

	pm, _, err := p2p.NewP2Pmanager(t.Context(), &p2p.P2PConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        "127.0.0.1",
		Port:          0,
		TCPPort:       0,
		NoDiscovery:   true,
		MaxPeerCount:  100,
	}, log.New(), ethClock)
	require.NoError(t, err)
	t.Cleanup(func() { pm.Host().Close() })

	sent, err := New(t.Context(), &SentinelConfig{
		P2PConfig: p2p.P2PConfig{
			NetworkConfig: networkConfig,
			BeaconConfig:  beaconConfig,
			MaxPeerCount:  100,
		},
		EnableBlocks: true,
	}, ethClock, nil, nil, nil, log.New(), nil, nil, peerDasStateReader, pm)
	require.NoError(t, err)
	t.Cleanup(sent.Stop)
	_, err = sent.Start()
	require.NoError(t, err)
	return sent, pm, beaconConfig
}

// Identity is reachable from the beacon API and over gRPC, including before
// the first Caplin runtime has attached a fork choice.
func TestSentinel_IdentityWithNoForkChoiceAttached(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	stateReader.EXPECT().GetAdvertisedCgc().Return(uint64(8)).AnyTimes()

	sent, _, _ := newDetachedTestSentinel(t, stateReader)

	require.NotPanics(t, func() {
		pid, _, _, _, metadata := sent.Identity()
		require.NotEmpty(t, pid)
		require.NotNil(t, metadata)
		require.NotNil(t, metadata.CustodyGroupCount)
		require.Equal(t, uint64(8), *metadata.CustodyGroupCount,
			"the custody count comes from the shared PeerDAS state, not via the fork choice")
	}, "Identity must answer while no runtime fork choice is attached")
}

// Caplin's standalone entry point and the tests build a Sentinel without
// PeerDAS state at all.
func TestSentinel_IdentityFallsBackToTheCustodyFloor(t *testing.T) {
	sent, _, beaconConfig := newDetachedTestSentinel(t, nil)

	var cgc uint64
	require.NotPanics(t, func() {
		_, _, _, _, metadata := sent.Identity()
		require.NotNil(t, metadata.CustodyGroupCount)
		cgc = *metadata.CustodyGroupCount
	})
	require.Equal(t, beaconConfig.CustodyRequirement, cgc,
		"with no PeerDAS state the floor is the config custody requirement, not zero")
}
