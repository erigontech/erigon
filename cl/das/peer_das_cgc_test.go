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

package das

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestPeerDasRestoresHighestCustodyGroupCount(t *testing.T) {
	beaconConfig := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&beaconConfig)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	caplinConfig := clparams.CaplinConfig{}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctrl := gomock.NewController(t)
	gossipManager := gossipmock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).AnyTimes()

	firstNode := newCgcTestLocalNode(t)
	firstState := peerdasstate.NewPeerDasState(&beaconConfig, &networkConfig)
	firstState.SetLocalNodeID(firstNode)
	firstPeerDas := NewPeerDas(nil, &beaconConfig, &caplinConfig, nil, nil, nil, firstNode.ID(), nil, firstState, gossipManager, nil, db)
	firstPeerDas.UpdateValidatorsCustody(96)

	secondNode := newCgcTestLocalNode(t)
	secondState := peerdasstate.NewPeerDasState(&beaconConfig, &networkConfig)
	secondState.SetLocalNodeID(secondNode)
	secondPeerDas := NewPeerDas(nil, &beaconConfig, &caplinConfig, nil, nil, nil, secondNode.ID(), nil, secondState, gossipManager, nil, db)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	secondPeerDas.Start(ctx)

	require.Equal(t, uint64(96), secondState.GetAdvertisedCgc())
	var enrCgc []byte
	require.NoError(t, secondNode.Node().Load(enr.WithEntry(networkConfig.CgcKey, &enrCgc)))
	require.Equal(t, peerdasstate.EncodeCgc(96), enrCgc)
}

func TestPeerDasStartWithoutStoredCustodyGroupCountKeepsRequirement(t *testing.T) {
	beaconConfig := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&beaconConfig)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ctrl := gomock.NewController(t)
	gossipManager := gossipmock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).AnyTimes()
	localNode := newCgcTestLocalNode(t)
	state := peerdasstate.NewPeerDasState(&beaconConfig, &networkConfig)
	state.SetLocalNodeID(localNode)
	peerDas := NewPeerDas(nil, &beaconConfig, &clparams.CaplinConfig{}, nil, nil, nil, localNode.ID(), nil, state, gossipManager, nil, db)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	peerDas.Start(ctx)

	require.Equal(t, beaconConfig.CustodyRequirement, state.GetAdvertisedCgc())
}

func newCgcTestLocalNode(t testing.TB) *enode.LocalNode {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	return enode.NewLocalNode(nodeDB, key)
}
