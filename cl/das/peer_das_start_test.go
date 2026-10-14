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

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
)

func TestPeerDasSubscribesOnlyAfterStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	gossipManager := gossipmock.NewMockGossip(ctrl)
	beaconConfig := clparams.MainnetBeaconConfig
	beaconConfig.DataColumnSidecarSubnetCount = 2
	caplinConfig := clparams.CaplinConfig{ArchiveBlobs: true}
	peerDasState := peerdasstate.NewPeerDasState(&beaconConfig, &clparams.NetworkConfig{})

	peerDas := NewPeerDas(nil, &beaconConfig, &caplinConfig, nil, nil, nil, [32]byte{}, nil, peerDasState, gossipManager, nil, nil)

	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).Times(2)
	ctx, cancel := context.WithCancel(context.Background())
	peerDas.Start(ctx)
	peerDas.Start(ctx)
	cancel()
}
