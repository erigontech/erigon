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

package subnets

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
)

func TestComputeSubscribedSubnets(t *testing.T) {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	tests := []struct {
		name         string
		nodeID       enode.ID
		epoch        uint64
		wantSubnets  []uint64
		wantRotation uint64
	}{
		{name: "zero at genesis", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 0, wantSubnets: []uint64{49, 50}, wantRotation: 256},
		{name: "zero before rotation", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 255, wantSubnets: []uint64{49, 50}, wantRotation: 256},
		{name: "zero at rotation", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 256, wantSubnets: []uint64{16, 17}, wantRotation: 512},
		{name: "zero at epoch 1000", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 1000, wantSubnets: []uint64{28, 29}, wantRotation: 1024},
		{name: "zero at epoch 100000", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 100000, wantSubnets: []uint64{35, 36}, wantRotation: 100096},
		{name: "zero wraps subnet count", nodeID: enode.HexID(strings.Repeat("00", 32)), epoch: 53760, wantSubnets: []uint64{63, 0}, wantRotation: 54016},
		{name: "max at genesis", nodeID: enode.HexID(strings.Repeat("ff", 32)), epoch: 0, wantSubnets: []uint64{57, 58}, wantRotation: 1},
		{name: "max at rotation", nodeID: enode.HexID(strings.Repeat("ff", 32)), epoch: 1, wantSubnets: []uint64{55, 56}, wantRotation: 257},
		{name: "max before rotation", nodeID: enode.HexID(strings.Repeat("ff", 32)), epoch: 256, wantSubnets: []uint64{55, 56}, wantRotation: 257},
		{name: "max after rotation", nodeID: enode.HexID(strings.Repeat("ff", 32)), epoch: 257, wantSubnets: []uint64{45, 46}, wantRotation: 513},
		{name: "max at epoch 100000", nodeID: enode.HexID(strings.Repeat("ff", 32)), epoch: 100000, wantSubnets: []uint64{25, 26}, wantRotation: 100097},
		{name: "0c before rotation", nodeID: enode.HexID(strings.Repeat("0c", 32)), epoch: 243, wantSubnets: []uint64{25, 26}, wantRotation: 244},
		{name: "0c at rotation", nodeID: enode.HexID(strings.Repeat("0c", 32)), epoch: 244, wantSubnets: []uint64{33, 34}, wantRotation: 500},
		{name: "0c at epoch 100000", nodeID: enode.HexID(strings.Repeat("0c", 32)), epoch: 100000, wantSubnets: []uint64{61, 62}, wantRotation: 100084},
		{name: "a0 before rotation", nodeID: enode.HexID("a0" + strings.Repeat("00", 30) + "07"), epoch: 248, wantSubnets: []uint64{37, 38}, wantRotation: 249},
		{name: "a0 at rotation", nodeID: enode.HexID("a0" + strings.Repeat("00", 30) + "07"), epoch: 249, wantSubnets: []uint64{60, 61}, wantRotation: 505},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ComputeSubscribedSubnets(beaconConfig, networkConfig, tt.nodeID, tt.epoch)
			require.NoError(t, err)
			require.Equal(t, tt.wantSubnets, got)
			require.Equal(t, tt.wantRotation, SubscribedSubnetsRotationEpoch(tt.nodeID, tt.epoch))
		})
	}
}

func TestComputeSubnetsForSyncCommittee(t *testing.T) {
	s := state.New(&clparams.MainnetBeaconConfig)
	for i := range 3 {
		v := solid.NewValidator()
		v.SetPublicKey(common.Bytes48{byte(i + 1)})
		s.AppendValidator(v)
	}
	committee := s.CurrentSyncCommittee().Copy()
	keys := committee.GetCommittee()
	subnetSize := clparams.MainnetBeaconConfig.SyncCommitteeSize / clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount
	keys[0], keys[1], keys[subnetSize+2] = common.Bytes48{1}, common.Bytes48{1}, common.Bytes48{1}
	keys[len(keys)-1] = common.Bytes48{2}
	committee.SetCommittee(keys)
	require.NoError(t, s.SetCurrentSyncCommittee(committee))

	got, err := ComputeSubnetsForSyncCommittee(s, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, got)
	got, err = ComputeSubnetsForSyncCommittee(s, 0, 1)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, got)
	got, err = ComputeSubnetsForSyncCommittee(s, 0, 2)
	require.NoError(t, err)
	require.Empty(t, got)
	_, err = ComputeSubnetsForSyncCommittee(s, 0, 3)
	require.Error(t, err)
}
