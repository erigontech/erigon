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

package gossip

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

// newRejoinTestManagerFactory returns a factory that builds GossipManagers
// sharing one libp2p host and one pubsub instance, as a process-lifetime
// network session does across Caplin restarts.
func newRejoinTestManagerFactory(t *testing.T) func() *GossipManager {
	t.Helper()
	ctrl := gomock.NewController(t)

	beaconConfig := &clparams.BeaconChainConfig{SlotsPerEpoch: 32, SecondsPerSlot: 12}
	networkConfig := &clparams.NetworkConfig{}

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentEpoch().Return(uint64(10)).AnyTimes()
	clock.EXPECT().GetCurrentSlot().Return(uint64(320)).AnyTimes()
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()
	clock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(common.Bytes4{0xab, 0xcd, 0x12, 0x34}, nil).AnyTimes()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close() })

	ps, err := pubsub.NewGossipSub(t.Context(), host, pubsub.WithMessageIdFn(func(pmsg *pb.Message) string {
		return string(pmsg.Data)
	}))
	require.NoError(t, err)

	p2p := mock_services.NewMockP2PManager(ctrl)
	p2p.EXPECT().Pubsub().Return(ps).AnyTimes()
	p2p.EXPECT().Host().Return(host).AnyTimes()
	p2p.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()

	return func() *GossipManager {
		return NewGossipManager(
			context.Background(), p2p, beaconConfig, networkConfig, clock,
			false, 0,
			datasize.ByteSize(1024*1024), datasize.ByteSize(1024*1024), false,
		)
	}
}

func TestGossipManager_CloseFreesTopicsForTheNextManager(t *testing.T) {
	newManager := newRejoinTestManagerFactory(t)
	service := &mockService{namesFunc: func() []string { return []string{"unknown_topic"} }}

	first := newManager()
	_, _, err := first.registerGossipService(service)
	require.NoError(t, err, "the first manager registers the topic")
	require.NoError(t, first.Close())

	second := newManager()
	t.Cleanup(func() { _ = second.Close() })
	_, _, err = second.registerGossipService(service)
	require.NoError(t, err,
		"a closed manager must leave the topic free to join again, or the "+
			"runtime that replaces it has no gossip at all")
}
