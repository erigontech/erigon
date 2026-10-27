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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	gossipnames "github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/p2p/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
)

var persistentSubnetNodeID = enode.HexID(strings.Repeat("0c", 32))

func newPersistentSubnetGossipManager(
	t *testing.T,
	ctrl *gomock.Controller,
	epoch *atomic.Uint64,
	subscribeAll bool,
) (*GossipManager, *mock_services.MockP2PManager, time.Time, common.Bytes4) {
	t.Helper()
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	digest := common.Bytes4{0x11, 0x22, 0x33, 0x44}
	baseTime := time.Now().Add(24 * time.Hour)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().CurrentForkDigest().Return(digest, nil).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().DoAndReturn(epoch.Load).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).DoAndReturn(func(slot uint64) time.Time {
		return baseTime.Add(time.Duration(slot) * 12 * time.Second)
	}).AnyTimes()
	clock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 {
		return epoch.Load() * beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	clock.EXPECT().GetEpochAtSlot(gomock.Any()).DoAndReturn(func(slot uint64) uint64 {
		return slot / beaconConfig.SlotsPerEpoch
	}).AnyTimes()
	clock.EXPECT().ComputeForkDigest(gomock.Any()).Return(digest, nil).AnyTimes()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	ps, err := pubsub.NewGossipSub(t.Context(), h, pubsub.WithPeerScore(
		&pubsub.PeerScoreParams{
			Topics:            make(map[string]*pubsub.TopicScoreParams),
			AppSpecificScore:  func(peer.ID) float64 { return 0 },
			AppSpecificWeight: 1,
			DecayInterval:     time.Second,
			DecayToZero:       0.01,
			RetainScore:       time.Minute,
		},
		&pubsub.PeerScoreThresholds{},
	))
	require.NoError(t, err)
	p2pManager := mock_services.NewMockP2PManager(ctrl)
	p2pManager.EXPECT().Pubsub().Return(ps).AnyTimes()
	p2pManager.EXPECT().Host().Return(h).AnyTimes()
	p2pManager.EXPECT().BandwidthCounter().Return(metrics.NewBandwidthCounter()).AnyTimes()
	manager := NewGossipManager(
		t.Context(), p2pManager, beaconConfig, networkConfig, clock, subscribeAll, 0,
		datasize.MB, datasize.MB, false,
	)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	return manager, p2pManager, baseTime, digest
}

func persistentAttestationService(networkConfig *clparams.NetworkConfig) *mockService {
	names := make([]string, networkConfig.AttestationSubnetCount)
	for subnet := range networkConfig.AttestationSubnetCount {
		names[subnet] = gossipnames.TopicNameBeaconAttestation(subnet)
	}
	return &mockService{namesFunc: func() []string { return names }}
}

func requireLiveAttestationSubnets(
	t *testing.T,
	manager *GossipManager,
	digest common.Bytes4,
	networkConfig *clparams.NetworkConfig,
	want map[uint64]time.Time,
) {
	t.Helper()
	for subnet := range networkConfig.AttestationSubnetCount {
		subscription := manager.subscriptions.Get(composeTopic(digest, gossipnames.TopicNameBeaconAttestation(subnet)))
		require.NotNil(t, subscription)
		if expiry, ok := want[subnet]; ok {
			require.NotNil(t, subscription.sub, "subnet %d", subnet)
			require.Equal(t, expiry, subscription.expiry, "subnet %d", subnet)
		} else {
			require.Nil(t, subscription.sub, "subnet %d", subnet)
		}
	}
}

func TestPersistentAttestationSubnetsStartupOrder(t *testing.T) {
	for _, setBeforeRegistration := range []bool{true, false} {
		name := "node ID after registration"
		wantENRCalls := 1
		if setBeforeRegistration {
			name = "node ID before registration"
			wantENRCalls = 2
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var epoch atomic.Uint64
			epoch.Store(243)
			manager, p2pManager, baseTime, digest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
			networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
			p2pManager.EXPECT().SetENRAttSubnets([]uint64{25, 26}).Times(wantENRCalls)
			if setBeforeRegistration {
				manager.SetLocalNodeID(persistentSubnetNodeID)
			}
			_, _, err := manager.registerGossipService(persistentAttestationService(networkConfig))
			require.NoError(t, err)
			if !setBeforeRegistration {
				manager.SetLocalNodeID(persistentSubnetNodeID)
			}

			expiry := baseTime.Add(time.Duration(244*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)
			requireLiveAttestationSubnets(t, manager, digest, networkConfig, map[uint64]time.Time{25: expiry, 26: expiry})
		})
	}
}

func TestPersistentAttestationSubnetsRotate(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	epoch.Store(243)
	manager, p2pManager, baseTime, digest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	_, _, err := manager.registerGossipService(persistentAttestationService(networkConfig))
	require.NoError(t, err)
	firstExpiry := baseTime.Add(time.Duration(244*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)
	secondExpiry := baseTime.Add(time.Duration(500*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)
	gomock.InOrder(
		p2pManager.EXPECT().SetENRAttSubnets([]uint64{25, 26}),
		p2pManager.EXPECT().SetENRAttSubnets([]uint64{33, 34}),
	)

	manager.SetLocalNodeID(persistentSubnetNodeID)
	requireLiveAttestationSubnets(t, manager, digest, networkConfig, map[uint64]time.Time{25: firstExpiry, 26: firstExpiry})
	epoch.Store(244)
	manager.subscribePersistentAttestationSubnets()
	requireLiveAttestationSubnets(t, manager, digest, networkConfig, map[uint64]time.Time{
		25: firstExpiry,
		26: firstExpiry,
		33: secondExpiry,
		34: secondExpiry,
	})
}

func TestPersistentAttestationSubnetDutyDoesNotShortenExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	epoch.Store(243)
	manager, p2pManager, baseTime, digest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	_, _, err := manager.registerGossipService(persistentAttestationService(networkConfig))
	require.NoError(t, err)
	p2pManager.EXPECT().SetENRAttSubnets([]uint64{25, 26})
	manager.SetLocalNodeID(persistentSubnetNodeID)
	persistentExpiry := baseTime.Add(time.Duration(244*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)

	require.NoError(t, manager.SubscribeWithExpiry(gossipnames.TopicNameBeaconAttestation(25), time.Now().Add(time.Hour)))
	subscription := manager.subscriptions.Get(composeTopic(digest, gossipnames.TopicNameBeaconAttestation(25)))
	require.NotNil(t, subscription.sub)
	require.Equal(t, persistentExpiry, subscription.expiry)
}

func TestDutySubscriptionIncludesUpcomingDigest(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	manager, _, _, currentDigest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
	name := gossipnames.TopicNameBeaconAttestation(1)
	service := &mockService{namesFunc: func() []string { return []string{name} }}
	_, _, err := manager.registerGossipService(service)
	require.NoError(t, err)
	upcomingDigest := common.Bytes4{0xaa, 0xbb, 0xcc, 0xdd}
	require.NoError(t, manager.subscribeUpcomingTopics(upcomingDigest))

	expiry := time.Now().Add(time.Hour)
	require.NoError(t, manager.SubscribeWithExpiry(name, expiry))

	for _, digest := range []common.Bytes4{currentDigest, upcomingDigest} {
		subscription := manager.subscriptions.Get(composeTopic(digest, name))
		require.NotNil(t, subscription)
		require.NotNil(t, subscription.sub)
		require.Equal(t, expiry, subscription.expiry)
	}
}

func TestDutySubscriptionContinuesAfterAnotherDigestFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	manager, _, _, currentDigest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
	name := gossipnames.TopicNameBeaconAttestation(1)
	service := &mockService{namesFunc: func() []string { return []string{name} }}
	_, _, err := manager.registerGossipService(service)
	require.NoError(t, err)
	failingDigest := common.Bytes4{0xaa, 0xbb, 0xcc, 0xdd}
	healthyDigest := common.Bytes4{0x55, 0x66, 0x77, 0x88}
	require.NoError(t, manager.subscribeUpcomingTopics(failingDigest))
	require.NoError(t, manager.subscribeUpcomingTopics(healthyDigest))
	failingTopic := composeTopic(failingDigest, name)
	require.NoError(t, manager.subscriptions.GetTopic(failingTopic).Close())

	expiry := time.Now().Add(time.Hour)
	require.NoError(t, manager.SubscribeWithExpiry(name, expiry))

	for _, digest := range []common.Bytes4{currentDigest, healthyDigest} {
		subscription := manager.subscriptions.Get(composeTopic(digest, name))
		require.NotNil(t, subscription)
		require.NotNil(t, subscription.sub)
		require.Equal(t, expiry, subscription.expiry)
	}
}

func TestPersistentAttestationSubnetsSurviveForkRollover(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	epoch.Store(243)
	manager, p2pManager, baseTime, oldDigest := newPersistentSubnetGossipManager(t, ctrl, &epoch, false)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	_, _, err := manager.registerGossipService(persistentAttestationService(networkConfig))
	require.NoError(t, err)
	gomock.InOrder(
		p2pManager.EXPECT().SetENRAttSubnets([]uint64{25, 26}),
		p2pManager.EXPECT().SetENRAttSubnets([]uint64{33, 34}),
	)
	manager.SetLocalNodeID(persistentSubnetNodeID)
	firstExpiry := baseTime.Add(time.Duration(244*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)
	secondExpiry := baseTime.Add(time.Duration(500*beaconConfig.SlotsPerEpoch+1) * 12 * time.Second)
	newDigest := common.Bytes4{0xaa, 0xbb, 0xcc, 0xdd}
	require.NoError(t, manager.subscribeUpcomingTopics(newDigest))
	epoch.Store(244)
	manager.subscribePersistentAttestationSubnets()
	want := map[uint64]time.Time{25: firstExpiry, 26: firstExpiry, 33: secondExpiry, 34: secondExpiry}
	requireLiveAttestationSubnets(t, manager, oldDigest, networkConfig, want)
	requireLiveAttestationSubnets(t, manager, newDigest, networkConfig, want)

	for subnet := range networkConfig.AttestationSubnetCount {
		oldTopic := composeTopic(oldDigest, gossipnames.TopicNameBeaconAttestation(subnet))
		require.NoError(t, manager.subscriptions.Unsubscribe(oldTopic))
		require.NoError(t, manager.subscriptions.Remove(oldTopic))
	}
	requireLiveAttestationSubnets(t, manager, newDigest, networkConfig, want)
}

func TestPersistentAttestationSubnetsSubscribeAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	var epoch atomic.Uint64
	epoch.Store(243)
	manager, _, _, digest := newPersistentSubnetGossipManager(t, ctrl, &epoch, true)
	networkConfig, _ := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	manager.SetLocalNodeID(persistentSubnetNodeID)
	_, _, err := manager.registerGossipService(persistentAttestationService(networkConfig))
	require.NoError(t, err)

	live := 0
	for subnet := range networkConfig.AttestationSubnetCount {
		subscription := manager.subscriptions.Get(composeTopic(digest, gossipnames.TopicNameBeaconAttestation(subnet)))
		require.NotNil(t, subscription)
		if subscription.sub != nil {
			live++
		}
	}
	require.Equal(t, int(networkConfig.AttestationSubnetCount), live)
}
