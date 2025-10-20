// Copyright 2024 The Erigon Authors
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
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}

func TestSentinelGossipOnHardFork(t *testing.T) {
	//t.Skip("issue #15001")

	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, _, _, _, _, reader := loadChain(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	bcfg := *beaconConfig
	bcfg.InitializeForkSchedule()

	// mock eth clock
	ctrl := gomock.NewController(t)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	var hardFork atomic.Bool
	hardFork.Store(false)
	ethClock.EXPECT().CurrentForkDigest().DoAndReturn(func() (common.Bytes4, error) {
		if hardFork.Load() {
			forkDigest := common.Bytes4{0x00, 0x00, 0x00, 0x01}
			return forkDigest, nil
		}
		return common.Bytes4{0x00, 0x00, 0x00, 0x00}, nil
	}).AnyTimes()
	ethClock.EXPECT().ForkId().DoAndReturn(func() ([]byte, error) {
		if hardFork.Load() {
			return []byte{0x00, 0x00, 0x00, 0x01}, nil
		}
		return []byte{0x00, 0x00, 0x00, 0x00}, nil
	}).AnyTimes()
	ethClock.EXPECT().NextForkDigest().DoAndReturn(func() (common.Bytes4, error) {
		if hardFork.Load() {
			return common.Bytes4{0x00, 0x00, 0x00, 0x02}, nil
		}
		return common.Bytes4{0x00, 0x00, 0x00, 0x01}, nil
	}).AnyTimes()
	ethClock.EXPECT().GetCurrentEpoch().DoAndReturn(func() uint64 {
		if hardFork.Load() {
			return uint64(1)
		}
		return uint64(0)
	}).AnyTimes()
	ethClock.EXPECT().NextForkEpochIncludeBPO().Return(bcfg.FarFutureEpoch).AnyTimes()

	// Create mock PeerDasStateReader
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	mockPeerDasStateReader.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()

	sentinel1, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  &bcfg,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
		MaxPeerCount:  9999999,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader)
	require.NoError(t, err)
	defer sentinel1.Stop()

	_, err = sentinel1.Start()
	require.NoError(t, err)
	h := sentinel1.host

	sentinel2, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  &bcfg,
		IpAddr:        listenAddrHost,
		Port:          7077,
		EnableBlocks:  true,
		TCPPort:       9123,
		MaxPeerCount:  9999999,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader)
	require.NoError(t, err)
	defer sentinel2.Stop()

	_, err = sentinel2.Start()
	require.NoError(t, err)
	h2 := sentinel2.host

	sub1, err := sentinel1.SubscribeGossip(BeaconBlockSsz, time.Unix(0, math.MaxInt64))
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := sentinel2.SubscribeGossip(BeaconBlockSsz, time.Unix(0, math.MaxInt64))
	require.NoError(t, err)
	defer sub2.Close()
	time.Sleep(200 * time.Millisecond)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	ch := sentinel2.RecvGossip()
	msg := []byte("hello")
	go func() {
		// delay to make sure that the connection is established
		sub1.Publish(msg)
	}()

	ans := <-ch
	require.Equal(t, ans.Data, msg)

	// check if it still works after hard fork
	previousTopic := ans.TopicName
	hardFork.Store(true)
	time.Sleep(1 * time.Second)

	msg = []byte("hello1")
	go func() {
		// delay to make sure that the connection is established
		sub1 = sentinel1.subManager.GetMatchingSubscription(BeaconBlockSsz.Name)
		sub1.Publish(msg)
	}()

	ans = <-ch
	require.Equal(t, ans.Data, msg)
	require.NotEqual(t, previousTopic, ans.TopicName)
}
