package sentinel

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
)

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}

func TestSentinelGossipOnHardFork(t *testing.T) {
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, _, _, _, _, reader := loadChain(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	bcfg := *beaconConfig

	s, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), &bcfg)

	bcfg.AltairForkEpoch = math.MaxUint64
	bcfg.BellatrixForkEpoch = math.MaxUint64
	bcfg.CapellaForkEpoch = math.MaxUint64
	bcfg.DenebForkEpoch = math.MaxUint64
	bcfg.InitializeForkSchedule()

	sentinel1, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  &bcfg,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{})
	require.NoError(t, err)
	defer sentinel1.Stop()

	require.NoError(t, sentinel1.Start())
	h := sentinel1.host

	sentinel2, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  &bcfg,
		IpAddr:        listenAddrHost,
		Port:          7077,
		EnableBlocks:  true,
		TCPPort:       9123,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{})
	require.NoError(t, err)
	defer sentinel2.Stop()

	require.NoError(t, sentinel2.Start())
	h2 := sentinel2.host

	sub1, err := sentinel1.SubscribeGossip(BeaconBlockSsz, time.Unix(0, math.MaxInt64))
	require.NoError(t, err)
	defer sub1.Close()

	sub1.Listen()

	sub2, err := sentinel2.SubscribeGossip(BeaconBlockSsz, time.Unix(0, math.MaxInt64))
	require.NoError(t, err)
	defer sub2.Close()
	sub2.Listen()
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
	previousTopic := ""

	ans := <-ch
	require.Equal(t, ans.Data, msg)
	previousTopic = ans.TopicName

	bcfg.AltairForkEpoch = clparams.MainnetBeaconConfig.AltairForkEpoch
	bcfg.InitializeForkSchedule()
	time.Sleep(5 * time.Second)

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
