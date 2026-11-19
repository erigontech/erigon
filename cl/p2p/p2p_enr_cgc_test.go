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

package p2p

import (
	"context"
	"testing"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enr"
)

func newENRTestP2P(t *testing.T, subscribeAll bool) (*p2pManager, *discover.UDPv5) {
	t.Helper()
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	cfg := &P2PConfig{
		NetworkConfig:      networkConfig,
		BeaconConfig:       beaconConfig,
		IpAddr:             "127.0.0.1",
		Port:               0,
		TCPPort:            4001,
		TmpDir:             t.TempDir(),
		SubscribeAllTopics: subscribeAll,
	}
	listener, err := NewUDPv5Listener(t.Context(), cfg, discover.Config{PrivateKey: privateKey}, log.Root())
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
		listener.LocalNode().Database().Close()
	})
	return &p2pManager{
		cfg:      cfg,
		udpv5:    listener,
		ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig),
	}, listener
}

func TestSetupENRAttestationSubnets(t *testing.T) {
	tests := []struct {
		name         string
		subscribeAll bool
		wantAttnets  uint64
		wantSyncnets byte
	}{
		{name: "persistent subnets are set later", wantAttnets: 0, wantSyncnets: 0x00},
		{name: "subscribe all", subscribeAll: true, wantAttnets: 64, wantSyncnets: 0x0f},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, listener := newENRTestP2P(t, tt.subscribeAll)
			require.NoError(t, p.setupENR())

			var attnets bitfield.Bitvector64
			require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(p.cfg.NetworkConfig.AttSubnetKey, &attnets)))
			require.Equal(t, tt.wantAttnets, attnets.Count())
			var syncnets bitfield.Bitvector4
			require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(p.cfg.NetworkConfig.SyncCommsSubnetKey, &syncnets)))
			require.Equal(t, []byte{tt.wantSyncnets}, syncnets.Bytes())
		})
	}
}

func TestSetENRAttSubnets(t *testing.T) {
	p, listener := newENRTestP2P(t, false)
	require.NoError(t, p.setupENR())

	p.SetENRAttSubnets([]uint64{3, 8})
	var attnets bitfield.Bitvector64
	require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(p.cfg.NetworkConfig.AttSubnetKey, &attnets)))
	require.Equal(t, []byte{0x08, 0x01, 0, 0, 0, 0, 0, 0}, attnets.Bytes())
	seq := listener.LocalNode().Node().Seq()

	p.SetENRAttSubnets([]uint64{3, 8})
	require.Equal(t, seq, listener.LocalNode().Node().Seq())

	p.SetENRAttSubnets([]uint64{1})
	var updated bitfield.Bitvector64
	require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(p.cfg.NetworkConfig.AttSubnetKey, &updated)))
	require.Equal(t, []byte{0x02, 0, 0, 0, 0, 0, 0, 0}, updated.Bytes())
	require.Greater(t, listener.LocalNode().Node().Seq(), seq)
}

// setupENR once wrote the cgc entry as an empty byte slice, which is the canonical
// encoding of zero, so the node advertised custody of no groups. This drives the real
// setup path rather than the encoder, so reverting that line fails here.
func TestSetupENRAdvertisesTheCustodyRequirement(t *testing.T) {
	netCfg, beaconCfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := &P2PConfig{
		NetworkConfig: netCfg,
		BeaconConfig:  beaconCfg,
		IpAddr:        "127.0.0.1",
		Port:          0,
		TCPPort:       4001,
		TmpDir:        t.TempDir(),
	}
	listener, err := NewUDPv5Listener(context.Background(), cfg, discover.Config{PrivateKey: privKey}, log.Root())
	require.NoError(t, err)
	defer listener.LocalNode().Database().Close()
	defer listener.Close()

	p := &p2pManager{
		cfg:      cfg,
		udpv5:    listener,
		ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconCfg),
	}
	require.NoError(t, p.setupENR())

	var got []byte
	require.NoError(t, listener.LocalNode().Node().Load(enr.WithEntry(cfg.NetworkConfig.CgcKey, &got)))

	// fulu p2p-interface.md: big endian, no leading zero bytes. CUSTODY_REQUIREMENT is
	// 4 on every network, so one byte - not a zero-padded uint64.
	require.NotEmpty(t, got, "cgc advertised as empty, i.e. custody of no groups")
	require.Equal(t, []byte{byte(beaconCfg.CustodyRequirement)}, got)
}

func TestSetupENRAdvertisesQUICPort(t *testing.T) {
	netCfg, beaconCfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := &P2PConfig{
		NetworkConfig: netCfg,
		BeaconConfig:  beaconCfg,
		IpAddr:        "127.0.0.1",
		Port:          0,
		TCPPort:       4001,
		QUICPort:      4002,
		TmpDir:        t.TempDir(),
	}
	listener, err := NewUDPv5Listener(context.Background(), cfg, discover.Config{PrivateKey: privKey}, log.Root())
	require.NoError(t, err)
	defer listener.LocalNode().Database().Close()
	defer listener.Close()

	p := &p2pManager{
		cfg:      cfg,
		udpv5:    listener,
		ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconCfg),
	}
	require.NoError(t, p.setupENR())

	var port enr.QUIC
	require.NoError(t, listener.LocalNode().Node().Load(&port))
	require.Equal(t, enr.QUIC(4002), port)
}

func TestSetupENRAdvertisesIPv6QUICPort(t *testing.T) {
	netCfg, beaconCfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := &P2PConfig{
		NetworkConfig: netCfg,
		BeaconConfig:  beaconCfg,
		IpAddr:        "::1",
		Port:          0,
		TCPPort:       4001,
		QUICPort:      4002,
		TmpDir:        t.TempDir(),
	}
	listener, err := NewUDPv5Listener(context.Background(), cfg, discover.Config{PrivateKey: privKey}, log.Root())
	require.NoError(t, err)
	defer listener.LocalNode().Database().Close()
	defer listener.Close()

	p := &p2pManager{
		cfg:      cfg,
		udpv5:    listener,
		ethClock: eth_clock.NewEthereumClock(0, common.Hash{}, beaconCfg),
	}
	require.NoError(t, p.setupENR())

	var port enr.QUIC6
	require.NoError(t, listener.LocalNode().Node().Load(&port))
	require.Equal(t, enr.QUIC6(4002), port)
}
