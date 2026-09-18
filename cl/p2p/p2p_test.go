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
	"fmt"
	"net"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestHostTCPPortReturnsBoundPort(t *testing.T) {
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer host.Close()

	port := hostTCPPort(host)
	require.NotZero(t, port)

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	conn.Close()
}

func TestHostQUICPortReturnsBoundPort(t *testing.T) {
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	require.NoError(t, err)
	defer host.Close()

	require.NotZero(t, hostQUICPort(host))
}

func TestNewP2PManagerRejectsSharedDiscoveryAndQUICPort(t *testing.T) {
	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	cfg := &P2PConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        "127.0.0.1",
		Port:          9000,
		TCPPort:       9000,
		QUICPort:      9000,
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)

	_, err = NewP2Pmanager(t.Context(), cfg, log.Root(), clock)
	require.EqualError(t, err, "discovery and QUIC ports must differ: 9000")
}

func TestDiscoveryAndQUICPortConflict(t *testing.T) {
	tests := []struct {
		name     string
		cfg      P2PConfig
		conflict bool
	}{
		{name: "same address", cfg: P2PConfig{IpAddr: "127.0.0.1", Port: 9000, QUICPort: 9000}, conflict: true},
		{name: "explicit same address", cfg: P2PConfig{IpAddr: "127.0.0.1", LocalIP: "127.0.0.1", Port: 9000, QUICPort: 9000}, conflict: true},
		{name: "different addresses", cfg: P2PConfig{IpAddr: "127.0.0.1", LocalIP: "127.0.0.2", Port: 9000, QUICPort: 9000}},
		{name: "discovery wildcard", cfg: P2PConfig{IpAddr: "0.0.0.0", LocalIP: "127.0.0.1", Port: 9000, QUICPort: 9000}, conflict: true},
		{name: "quic wildcard", cfg: P2PConfig{IpAddr: "127.0.0.1", LocalIP: "0.0.0.0", Port: 9000, QUICPort: 9000}, conflict: true},
		{name: "different families", cfg: P2PConfig{IpAddr: "127.0.0.1", LocalIP: "::1", Port: 9000, QUICPort: 9000}},
		{name: "ephemeral discovery", cfg: P2PConfig{IpAddr: "127.0.0.1", Port: 0, QUICPort: 0}},
		{name: "different ports", cfg: P2PConfig{IpAddr: "127.0.0.1", Port: 9000, QUICPort: 9001}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.conflict, discoveryAndQUICPortConflict(&tt.cfg))
		})
	}
}

func TestNewP2PManagerAllowsSharedDiscoveryAndTCPPort(t *testing.T) {
	tcpProbe, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	discoveryPort := tcpProbe.Addr().(*net.TCPAddr).Port
	discoveryProbe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: discoveryPort})
	require.NoError(t, err)
	quicProbe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	quicPort := quicProbe.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, quicProbe.Close())
	require.NoError(t, discoveryProbe.Close())
	require.NoError(t, tcpProbe.Close())

	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	networkConfigCopy := *networkConfig
	networkConfigCopy.BootNodes = nil
	cfg := &P2PConfig{
		NetworkConfig: &networkConfigCopy,
		BeaconConfig:  beaconConfig,
		IpAddr:        "127.0.0.1",
		Port:          discoveryPort,
		TCPPort:       uint(discoveryPort),
		QUICPort:      uint(quicPort),
		TmpDir:        t.TempDir(),
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)
	ctx, cancel := context.WithCancel(t.Context())

	manager, err := NewP2Pmanager(ctx, cfg, log.Root(), clock)
	require.NoError(t, err)
	listener := manager.UDPv5Listener()
	localNodeDB := listener.LocalNode().Database()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, manager.Host().Close())
		listener.Close()
		localNodeDB.Close()
	})
}

func TestNewP2PManagerClosesHostWhenDiscoveryStartupFails(t *testing.T) {
	blockedDiscovery, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	defer blockedDiscovery.Close()

	networkConfig, beaconConfig, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)
	networkConfigCopy := *networkConfig
	networkConfigCopy.BootNodes = nil
	cfg := &P2PConfig{
		NetworkConfig: &networkConfigCopy,
		BeaconConfig:  beaconConfig,
		IpAddr:        "127.0.0.1",
		Port:          blockedDiscovery.LocalAddr().(*net.UDPAddr).Port,
		TmpDir:        t.TempDir(),
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, beaconConfig)

	_, err = NewP2Pmanager(context.Background(), cfg, log.Root(), clock)
	require.Error(t, err)
	require.NotZero(t, cfg.TCPPort)
	require.NotZero(t, cfg.QUICPort)

	var listenConfig net.ListenConfig
	tcpListener, err := listenConfig.Listen(t.Context(), "tcp4", fmt.Sprintf("127.0.0.1:%d", cfg.TCPPort))
	require.NoError(t, err)
	quicListener, err := listenConfig.ListenPacket(t.Context(), "udp4", fmt.Sprintf("127.0.0.1:%d", cfg.QUICPort))
	require.NoError(t, err)
	require.NoError(t, quicListener.Close())
	require.NoError(t, tcpListener.Close())
}
