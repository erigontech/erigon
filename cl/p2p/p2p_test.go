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
