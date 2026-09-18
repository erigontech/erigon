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

package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"net"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

func MockPrivateKey(dec int64) *ecdsa.PrivateKey {
	pKey := new(ecdsa.PrivateKey)
	pKey.D = big.NewInt(dec)
	return pKey
}

func TestConvertToCryptoPrivkey(t *testing.T) {
	testCases := []struct {
		dec      int64
		expected string
	}{
		{
			dec:      1234567890123456,
			expected: "000000000000000000000000000000000000000000000000000462d53c8abac0",
		},
		{
			dec:      456723645272495,
			expected: "00000000000000000000000000000000000000000000000000019f6342a311af",
		},
		{
			dec:      238762543819574,
			expected: "0000000000000000000000000000000000000000000000000000d9273c9c2b36",
		},
	}

	for _, testCase := range testCases {
		pKey := MockPrivateKey(testCase.dec)

		cryptoPKey, err := convertToCryptoPrivkey(pKey)
		require.NoError(t, err)

		raw, err := cryptoPKey.Raw()
		require.NoError(t, err)

		rawString := fmt.Sprintf("%x", raw)
		require.EqualValues(t, testCase.expected, rawString)
	}
}

func TestMultiAddressBuilder(t *testing.T) {
	testCases := []struct {
		ipAddr      string
		port        uint
		expected    string
		shouldError bool
	}{
		{
			ipAddr:      "192.158.1.38",
			port:        80,
			expected:    "/ip4/192.158.1.38/tcp/80",
			shouldError: false,
		},
		{
			ipAddr:      "192.158..1.38",
			port:        80,
			expected:    "",
			shouldError: true,
		},
		{
			ipAddr:      "192.15.38",
			port:        45,
			expected:    "",
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		multiAddr, err := multiAddressBuilder(testCase.ipAddr, testCase.port)
		if testCase.shouldError {
			require.Error(t, err)
			require.Nil(t, multiAddr)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, testCase.expected, multiAddr.String())
	}
}

func TestBuildOptionsListenOnTCPAndQUIC(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	options, err := buildOptions(&P2PConfig{
		IpAddr:   "127.0.0.1",
		TCPPort:  0,
		QUICPort: 0,
	}, key)
	require.NoError(t, err)
	host, err := libp2p.New(options...)
	require.NoError(t, err)
	defer host.Close()

	var hasTCP, hasQUIC bool
	for _, addr := range host.Network().ListenAddresses() {
		if _, err := addr.ValueForProtocol(multiaddr.P_TCP); err == nil {
			hasTCP = true
		}
		if _, err := addr.ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
			hasQUIC = true
		}
	}
	require.True(t, hasTCP)
	require.True(t, hasQUIC)
}

func TestHostsConnectOverQUIC(t *testing.T) {
	serverKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	serverOptions, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, serverKey)
	require.NoError(t, err)
	server, err := libp2p.New(serverOptions...)
	require.NoError(t, err)
	defer server.Close()

	clientKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	clientOptions, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, clientKey)
	require.NoError(t, err)
	client, err := libp2p.New(clientOptions...)
	require.NoError(t, err)
	defer client.Close()

	var quicAddr multiaddr.Multiaddr
	for _, addr := range server.Addrs() {
		if _, err := addr.ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
			quicAddr = addr
			break
		}
	}
	require.NotNil(t, quicAddr)
	require.NoError(t, client.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{quicAddr}}))

	connections := client.Network().ConnsToPeer(server.ID())
	require.Len(t, connections, 1)
	_, err = connections[0].RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1)
	require.NoError(t, err)
}

func TestBuildOptionsAdvertiseExternalTCPAndQUICAddresses(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	cfg := &P2PConfig{
		IpAddr:     "127.0.0.1",
		ExternalIP: net.ParseIP("192.0.2.1"),
	}
	options, err := buildOptions(cfg, key)
	require.NoError(t, err)
	host, err := libp2p.New(options...)
	require.NoError(t, err)
	defer host.Close()

	cfg.TCPPort = hostTCPPort(host)
	cfg.QUICPort = hostQUICPort(host)
	var hasTCP, hasQUIC bool
	for _, addr := range host.Addrs() {
		ip, err := addr.ValueForProtocol(multiaddr.P_IP4)
		if err != nil || ip != "192.0.2.1" {
			continue
		}
		if _, err := addr.ValueForProtocol(multiaddr.P_TCP); err == nil {
			hasTCP = true
		}
		if _, err := addr.ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
			hasQUIC = true
		}
	}
	require.True(t, hasTCP)
	require.True(t, hasQUIC)
}
