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
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

func TestInterceptSecuredAllowsBeforeHostIsSet(t *testing.T) {
	g := &Gater{}
	require.True(t, g.InterceptSecured(network.DirInbound, peer.ID("peer"), nil), "must fail open before SetHost is called")
}

func TestInterceptSecuredAllowsOutboundRegardlessOfExistingConns(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	opts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, key)
	require.NoError(t, err)
	host, err := libp2p.New(opts...)
	require.NoError(t, err)
	defer host.Close()

	g := &Gater{}
	g.SetHost(host)
	require.True(t, g.InterceptSecured(network.DirOutbound, peer.ID("peer"), nil))
}

func TestInterceptSecuredAllowsInboundQUICRegardlessOfExistingConns(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	opts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, key)
	require.NoError(t, err)
	host, err := libp2p.New(opts...)
	require.NoError(t, err)
	defer host.Close()

	g := &Gater{}
	g.SetHost(host)
	quicAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1")
	require.NoError(t, err)
	require.True(t, g.InterceptSecured(network.DirInbound, peer.ID("peer"), stubConnMultiaddrs{remote: quicAddr}))
}

// TestInterceptSecuredRejectsRedundantInboundTCPWhenPeerAlreadyHasQUIC reproduces the
// dual-transport connection race: two independent nodes discover each other and dial one
// another over different transports around the same time. Without this gate, both
// connections succeed and never converge; the gate makes the server reject the second
// (non-preferred) transport for a peer it is already connected to over QUIC.
func TestInterceptSecuredRejectsRedundantInboundTCPWhenPeerAlreadyHasQUIC(t *testing.T) {
	serverKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	serverOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, serverKey)
	require.NoError(t, err)
	gater, err := NewGater(&P2PConfig{IpAddr: "127.0.0.1"})
	require.NoError(t, err)
	serverOpts = append(serverOpts, libp2p.ConnectionGater(gater))
	server, err := libp2p.New(serverOpts...)
	require.NoError(t, err)
	defer server.Close()
	gater.SetHost(server)

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	serverQUICAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_QUIC_V1)
	serverTCPAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_TCP)

	quicClientOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, peerKey)
	require.NoError(t, err)
	quicClient, err := libp2p.New(quicClientOpts...)
	require.NoError(t, err)
	defer quicClient.Close()

	require.NoError(t, quicClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverQUICAddr}}))
	require.Len(t, server.Network().ConnsToPeer(quicClient.ID()), 1)

	tcpOnlyClientOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1", DisableQUIC: true}, peerKey)
	require.NoError(t, err)
	tcpOnlyClient, err := libp2p.New(tcpOnlyClientOpts...)
	require.NoError(t, err)
	defer tcpOnlyClient.Close()

	// The reject closes the connection on the accepting (server) side; the dialer's
	// Connect() call itself does not reliably surface that as an error (the security
	// handshake completes locally before the remote's gater verdict tears it down), so
	// the invariant to assert is connection state, not Connect()'s return value.
	_ = tcpOnlyClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverTCPAddr}})

	require.Eventually(t, func() bool {
		return len(tcpOnlyClient.Network().ConnsToPeer(server.ID())) == 0
	}, time.Second, 10*time.Millisecond, "the redundant TCP connection must not remain established on the dialer side")

	conns := server.Network().ConnsToPeer(quicClient.ID())
	require.Len(t, conns, 1, "the server must not have accepted a second connection for the same peer")
	_, err = conns[0].RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1)
	require.NoError(t, err, "the surviving connection must be the QUIC one")
}

// TestInterceptSecuredClosesStaleInboundTCPWhenQUICArrivesSecond covers the reverse
// arrival order from TestInterceptSecuredRejectsRedundantInboundTCPWhenPeerAlreadyHasQUIC:
// TCP connects first, then the same peer's QUIC connection arrives. The preferred
// (QUIC) connection must still end up as the sole survivor, not just the non-preferred
// one when it arrives second.
func TestInterceptSecuredClosesStaleInboundTCPWhenQUICArrivesSecond(t *testing.T) {
	serverKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	serverOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, serverKey)
	require.NoError(t, err)
	gater, err := NewGater(&P2PConfig{IpAddr: "127.0.0.1"})
	require.NoError(t, err)
	serverOpts = append(serverOpts, libp2p.ConnectionGater(gater))
	server, err := libp2p.New(serverOpts...)
	require.NoError(t, err)
	defer server.Close()
	gater.SetHost(server)

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	serverQUICAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_QUIC_V1)
	serverTCPAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_TCP)

	tcpOnlyClientOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1", DisableQUIC: true}, peerKey)
	require.NoError(t, err)
	tcpOnlyClient, err := libp2p.New(tcpOnlyClientOpts...)
	require.NoError(t, err)
	defer tcpOnlyClient.Close()

	require.NoError(t, tcpOnlyClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverTCPAddr}}))
	require.Len(t, server.Network().ConnsToPeer(tcpOnlyClient.ID()), 1)

	quicClientOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, peerKey)
	require.NoError(t, err)
	quicClient, err := libp2p.New(quicClientOpts...)
	require.NoError(t, err)
	defer quicClient.Close()

	require.NoError(t, quicClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverQUICAddr}}))

	require.Eventually(t, func() bool {
		conns := server.Network().ConnsToPeer(quicClient.ID())
		if len(conns) != 1 {
			return false
		}
		_, err := conns[0].RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1)
		return err == nil
	}, time.Second, 10*time.Millisecond, "the stale TCP connection must be closed once the preferred QUIC connection is admitted")
}

func firstMultiaddrWithProtocol(t *testing.T, addrs []multiaddr.Multiaddr, protocol int) multiaddr.Multiaddr {
	t.Helper()
	for _, addr := range addrs {
		if _, err := addr.ValueForProtocol(protocol); err == nil {
			return addr
		}
	}
	t.Fatalf("no address with protocol %d found among %v", protocol, addrs)
	return nil
}

type stubConnMultiaddrs struct {
	local, remote multiaddr.Multiaddr
}

func (c stubConnMultiaddrs) LocalMultiaddr() multiaddr.Multiaddr  { return c.local }
func (c stubConnMultiaddrs) RemoteMultiaddr() multiaddr.Multiaddr { return c.remote }
