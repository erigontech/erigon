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

// TestInterceptSecuredAllowsFirstConnectionRegardlessOfDirection covers a peer with no
// existing connections: direction alone must never cause a rejection, for either
// transport, since there is nothing yet to converge with.
func TestInterceptSecuredAllowsFirstConnectionRegardlessOfDirection(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	opts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, key)
	require.NoError(t, err)
	host, err := libp2p.New(opts...)
	require.NoError(t, err)
	defer host.Close()

	g := &Gater{}
	g.SetHost(host)
	tcpAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	require.NoError(t, err)
	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.True(t, g.InterceptSecured(dir, peer.ID("peer"), stubConnMultiaddrs{remote: tcpAddr}))
	}
}

// TestInterceptSecuredAllowsQUICRegardlessOfDirection covers the same "nothing to
// converge with yet" case as above, specifically for the preferred transport.
func TestInterceptSecuredAllowsQUICRegardlessOfDirection(t *testing.T) {
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
	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.True(t, g.InterceptSecured(dir, peer.ID("peer"), stubConnMultiaddrs{remote: quicAddr}))
	}
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

// TestInterceptSecuredClosesStaleInboundTCPWhenOurOwnOutboundQUICArrivesSecond covers
// the arrival-order combination the prior fix missed: it only applied the dedup logic
// to inbound connections, so a peer's connections were still deduplicated against each
// other, but our own outbound dials were exempt entirely (InterceptSecured returned
// true unconditionally for any non-inbound direction). A peer dialing us over TCP,
// followed by us independently dialing that same peer over QUIC, left both live.
//
// This can't be driven through host.Connect()/Network().DialPeer(), even with
// network.WithForceDirectDial: go-libp2p reuses any existing live connection to a peer
// rather than dialing a second one once Connectedness already reports Connected, so a
// second high-level dial call is a silent no-op here. In production the only way an
// outbound QUIC attempt reaches this gate at all is when it was already in flight
// before the inbound TCP connection registered - an unreproducible timing race, not a
// sequencing a test can drive. Calling the gate directly with a real pre-existing
// connection reproduces that arrival deterministically instead.
func TestInterceptSecuredClosesStaleInboundTCPWhenOurOwnOutboundQUICArrivesSecond(t *testing.T) {
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
	peerOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, peerKey)
	require.NoError(t, err)
	peerHost, err := libp2p.New(peerOpts...)
	require.NoError(t, err)
	defer peerHost.Close()

	serverTCPAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_TCP)

	// The peer dials the server over TCP: inbound at the server, a real, fully
	// registered connection, to prove the close side effect works against actual
	// swarm state rather than a stub.
	require.NoError(t, peerHost.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverTCPAddr}}))
	require.Len(t, server.Network().ConnsToPeer(peerHost.ID()), 1)

	quicAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1")
	require.NoError(t, err)
	require.True(t, gater.InterceptSecured(network.DirOutbound, peerHost.ID(), stubConnMultiaddrs{remote: quicAddr}),
		"the outbound QUIC leg itself must still be admitted")

	require.Empty(t, server.Network().ConnsToPeer(peerHost.ID()),
		"the stale inbound TCP connection must be closed as a side effect of admitting the outbound QUIC leg")
}

// TestInterceptSecuredRejectsInboundTCPWhenOurOwnOutboundQUICAlreadyExists is the
// mirror arrival order: we dial out over QUIC first, then the same peer separately
// dials us over TCP. The inbound TCP must be rejected, not just tolerated alongside it.
// As above, the second leg is simulated directly rather than dialed, since a real
// second dial from the peer's side would race the same Connectedness short-circuit.
func TestInterceptSecuredRejectsInboundTCPWhenOurOwnOutboundQUICAlreadyExists(t *testing.T) {
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
	peerOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, peerKey)
	require.NoError(t, err)
	peerHost, err := libp2p.New(peerOpts...)
	require.NoError(t, err)
	defer peerHost.Close()

	peerQUICAddr := firstMultiaddrWithProtocol(t, peerHost.Addrs(), multiaddr.P_QUIC_V1)

	// The server dials the peer over QUIC: outbound from the server's own gater, a
	// real, fully registered connection.
	require.NoError(t, server.Connect(t.Context(), peer.AddrInfo{ID: peerHost.ID(), Addrs: []multiaddr.Multiaddr{peerQUICAddr}}))
	require.Len(t, server.Network().ConnsToPeer(peerHost.ID()), 1)

	tcpAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	require.NoError(t, err)
	require.False(t, gater.InterceptSecured(network.DirInbound, peerHost.ID(), stubConnMultiaddrs{remote: tcpAddr}),
		"the redundant inbound TCP leg must be rejected")

	conns := server.Network().ConnsToPeer(peerHost.ID())
	require.Len(t, conns, 1, "the pre-existing outbound QUIC connection must be untouched")
	_, err = conns[0].RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1)
	require.NoError(t, err)
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
