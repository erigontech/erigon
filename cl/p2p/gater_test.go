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

// TestInterceptSecuredAllowsFirstConnectionToAPeer covers a peer with no existing
// connections: neither direction nor transport should ever cause a rejection, since
// there is nothing yet to collide with.
func TestInterceptSecuredAllowsFirstConnectionToAPeer(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	opts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, key)
	require.NoError(t, err)
	host, err := libp2p.New(opts...)
	require.NoError(t, err)
	defer host.Close()

	g := &Gater{}
	g.SetHost(host)
	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.True(t, g.InterceptSecured(dir, peer.ID("peer"), nil))
	}
}

// TestInterceptSecuredRejectsSecondConnectionRegardlessOfTransport reproduces the
// dual-transport connection race (two nodes discover each other via discv5 and dial one
// another around the same time) across every transport combination, and pins the
// safety requirement from review: whichever connection registers first survives, and it
// is never closed to make room for a later one of a different transport.
func TestInterceptSecuredRejectsSecondConnectionRegardlessOfTransport(t *testing.T) {
	tests := []struct {
		name       string
		firstQUIC  bool
		secondQUIC bool
	}{
		{"TCP first, QUIC second", false, true},
		{"QUIC first, TCP second", true, false},
		{"TCP first, TCP second", false, false},
		{"QUIC first, QUIC second", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			protocolFor := func(wantQUIC bool) int {
				if wantQUIC {
					return multiaddr.P_QUIC_V1
				}
				return multiaddr.P_TCP
			}

			firstOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1", DisableQUIC: !tt.firstQUIC}, peerKey)
			require.NoError(t, err)
			firstClient, err := libp2p.New(firstOpts...)
			require.NoError(t, err)
			defer firstClient.Close()

			firstAddr := firstMultiaddrWithProtocol(t, server.Addrs(), protocolFor(tt.firstQUIC))
			require.NoError(t, firstClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{firstAddr}}))
			require.Len(t, server.Network().ConnsToPeer(firstClient.ID()), 1)

			// A second dial from the same client would be a silent no-op once
			// go-libp2p already reports it Connected to the peer, so the second leg
			// uses a separate client host sharing the same peer identity.
			secondOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1", DisableQUIC: !tt.secondQUIC}, peerKey)
			require.NoError(t, err)
			secondClient, err := libp2p.New(secondOpts...)
			require.NoError(t, err)
			defer secondClient.Close()

			secondAddr := firstMultiaddrWithProtocol(t, server.Addrs(), protocolFor(tt.secondQUIC))
			_ = secondClient.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{secondAddr}})

			require.Eventually(t, func() bool {
				return len(secondClient.Network().ConnsToPeer(server.ID())) == 0
			}, time.Second, 10*time.Millisecond, "the second connection must be rejected")

			conns := server.Network().ConnsToPeer(firstClient.ID())
			require.Len(t, conns, 1, "the first connection must never be closed to make room for a later one")
			_, err = conns[0].RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1)
			if tt.firstQUIC {
				require.NoError(t, err, "the surviving connection must be the one that registered first")
			} else {
				require.Error(t, err, "the surviving connection must be the one that registered first")
			}
		})
	}
}

// TestInterceptSecuredRejectsRegardlessOfDirection pins that the rejection is based
// purely on whether a connection already exists for the peer, not on which side dialed.
func TestInterceptSecuredRejectsRegardlessOfDirection(t *testing.T) {
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

	serverQUICAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_QUIC_V1)
	require.NoError(t, peerHost.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverQUICAddr}}))
	require.Len(t, server.Network().ConnsToPeer(peerHost.ID()), 1)

	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.False(t, gater.InterceptSecured(dir, peerHost.ID(), nil))
	}
	require.Len(t, server.Network().ConnsToPeer(peerHost.ID()), 1, "the existing connection must be untouched")
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
