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
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
)

func remoteAddr(t *testing.T, s string) multiaddr.Multiaddr {
	t.Helper()
	addr, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return addr
}

func TestInterceptAcceptRateLimitsRepeatedAttemptsFromOneIP(t *testing.T) {
	g, err := NewGater(&P2PConfig{IpAddr: "127.0.0.1"}, log.Root())
	require.NoError(t, err)
	// Small, deterministic limits for the test instead of the production
	// defaults, which would need hundreds of iterations to exhaust.
	g.rateLimiter = newIPRateLimiter(ipRateLimiterConfig{rate: 0, burst: 2, maxTrackedIPs: 10, summaryInterval: time.Minute}, log.Root(), time.Now)

	addr := stubConnMultiaddrs{remote: remoteAddr(t, "/ip4/203.0.113.5/tcp/4001")}
	require.True(t, g.InterceptAccept(addr))
	require.True(t, g.InterceptAccept(addr))
	require.False(t, g.InterceptAccept(addr), "third attempt within the burst window must be rate limited")
}

func TestInterceptAcceptCapsOccupancyFromOneIP(t *testing.T) {
	g, err := NewGater(&P2PConfig{IpAddr: "127.0.0.1", MaxPeerCount: 0}, log.Root())
	require.NoError(t, err)
	fixture := &connsFixtureHost{}
	g.poolLimiter = &peerPoolLimiter{
		maxPerIP: 1, maxPerSubscriberBlock: 100, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	g.poolLimiter.setHost(fixture)

	addr := stubConnMultiaddrs{remote: remoteAddr(t, "/ip4/203.0.113.5/tcp/4001")}
	require.True(t, g.InterceptAccept(addr), "no existing connections from this IP yet")

	fixture.conns = []string{"203.0.113.5"}
	require.False(t, g.InterceptAccept(addr), "already at the per-IP occupancy cap")
}

func TestInterceptAcceptStillAppliesThePrivateAddressFilter(t *testing.T) {
	g, err := NewGater(&P2PConfig{IpAddr: "127.0.0.1", LocalDiscovery: false}, log.Root())
	require.NoError(t, err)

	addr := stubConnMultiaddrs{remote: remoteAddr(t, "/ip4/10.0.0.5/tcp/4001")}
	require.False(t, g.InterceptAccept(addr), "private-range filtering must be unaffected by the new checks")
}

// TestHostConnsRemoteIPsOnlyReportsInboundConnections reproduces review finding
// "Aggregate deltas incorrectly retire reservations and undercount occupancy":
// peerPoolLimiter.allow is only ever invoked for inbound attempts (via
// InterceptAccept), so it only ever creates reservations for inbound connections. If
// the live view it reconciles against also counted outbound connections - ones we
// dialed ourselves, which never went through this limiter at all - an outbound
// connection to some IP could be misread as "one of this IP's pending reservations
// just matured" and retire a still-genuinely-pending inbound one, undercounting
// occupancy. hostConns must only report inbound connections to the limiter.
func TestHostConnsRemoteIPsOnlyReportsInboundConnections(t *testing.T) {
	serverKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	serverOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, serverKey)
	require.NoError(t, err)
	server, err := libp2p.New(serverOpts...)
	require.NoError(t, err)
	defer server.Close()

	inboundPeerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	inboundPeerOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, inboundPeerKey)
	require.NoError(t, err)
	inboundPeer, err := libp2p.New(inboundPeerOpts...)
	require.NoError(t, err)
	defer inboundPeer.Close()

	outboundPeerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	outboundPeerOpts, err := buildOptions(&P2PConfig{IpAddr: "127.0.0.1"}, outboundPeerKey)
	require.NoError(t, err)
	outboundPeer, err := libp2p.New(outboundPeerOpts...)
	require.NoError(t, err)
	defer outboundPeer.Close()

	serverAddr := firstMultiaddrWithProtocol(t, server.Addrs(), multiaddr.P_TCP)
	require.NoError(t, inboundPeer.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{serverAddr}}))

	outboundPeerAddr := firstMultiaddrWithProtocol(t, outboundPeer.Addrs(), multiaddr.P_TCP)
	require.NoError(t, server.Connect(t.Context(), peer.AddrInfo{ID: outboundPeer.ID(), Addrs: []multiaddr.Multiaddr{outboundPeerAddr}}))

	require.Eventually(t, func() bool {
		return len(server.Network().Conns()) == 2
	}, time.Second, 10*time.Millisecond, "both the inbound and outbound connections should be established")

	ips := (hostConns{server}).remoteIPs()
	require.Len(t, ips, 1, "only the inbound connection must be reported; the outbound one must be excluded")
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
