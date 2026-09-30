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

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

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

type stubConnMultiaddrs struct {
	local, remote multiaddr.Multiaddr
}

func (c stubConnMultiaddrs) LocalMultiaddr() multiaddr.Multiaddr  { return c.local }
func (c stubConnMultiaddrs) RemoteMultiaddr() multiaddr.Multiaddr { return c.remote }
