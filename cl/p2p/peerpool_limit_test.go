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
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewPeerPoolLimiterScalesWithMaxPeerCount(t *testing.T) {
	tests := []struct {
		name                      string
		maxPeerCount              uint64
		wantMaxPerIP              int
		wantMaxPerSubscriberBlock int
		wantMaxPerASBlock         int
	}{
		// A tiny configured pool must still get the floors, not zero or a
		// fraction that rounds down to nothing.
		{"tiny pool floors at the minimums", 10, 2, 4, 7},
		{"default pool", 128, 2, 12, 96},
		{"large pool scales up", 2000, 40, 200, 1500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newPeerPoolLimiter(tt.maxPeerCount)
			require.Equal(t, tt.wantMaxPerIP, l.maxPerIP)
			require.Equal(t, tt.wantMaxPerSubscriberBlock, l.maxPerSubscriberBlock)
			require.Equal(t, tt.wantMaxPerASBlock, l.maxPerASBlock)
			require.True(t, l.maxPerSubscriberBlock >= l.maxPerIP, "a subscriber block must never be capped tighter than a single IP within it")
			require.True(t, l.maxPerASBlock >= l.maxPerSubscriberBlock, "an AS block must never be capped tighter than a subscriber block within it")
			require.LessOrEqual(t, float64(l.maxPerASBlock), float64(tt.maxPeerCount)*peerPoolLimiterASBlockPoolFraction+1,
				"an AS block's cap must not exceed its configured share of the pool")
		})
	}
}

func TestPeerPoolLimiterAllowsWithoutHostSet(t *testing.T) {
	l := newPeerPoolLimiter(128)
	require.True(t, l.allow(net.ParseIP("203.0.113.5")), "must fail open before setHost is called")
}

func TestPeerPoolLimiterExemptsLoopback(t *testing.T) {
	l := newPeerPoolLimiter(128)
	l.setHost(&connsFixtureHost{})
	require.True(t, l.allow(net.ParseIP("127.0.0.1")))
}

func TestPeerPoolLimiterCapsConnectionsFromSameIP(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 2, maxPerSubscriberBlock: 100, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	ip := "203.0.113.5"
	fixture.conns = nil
	require.True(t, l.allow(net.ParseIP(ip)), "0 existing connections, under the per-IP cap of 2")

	fixture.conns = []string{ip}
	require.True(t, l.allow(net.ParseIP(ip)), "1 existing connection, still under the cap")

	fixture.conns = []string{ip, ip}
	require.False(t, l.allow(net.ParseIP(ip)), "2 existing connections already at the per-IP cap")
}

// TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash24 is the user's own scenario:
// 130.0.0.0/24 filling the whole pool is possible in theory but has a high chance of
// being an attack, so the subscriber-block tier must stop it well before that point.
func TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash24(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 100, maxPerSubscriberBlock: 2, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	// Same /24 (130.0.0.0/24), distinct addresses - a per-IP cap alone would
	// miss this, since no individual address repeats.
	fixture.conns = []string{"130.0.0.1", "130.0.0.2"}
	require.False(t, l.allow(net.ParseIP("130.0.0.3")), "third distinct address within the same /24 must hit the subscriber-block cap")

	// A different /24 within the same /16 must not be affected by the /24 tier.
	require.True(t, l.allow(net.ParseIP("130.0.1.1")))
}

// TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash16At75PercentOfPool is the
// user's other scenario: 130.0.0.0/16 must never occupy more than 75% of the pool,
// even though it is spread across many distinct /24s so the subscriber-block tier
// never trips on its own.
func TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash16At75PercentOfPool(t *testing.T) {
	const maxPeerCount = 100
	l := newPeerPoolLimiter(maxPeerCount)
	require.Equal(t, 75, l.maxPerASBlock, "sanity check on the 75%-of-pool AS-block cap for a 100-peer pool")

	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	// 75 connections, one per distinct /24 within 130.0.0.0/16, so no single
	// /24 ever holds more than 1 (well under maxPerSubscriberBlock) - only the
	// /16 AS-block tier can be the one rejecting the 76th.
	conns := make([]string, 0, 75)
	for i := range 75 {
		conns = append(conns, fmt.Sprintf("130.0.%d.1", i))
	}
	fixture.conns = conns[:74]
	require.True(t, l.allow(net.ParseIP("130.0.74.1")), "74 existing peers from the /16 is still under the 75-peer cap")

	fixture.conns = conns
	require.False(t, l.allow(net.ParseIP("130.0.75.1")), "75 existing peers from the /16 already occupy the 75%-of-pool cap")

	require.True(t, l.allow(net.ParseIP("198.51.100.9")), "an address in an unrelated /16 must be unaffected")
}

func TestPeerPoolLimiterGroupsIPv6BySlash56PerRFC6177(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 100, maxPerSubscriberBlock: 2, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	// RFC 6177 recommends /48-/64 (commonly /56) as a single end-site
	// allocation: these three addresses are within the same /56 and must be
	// treated as one subscriber even though no individual /128 repeats.
	fixture.conns = []string{"2001:db8:1234:5600::1", "2001:db8:1234:56ff::2"}
	require.False(t, l.allow(net.ParseIP("2001:db8:1234:5600::3")), "third distinct address within the same /56 must hit the subscriber-block cap")

	// A different /56 under the same /48 must not be affected by that tier.
	require.True(t, l.allow(net.ParseIP("2001:db8:1234:5700::1")))
}

func TestPeerPoolLimiterCapsConnectionsFromSameIPv6Slash32(t *testing.T) {
	// /32 is the typical RIR-to-ISP IPv6 allocation size, the v6 analogue of a
	// v4 /16: a loose backstop so a whole ISP allocation still can't take over
	// the pool, even though it legitimately holds many distinct subscribers.
	l := &peerPoolLimiter{
		maxPerIP: 100, maxPerSubscriberBlock: 100, maxPerASBlock: 2,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	// Same /32 (2001:db8::/32), different /56 subscriber blocks within it.
	fixture.conns = []string{"2001:db8:1::1", "2001:db8:2::1"}
	require.False(t, l.allow(net.ParseIP("2001:db8:3::1")), "third distinct subscriber block within the same /32 must hit the AS-block cap")

	require.True(t, l.allow(net.ParseIP("2001:db9::1")), "an address in an unrelated /32 must be unaffected")
}

// TestPeerPoolLimiterEnforcesCapUnderConcurrentAccepts reproduces the TOCTOU race
// flagged in review on PR #24449: InterceptAccept runs before the connection it is
// deciding on is registered in the live host's connection list. Deliberately never
// update the fixture's conns, so every concurrent allow() call sees the same
// "nothing live yet" snapshot - the cap can only be enforced if the limiter tracks
// admissions itself, not just by querying the live host.
func TestPeerPoolLimiterEnforcesCapUnderConcurrentAccepts(t *testing.T) {
	const maxPerIP = 3
	l := &peerPoolLimiter{
		maxPerIP: maxPerIP, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	l.setHost(&connsFixtureHost{})

	ip := net.ParseIP("203.0.113.5")
	const attempts = 50
	var wg sync.WaitGroup
	var allowed atomic.Int32
	wg.Add(attempts)
	for range attempts {
		go func() {
			defer wg.Done()
			if l.allow(ip) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	require.LessOrEqual(t, int(allowed.Load()), maxPerIP,
		"concurrent accepts from one IP must never exceed the per-IP cap, even though none of them are yet reflected in the live connection list")
}

func TestPeerPoolLimiterReservationsExpireAndFreeCapacity(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	l := &peerPoolLimiter{
		maxPerIP: 1, maxPerSubscriberBlock: 100, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Second, now: clock.Now,
	}
	l.setHost(&connsFixtureHost{})

	ip := net.ParseIP("203.0.113.5")
	require.True(t, l.allow(ip))
	require.False(t, l.allow(ip), "the reservation from the first admission still occupies the only slot")

	clock.advance(2 * time.Second)
	require.True(t, l.allow(ip), "an expired reservation must free its slot")
}

func TestPeerPoolLimiterReservationDoesNotDoubleCountOnceLive(t *testing.T) {
	// Same sequence as TestPeerPoolLimiterCapsConnectionsFromSameIP, but explicit
	// about why it still holds under the reservation mechanism: once a reservation's
	// connection becomes visible in the live host, the two must not stack and produce
	// an over-strict effective count of live+reservation.
	l := &peerPoolLimiter{
		maxPerIP: 2, maxPerSubscriberBlock: 100, maxPerASBlock: 100,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	ip := "203.0.113.5"
	require.True(t, l.allow(net.ParseIP(ip)), "first admission reserves a slot")

	fixture.conns = []string{ip}
	require.True(t, l.allow(net.ParseIP(ip)), "the first admission is now live; its reservation must not also count separately")

	fixture.conns = []string{ip, ip}
	require.False(t, l.allow(net.ParseIP(ip)), "two live connections already occupy the cap of 2")
}

// TestDefaultPeerPoolReservationTTLCoversLibp2pHandshakeTimeouts guards against
// review finding "Short reservation expiry allows slow-handshake admission bypass":
// go-libp2p's shared upgrader allows up to 15s to accept a connection plus 60s to
// negotiate it, so a reservation shorter than that could expire - and stop counting
// toward the cap - while a legitimate, still-pending handshake is neither live nor
// reserved, letting a source accumulate more admissions than the configured cap once
// enough slow handshakes land.
func TestDefaultPeerPoolReservationTTLCoversLibp2pHandshakeTimeouts(t *testing.T) {
	require.GreaterOrEqual(t, defaultPeerPoolReservationTTL, 75*time.Second)
}

// TestPeerPoolLimiterReservationCountingNeverExceedsCapWithPreExistingLive reproduces
// review finding "Reservation counting can exceed the configured connection cap":
// with a cap of 3 and two already-live connections, max(live, reserved) let two
// concurrent new reservations both pass (max(2,2)==2), which would put the source at
// 4 connections once both materialized.
func TestPeerPoolLimiterReservationCountingNeverExceedsCapWithPreExistingLive(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 3, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now,
	}
	ip := "203.0.113.5"
	fixture := &connsFixtureHost{conns: []string{ip, ip}} // 2 pre-existing live connections
	l.setHost(fixture)

	require.True(t, l.allow(net.ParseIP(ip)), "1st new admission: 2 live + 0 reserved is under the cap of 3")
	require.False(t, l.allow(net.ParseIP(ip)), "2nd new admission must be rejected: 2 live + 1 pending reservation already equals the cap")
}

// TestPeerPoolLimiterBoundsTotalReservations reproduces review finding "Unbounded
// reservations enable attacker-controlled memory growth": many distinct sources, each
// individually within its own per-key cap, must not be able to grow the reservation
// pool without bound.
func TestPeerPoolLimiterBoundsTotalReservations(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 1000, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now, maxReservations: 3,
	}
	l.setHost(&connsFixtureHost{})

	for i, ipStr := range []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"} {
		require.True(t, l.allow(net.ParseIP(ipStr)), "attempt %d should fit within the reservation bound", i)
	}
	require.False(t, l.allow(net.ParseIP("203.0.113.4")),
		"a 4th distinct source must be rejected once the global reservation bound is reached, not silently grow past it")
	require.LessOrEqual(t, len(l.reservations), 3)
}

// connsFixtureHost is a minimal liveConnsSource stand-in: real libp2p hosts are
// exercised in gater_test.go's integration tests, but the occupancy math itself
// (per-IP / per-block counting and RFC 6177 aggregation) doesn't need a live
// network stack, just a list of remote IPs to count against.
type connsFixtureHost struct {
	conns []string
}

func (f *connsFixtureHost) remoteIPs() []net.IP {
	ips := make([]net.IP, 0, len(f.conns))
	for _, addr := range f.conns {
		ips = append(ips, net.ParseIP(addr))
	}
	return ips
}
