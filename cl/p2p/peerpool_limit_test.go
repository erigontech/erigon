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
			l := newPeerPoolLimiter(tt.maxPeerCount, nil)
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
	l := newPeerPoolLimiter(128, nil)
	require.True(t, l.allow(net.ParseIP("203.0.113.5")), "must fail open before setHost is called")
}

func TestPeerPoolLimiterExemptsLoopback(t *testing.T) {
	l := newPeerPoolLimiter(128, nil)
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
	l.onConnected(net.ParseIP(ip))
	require.True(t, l.allow(net.ParseIP(ip)), "1 existing connection, still under the cap")

	fixture.conns = []string{ip, ip}
	l.onConnected(net.ParseIP(ip))
	require.False(t, l.allow(net.ParseIP(ip)), "2 existing connections already at the per-IP cap")
}

// TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash24 pins that a single /24 cannot
// fill the whole pool by spreading across distinct addresses within it - a high enough
// concentration from one allocation is suspicious even with no individual address
// repeating, so the subscriber-block tier must stop it well before that point.
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

// TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash16At75PercentOfPool pins that a
// single /16 can never occupy more than 75% of the pool, even when spread across many
// distinct /24s so the subscriber-block tier never trips on its own.
func TestPeerPoolLimiterCapsConnectionsFromSameIPv4Slash16At75PercentOfPool(t *testing.T) {
	const maxPeerCount = 100
	l := newPeerPoolLimiter(maxPeerCount, nil)
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

// TestPeerPoolLimiterEnforcesCapUnderConcurrentAccepts pins the TOCTOU case:
// InterceptAccept runs before the connection it is deciding on is registered in the
// live host's connection list. Deliberately never update the fixture's conns, so every
// concurrent allow() call sees the same "nothing live yet" snapshot - the cap can only
// be enforced if the limiter tracks admissions itself, not just by querying the live
// host.
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
	l.onConnected(net.ParseIP(ip))
	require.True(t, l.allow(net.ParseIP(ip)), "the first admission is now live; its reservation must not also count separately")

	fixture.conns = []string{ip, ip}
	l.onConnected(net.ParseIP(ip))
	require.False(t, l.allow(net.ParseIP(ip)), "two live connections already occupy the cap of 2")
}

// TestDefaultPeerPoolReservationTTLCoversLibp2pHandshakeTimeouts pins that the default
// TTL outlives go-libp2p's own handshake timeouts (shared upgrader: up to 15s to accept
// a connection plus 60s to negotiate it). A shorter TTL would let a reservation expire
// while its handshake is still legitimately pending - neither live nor reserved -
// letting a source accumulate more admissions than the configured cap.
func TestDefaultPeerPoolReservationTTLCoversLibp2pHandshakeTimeouts(t *testing.T) {
	require.GreaterOrEqual(t, defaultPeerPoolReservationTTL, 75*time.Second)
}

// TestPeerPoolLimiterReservationCountingNeverExceedsCapWithPreExistingLive pins that
// occupancy counts live and reserved connections additively, not as max(live,
// reserved): with a cap of 3 and two already-live connections, the max form would let
// two concurrent new reservations both pass (max(2,2)==2), putting the source at 4
// connections once both materialized.
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

// TestPeerPoolLimiterBoundsTotalReservations pins that many distinct sources, each
// individually within its own per-key cap, cannot grow the reservation pool without
// bound.
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

// TestPeerPoolLimiterOnConnectedRetiresExactlyOneReservation pins that confirming one
// connection live only retires one of that IP's pending reservations, leaving any other
// still-genuinely-pending ones for the same IP untouched.
func TestPeerPoolLimiterOnConnectedRetiresExactlyOneReservation(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 1000, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)
	ip := net.ParseIP("203.0.113.1")

	require.True(t, l.allow(ip))
	require.True(t, l.allow(ip))
	require.Len(t, l.reservations, 2, "two concurrent attempts from the same IP should both reserve")

	l.onConnected(ip)
	require.Len(t, l.reservations, 1, "only the one reservation whose connection went live should retire")
}

// TestPeerPoolLimiterLogsEachRejectionAtTraceOnly pins that a cap rejection is visible
// at Trace level, so it isn't completely silent against the live host - previously
// nothing logged a peer-pool rejection at all.
func TestPeerPoolLimiterLogsEachRejectionAtTraceOnly(t *testing.T) {
	logger := &recordingLogger{}
	l := &peerPoolLimiter{
		maxPerIP: 1, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now, logger: logger, summaryInterval: 30 * time.Second,
	}
	fixture := &connsFixtureHost{conns: []string{"203.0.113.1"}}
	l.setHost(fixture)

	for range 5 {
		require.False(t, l.allow(net.ParseIP("203.0.113.1")))
	}

	require.Len(t, logger.traceMsgs, 5, "every individual rejection should be visible at Trace level")
	require.Len(t, logger.warnMsgs, 1, "but only one summary line at Warn level")
}

// TestPeerPoolLimiterLogsPeriodicSummaryNotPerRejection pins that a flood of
// rejections produces one periodic Warn-level summary rather than flooding the log at
// the attacker's own request rate.
func TestPeerPoolLimiterLogsPeriodicSummaryNotPerRejection(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	logger := &recordingLogger{}
	l := &peerPoolLimiter{
		maxPerIP: 1, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: clock.Now, logger: logger, summaryInterval: 30 * time.Second,
	}
	fixture := &connsFixtureHost{conns: []string{"203.0.113.1"}}
	l.setHost(fixture)

	for range 100 {
		require.False(t, l.allow(net.ParseIP("203.0.113.1")))
	}
	require.Len(t, logger.warnMsgs, 1, "100 rejections within one window must produce exactly one summary line")

	clock.advance(31 * time.Second)
	require.False(t, l.allow(net.ParseIP("203.0.113.1")))
	require.Len(t, logger.warnMsgs, 2, "a rejection in a new window must produce a new summary line")
}

// TestPeerPoolLimiterLoggingStaysSilentWhenNothingRejected pins that an allowed
// connection never logs anything, so the limiter stays silent in steady state.
func TestPeerPoolLimiterLoggingStaysSilentWhenNothingRejected(t *testing.T) {
	logger := &recordingLogger{}
	l := &peerPoolLimiter{
		maxPerIP: 1000, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now, logger: logger, summaryInterval: 30 * time.Second,
	}
	l.setHost(&connsFixtureHost{})

	for range 20 {
		require.True(t, l.allow(net.ParseIP("203.0.113.1")))
		l.onConnected(net.ParseIP("203.0.113.1"))
	}
	require.Empty(t, logger.traceMsgs)
	require.Empty(t, logger.warnMsgs)
}

// TestPeerPoolLimiterSubscriberBlockDoesNotDoubleCountMaturedConnections pins that once
// a reservation's connection is confirmed live, it stops counting toward the
// subscriber-block tier as a reservation too - not just for its own IP's tier, which
// the per-attempt reconciliation already covered, but for every other IP sharing its
// block.
func TestPeerPoolLimiterSubscriberBlockDoesNotDoubleCountMaturedConnections(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 100, maxPerSubscriberBlock: 12, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	for i := 1; i <= 6; i++ {
		ip := fmt.Sprintf("130.0.0.%d", i)
		require.True(t, l.allow(net.ParseIP(ip)), "distinct address %d within the subscriber block", i)
		fixture.conns = append(fixture.conns, ip)
		l.onConnected(net.ParseIP(ip))
	}

	require.True(t, l.allow(net.ParseIP("130.0.0.7")),
		"a 7th distinct, already-matured address must still be admitted under a cap of 12 - each of the first 6 must count once, not twice (live + still-reserved)")
}

// TestPeerPoolLimiterChurningPeerIsNotPenalizedAfterDisconnecting pins that a
// reservation retires once its connection is confirmed live even if that connection
// later closes before any other admission attempt would have observed it live.
func TestPeerPoolLimiterChurningPeerIsNotPenalizedAfterDisconnecting(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 2, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)
	ip := net.ParseIP("203.0.113.5")

	for i := range 2 {
		require.True(t, l.allow(ip), "connect attempt %d", i)
		fixture.conns = []string{ip.String()}
		l.onConnected(ip)
		fixture.conns = nil
	}

	require.True(t, l.allow(ip), "a peer that connected and disconnected twice, with nothing of its currently live, must not be refused a third time")
}

// TestPeerPoolLimiterGlobalReservationPoolSurvivesConnectAndCloseChurn pins that
// ordinary connect-then-disconnect churn across many distinct IPs cannot exhaust the
// global reservation pool and lock out every other inbound peer.
func TestPeerPoolLimiterGlobalReservationPoolSurvivesConnectAndCloseChurn(t *testing.T) {
	l := &peerPoolLimiter{
		maxPerIP: 1000, maxPerSubscriberBlock: 1000, maxPerASBlock: 1000,
		v4SubscriberBlockBits: 24, v6SubscriberBlockBits: 56, v4ASBlockBits: 16, v6ASBlockBits: 32,
		reservationTTL: time.Minute, now: time.Now, maxReservations: 10,
	}
	fixture := &connsFixtureHost{}
	l.setHost(fixture)

	for i := range 10 {
		ip := net.ParseIP(fmt.Sprintf("203.0.113.%d", i+1))
		require.True(t, l.allow(ip), "attempt %d, each from a distinct IP, should fit within the reservation bound", i)
		fixture.conns = []string{ip.String()}
		l.onConnected(ip)
		fixture.conns = nil
	}

	require.True(t, l.allow(net.ParseIP("203.0.113.99")),
		"an 11th distinct IP must still be admitted: all 10 prior connections matured and closed, so none of their reservations should still occupy the global pool")
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
