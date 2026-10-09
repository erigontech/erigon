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
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	peerPoolLimiterMinPerIP     = 2
	peerPoolLimiterPerIPDivisor = 50

	// The subscriber-block tier is deliberately tight: a handful of peers from
	// the same allocation is normal, dozens is not.
	peerPoolLimiterMinPerSubscriberBlock     = 4
	peerPoolLimiterPerSubscriberBlockDivisor = 10

	// The AS-block tier is a loose backstop, not a primary control: even a large
	// shared block (a big cloud region, a CGNAT range) legitimately holds many
	// distinct operators, so it must stay permissive - but it must still never be
	// allowed to occupy the whole pool.
	peerPoolLimiterASBlockPoolFraction = 0.75

	// peerPoolLimiterV4SubscriberBlockBits (/24) and
	// peerPoolLimiterV6SubscriberBlockBits (/56, RFC 6177's recommended
	// single end-site allocation) are the subscriber-block tier.
	peerPoolLimiterV4SubscriberBlockBits = 24
	peerPoolLimiterV6SubscriberBlockBits = 56

	// peerPoolLimiterV4ASBlockBits (/16) and peerPoolLimiterV6ASBlockBits (/32,
	// the typical RIR-to-ISP allocation size) are the AS-block tier.
	peerPoolLimiterV4ASBlockBits = 16
	peerPoolLimiterV6ASBlockBits = 32

	// defaultPeerPoolReservationTTL bounds how long an admission reservation
	// counts toward the cap before it is assumed to have either registered in the
	// live host (where it then counts from there instead) or failed outright. It
	// must outlive the slowest legitimate handshake go-libp2p itself still permits:
	// the shared upgrader's own defaults allow up to 15s to accept a connection
	// plus 60s to negotiate it. A shorter TTL would let a reservation expire while
	// the connection is still pending (not yet live, no longer reserved), letting
	// a source accumulate more admissions than the cap once the slow handshakes
	// eventually land.
	defaultPeerPoolReservationTTL = 90 * time.Second

	// defaultPeerPoolMaxReservations bounds the limiter's own memory: a flood
	// spread across many distinct keys, each individually within its own cap,
	// must not be able to grow the reservation pool without bound.
	defaultPeerPoolMaxReservations = 8192

	defaultPeerPoolSummaryInterval = 30 * time.Second
)

// liveConnsSource gives the occupancy count a live view of currently connected
// remote IPs. Narrow on purpose so the occupancy math is testable without a real
// libp2p network stack.
type liveConnsSource interface {
	remoteIPs() []net.IP
}

// peerPoolLimiter bounds how many of the node's connection slots a single source IP,
// or a single IP block, can occupy at once. This is a different axis from
// ipRateLimiter's attempt-frequency cap: a patient attacker who stays under the rate
// limit could otherwise slowly acquire many concurrent connections and eclipse the
// node's view of the network - one address at a time, or spread across an allocation
// block so no single address repeats.
//
// Three nested tiers, tightest to loosest: a single IP, a subscriber-sized block
// (/24 v4, /56 v6) where a handful of peers is normal but dozens is suspicious, and
// an AS-sized block (/16 v4, /32 v6) that legitimately holds many distinct
// operators and is only capped to stop it from taking over the whole pool.
//
// InterceptAccept runs before the connection it is deciding on is registered in the
// host's live connection list, so counting only live connections is a TOCTOU race: a
// burst of concurrent accepts from the same source would all observe the same
// pre-admission snapshot and could all pass. allow tracks its own short-lived
// admission reservations, counted additively alongside live connections (occupancy =
// live + pending). A reservation is released once its connection is confirmed live
// (see onConnected) or, failing that, once it expires.
type peerPoolLimiter struct {
	host atomic.Pointer[liveConnsSource]

	mu              sync.Mutex
	reservations    []peerPoolReservation
	reservationTTL  time.Duration
	maxReservations int
	now             func() time.Time

	logger               poolLimiterLogger
	summaryInterval      time.Duration
	rejectedSinceSummary int
	lastSummary          time.Time

	maxPerIP              int
	maxPerSubscriberBlock int
	maxPerASBlock         int

	v4SubscriberBlockBits int
	v6SubscriberBlockBits int
	v4ASBlockBits         int
	v6ASBlockBits         int
}

type poolLimiterLogger interface {
	Trace(msg string, ctx ...any)
	Warn(msg string, ctx ...any)
}

type peerPoolReservation struct {
	ipKey         string
	subscriberKey string
	asKey         string
	expiresAt     time.Time
}

// newPeerPoolLimiter scales its caps with the configured peer pool size rather than
// using fixed constants, so the limiter stays meaningful for both a small
// --caplin.max-peer-count and a large one.
func newPeerPoolLimiter(maxPeerCount uint64, logger poolLimiterLogger) *peerPoolLimiter {
	maxPerSubscriberBlock := max(peerPoolLimiterMinPerSubscriberBlock, int(maxPeerCount)/peerPoolLimiterPerSubscriberBlockDivisor)
	maxPerASBlock := max(maxPerSubscriberBlock, int(float64(maxPeerCount)*peerPoolLimiterASBlockPoolFraction))
	return &peerPoolLimiter{
		reservationTTL:        defaultPeerPoolReservationTTL,
		maxReservations:       defaultPeerPoolMaxReservations,
		now:                   time.Now,
		logger:                logger,
		summaryInterval:       defaultPeerPoolSummaryInterval,
		maxPerIP:              max(peerPoolLimiterMinPerIP, int(maxPeerCount)/peerPoolLimiterPerIPDivisor),
		maxPerSubscriberBlock: maxPerSubscriberBlock,
		maxPerASBlock:         maxPerASBlock,
		v4SubscriberBlockBits: peerPoolLimiterV4SubscriberBlockBits,
		v6SubscriberBlockBits: peerPoolLimiterV6SubscriberBlockBits,
		v4ASBlockBits:         peerPoolLimiterV4ASBlockBits,
		v6ASBlockBits:         peerPoolLimiterV6ASBlockBits,
	}
}

// setHost lets the limiter see live connections once the host exists. The limiter is
// constructed before libp2p.New returns the host it gates, so allow fails open
// (allow) until this is called.
func (l *peerPoolLimiter) setHost(h liveConnsSource) {
	l.host.Store(&h)
}

func (l *peerPoolLimiter) allow(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() {
		return true
	}
	hostPtr := l.host.Load()
	if hostPtr == nil {
		return true
	}
	ipKey := ip.String()
	subscriberKey := l.subnetKey(ip, l.v4SubscriberBlockBits, l.v6SubscriberBlockBits)
	asKey := l.subnetKey(ip, l.v4ASBlockBits, l.v6ASBlockBits)

	l.mu.Lock()
	defer l.mu.Unlock()

	// Sampled under the lock, not before it: a connection that registers in the
	// host between an earlier sample and this decision must not be missed, or the
	// decision could use a stale, too-low count and admit past the cap.
	liveIP, liveSubscriberBlock, liveASBlock := l.liveCounts(*hostPtr, ip, subscriberKey, asKey)

	now := l.clock()
	l.pruneExpiredLocked(now)

	var reservedIP, reservedSubscriberBlock, reservedASBlock int
	for _, r := range l.reservations {
		if r.ipKey == ipKey {
			reservedIP++
		}
		if r.subscriberKey == subscriberKey {
			reservedSubscriberBlock++
		}
		if r.asKey == asKey {
			reservedASBlock++
		}
	}

	// Additive, not max: a reservation represents a connection attempt in flight
	// on top of whatever is already live, not an alternate accounting of the same
	// connections - onConnected is what retires a reservation once the live host
	// actually confirms it, so by this point every remaining reservation is still
	// genuinely pending.
	sameIP := liveIP + reservedIP
	sameSubscriberBlock := liveSubscriberBlock + reservedSubscriberBlock
	sameASBlock := liveASBlock + reservedASBlock

	if sameIP >= l.maxPerIP || sameSubscriberBlock >= l.maxPerSubscriberBlock || sameASBlock >= l.maxPerASBlock {
		l.recordRejection(ipKey, now)
		return false
	}

	if len(l.reservations) >= l.maxReservationsOrDefault() {
		// Global memory bound reached. Reject rather than evict: evicting an
		// unrelated key's reservation here would silently weaken that source's
		// cap to make room for this one.
		l.recordRejection(ipKey, now)
		return false
	}

	l.reservations = append(l.reservations, peerPoolReservation{
		ipKey:         ipKey,
		subscriberKey: subscriberKey,
		asKey:         asKey,
		expiresAt:     now.Add(l.ttl()),
	})
	return true
}

// onConnected retires one of ip's oldest pending reservations once a connection from
// it is confirmed live. This is what lets a reservation stop being counted once its
// connection is live, independent of whether any later admission attempt ever happens
// to observe that - polling for a live-count increase at the next unrelated allow()
// call misses a connection whose entire lifecycle completes between two such calls,
// and misses it entirely for every IP but the one being admitted at that moment.
func (l *peerPoolLimiter) onConnected(ip net.IP) {
	if ip == nil {
		return
	}
	ipKey := ip.String()
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, r := range l.reservations {
		if r.ipKey == ipKey {
			l.reservations = append(l.reservations[:i], l.reservations[i+1:]...)
			return
		}
	}
}

func (l *peerPoolLimiter) ttl() time.Duration {
	if l.reservationTTL > 0 {
		return l.reservationTTL
	}
	return defaultPeerPoolReservationTTL
}

func (l *peerPoolLimiter) maxReservationsOrDefault() int {
	if l.maxReservations > 0 {
		return l.maxReservations
	}
	return defaultPeerPoolMaxReservations
}

func (l *peerPoolLimiter) summaryIntervalOrDefault() time.Duration {
	if l.summaryInterval > 0 {
		return l.summaryInterval
	}
	return defaultPeerPoolSummaryInterval
}

// recordRejection logs every rejection at Trace (opt-in, silent by default) and
// flushes at most one Warn-level summary per summary interval, mirroring
// ipRateLimiter.recordRejection: a cap refusing a whole CGNAT or cloud block must stay
// visible without flooding the log at the same rate as the rejections themselves. Must
// be called with l.mu held.
func (l *peerPoolLimiter) recordRejection(ipKey string, now time.Time) {
	if l.logger == nil {
		return
	}
	l.logger.Trace("[Caplin] Rejected inbound connection attempt (peer-pool cap)", "ip", ipKey)

	l.rejectedSinceSummary++
	interval := l.summaryIntervalOrDefault()
	elapsed := now.Sub(l.lastSummary)
	if l.lastSummary.IsZero() {
		elapsed = interval
	}
	if elapsed < interval {
		return
	}
	rejected := l.rejectedSinceSummary
	l.rejectedSinceSummary = 0
	l.lastSummary = now

	l.logger.Warn("[Caplin] P2P peer-pool cap rejected inbound connection attempts",
		"rejected", rejected, "window", interval)
}

func (l *peerPoolLimiter) liveCounts(src liveConnsSource, ip net.IP, subscriberKey, asKey string) (sameIP, sameSubscriberBlock, sameASBlock int) {
	for _, remote := range src.remoteIPs() {
		if remote == nil {
			continue
		}
		if remote.Equal(ip) {
			sameIP++
		}
		if l.subnetKey(remote, l.v4SubscriberBlockBits, l.v6SubscriberBlockBits) == subscriberKey {
			sameSubscriberBlock++
		}
		if l.subnetKey(remote, l.v4ASBlockBits, l.v6ASBlockBits) == asKey {
			sameASBlock++
		}
	}
	return
}

func (l *peerPoolLimiter) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

// pruneExpiredLocked must be called with l.mu held.
func (l *peerPoolLimiter) pruneExpiredLocked(now time.Time) {
	kept := l.reservations[:0]
	for _, r := range l.reservations {
		if now.Before(r.expiresAt) {
			kept = append(kept, r)
		}
	}
	l.reservations = kept
}

func (l *peerPoolLimiter) subnetKey(ip net.IP, v4Bits, v6Bits int) string {
	if v4 := ip.To4(); v4 != nil {
		return v4.Mask(net.CIDRMask(v4Bits, 32)).String()
	}
	return ip.Mask(net.CIDRMask(v6Bits, 128)).String()
}
