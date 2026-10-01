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
	"container/list"
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

	// defaultPeerPoolMaxIPBaselines bounds ipLiveBaseline. An IP's baseline entry
	// is only ever revisited (and so only ever cleaned up) by a later admission
	// attempt from the same IP; one that connects once and never attempts again
	// (ordinary peer churn, not an attacker) would otherwise sit in the map
	// forever. Least-recently-reconciled entries are evicted first.
	defaultPeerPoolMaxIPBaselines = 8192
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
// admission reservations, counted *additively* alongside live connections (live +
// reserved, not max(live, reserved): the max form undercounts whenever a reservation
// represents a genuinely new connection rather than one already reflected live -
// e.g. two pre-existing live connections plus two brand new concurrent reservations
// is 4 pending connections, not max(2,2)=2).
//
// Reservations are released two ways: by TTL (since a connection rejected by a later
// gater hook never fires a close notification, so a notifee-based release would leak),
// and by reconciliation - once the live host confirms at least one more connection
// from a given IP than last observed, that many of the IP's oldest reservations are
// retired immediately rather than waiting out the rest of their TTL. Reconciling at
// the IP tier is sufficient for the coarser tiers too: a reservation tagged with a
// subscriber-block or AS-block key is also tagged with the IP key, so retiring it once
// its own IP's live count catches up removes its contribution from every tier it was
// counted in.
type peerPoolLimiter struct {
	host atomic.Pointer[liveConnsSource]

	mu              sync.Mutex
	reservations    []peerPoolReservation
	reservationTTL  time.Duration
	maxReservations int
	now             func() time.Time
	// ipLiveBaseline is the live-connection count per IP as of its last
	// reconciliation, LRU-bounded by maxIPBaselines (see its doc comment).
	ipLiveBaseline      map[string]int
	ipLiveBaselineLRU   *list.List
	ipLiveBaselineElems map[string]*list.Element
	maxIPBaselines      int

	maxPerIP              int
	maxPerSubscriberBlock int
	maxPerASBlock         int

	v4SubscriberBlockBits int
	v6SubscriberBlockBits int
	v4ASBlockBits         int
	v6ASBlockBits         int
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
func newPeerPoolLimiter(maxPeerCount uint64) *peerPoolLimiter {
	maxPerSubscriberBlock := max(peerPoolLimiterMinPerSubscriberBlock, int(maxPeerCount)/peerPoolLimiterPerSubscriberBlockDivisor)
	maxPerASBlock := max(maxPerSubscriberBlock, int(float64(maxPeerCount)*peerPoolLimiterASBlockPoolFraction))
	return &peerPoolLimiter{
		reservationTTL:        defaultPeerPoolReservationTTL,
		maxReservations:       defaultPeerPoolMaxReservations,
		now:                   time.Now,
		ipLiveBaseline:        make(map[string]int),
		ipLiveBaselineLRU:     list.New(),
		ipLiveBaselineElems:   make(map[string]*list.Element),
		maxIPBaselines:        defaultPeerPoolMaxIPBaselines,
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
	l.reconcileLocked(ipKey, liveIP)

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
	// connections - reconcileLocked above is what retires a reservation once the
	// live host actually confirms it, so by this point every remaining reservation
	// is still genuinely pending.
	sameIP := liveIP + reservedIP
	sameSubscriberBlock := liveSubscriberBlock + reservedSubscriberBlock
	sameASBlock := liveASBlock + reservedASBlock

	if sameIP >= l.maxPerIP || sameSubscriberBlock >= l.maxPerSubscriberBlock || sameASBlock >= l.maxPerASBlock {
		return false
	}

	if len(l.reservations) >= l.maxReservationsOrDefault() {
		// Global memory bound reached. Reject rather than evict: evicting an
		// unrelated key's reservation here would silently weaken that source's
		// cap to make room for this one.
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

// reconcileLocked retires ipKey's oldest reservations once the live host confirms at
// least as many of its connections as were pending, rather than waiting out their full
// TTL. It must be called with l.mu held, after pruneExpiredLocked.
func (l *peerPoolLimiter) reconcileLocked(ipKey string, liveIP int) {
	l.ensureIPLiveBaselineLocked()
	delta := liveIP - l.ipLiveBaseline[ipKey]
	if delta <= 0 {
		if liveIP == 0 {
			l.deleteIPBaselineLocked(ipKey)
		}
		return
	}
	retired := 0
	remaining := l.reservations[:0]
	for _, r := range l.reservations {
		if retired < delta && r.ipKey == ipKey {
			retired++
			continue
		}
		remaining = append(remaining, r)
	}
	l.reservations = remaining
	l.setIPBaselineLocked(ipKey, liveIP)
}

func (l *peerPoolLimiter) ensureIPLiveBaselineLocked() {
	if l.ipLiveBaseline == nil {
		l.ipLiveBaseline = make(map[string]int)
	}
	if l.ipLiveBaselineLRU == nil {
		l.ipLiveBaselineLRU = list.New()
	}
	if l.ipLiveBaselineElems == nil {
		l.ipLiveBaselineElems = make(map[string]*list.Element)
	}
}

func (l *peerPoolLimiter) maxIPBaselinesOrDefault() int {
	if l.maxIPBaselines > 0 {
		return l.maxIPBaselines
	}
	return defaultPeerPoolMaxIPBaselines
}

// setIPBaselineLocked records ipKey's reconciled live count and marks it
// most-recently-used, evicting the least-recently-reconciled entry if the bound is
// exceeded. Must be called with l.mu held.
func (l *peerPoolLimiter) setIPBaselineLocked(ipKey string, liveIP int) {
	l.ipLiveBaseline[ipKey] = liveIP
	if elem, ok := l.ipLiveBaselineElems[ipKey]; ok {
		l.ipLiveBaselineLRU.MoveToFront(elem)
		return
	}
	l.ipLiveBaselineElems[ipKey] = l.ipLiveBaselineLRU.PushFront(ipKey)
	if l.ipLiveBaselineLRU.Len() <= l.maxIPBaselinesOrDefault() {
		return
	}
	oldest := l.ipLiveBaselineLRU.Back()
	if oldest == nil {
		return
	}
	l.deleteIPBaselineLocked(oldest.Value.(string))
}

func (l *peerPoolLimiter) deleteIPBaselineLocked(ipKey string) {
	delete(l.ipLiveBaseline, ipKey)
	if elem, ok := l.ipLiveBaselineElems[ipKey]; ok {
		l.ipLiveBaselineLRU.Remove(elem)
		delete(l.ipLiveBaselineElems, ipKey)
	}
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
