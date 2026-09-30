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
	"sync/atomic"
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
type peerPoolLimiter struct {
	host atomic.Pointer[liveConnsSource]

	maxPerIP              int
	maxPerSubscriberBlock int
	maxPerASBlock         int

	v4SubscriberBlockBits int
	v6SubscriberBlockBits int
	v4ASBlockBits         int
	v6ASBlockBits         int
}

// newPeerPoolLimiter scales its caps with the configured peer pool size rather than
// using fixed constants, so the limiter stays meaningful for both a small
// --caplin.max-peer-count and a large one.
func newPeerPoolLimiter(maxPeerCount uint64) *peerPoolLimiter {
	maxPerSubscriberBlock := max(peerPoolLimiterMinPerSubscriberBlock, int(maxPeerCount)/peerPoolLimiterPerSubscriberBlockDivisor)
	maxPerASBlock := max(maxPerSubscriberBlock, int(float64(maxPeerCount)*peerPoolLimiterASBlockPoolFraction))
	return &peerPoolLimiter{
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
	subscriberKey := l.subnetKey(ip, l.v4SubscriberBlockBits, l.v6SubscriberBlockBits)
	asKey := l.subnetKey(ip, l.v4ASBlockBits, l.v6ASBlockBits)

	var sameIP, sameSubscriberBlock, sameASBlock int
	for _, remote := range (*hostPtr).remoteIPs() {
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
	return sameIP < l.maxPerIP &&
		sameSubscriberBlock < l.maxPerSubscriberBlock &&
		sameASBlock < l.maxPerASBlock
}

func (l *peerPoolLimiter) subnetKey(ip net.IP, v4Bits, v6Bits int) string {
	if v4 := ip.To4(); v4 != nil {
		return v4.Mask(net.CIDRMask(v4Bits, 32)).String()
	}
	return ip.Mask(net.CIDRMask(v6Bits, 128)).String()
}
