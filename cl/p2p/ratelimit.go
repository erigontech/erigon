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
	"time"
)

const (
	// defaultIPRateLimiterRate/Burst bound how many inbound connection attempts
	// (TCP or QUIC) one source IP can make per second before InterceptAccept starts
	// rejecting it pre-handshake. A legitimate peer reconnects occasionally, not
	// several times a second.
	defaultIPRateLimiterRate  = 2.0
	defaultIPRateLimiterBurst = 5.0

	// defaultIPRateLimiterMaxTrackedIPs bounds the limiter's own memory: a flood
	// from many distinct source IPs must not be able to grow this map without
	// bound, since that would turn the defense itself into a memory-exhaustion
	// vector. Oldest-tracked IPs are evicted first.
	defaultIPRateLimiterMaxTrackedIPs = 8192

	defaultIPRateLimiterSummaryInterval = 30 * time.Second
)

type rateLimiterLogger interface {
	Trace(msg string, ctx ...any)
	Warn(msg string, ctx ...any)
}

type ipRateLimiterConfig struct {
	rate            float64
	burst           float64
	maxTrackedIPs   int
	summaryInterval time.Duration
}

func defaultIPRateLimiterConfig() ipRateLimiterConfig {
	return ipRateLimiterConfig{
		rate:            defaultIPRateLimiterRate,
		burst:           defaultIPRateLimiterBurst,
		maxTrackedIPs:   defaultIPRateLimiterMaxTrackedIPs,
		summaryInterval: defaultIPRateLimiterSummaryInterval,
	}
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// ipRateLimiter bounds inbound connection *attempts* per source IP, independent of
// peer identity: a libp2p peer ID is a free, self-certified keypair, so an
// identity-based dedup gives no protection against a connection flood — an attacker
// mints a new identity per attempt. This limiter runs in InterceptAccept, before any
// handshake cost is paid for TCP.
type ipRateLimiter struct {
	cfg    ipRateLimiterConfig
	logger rateLimiterLogger
	now    func() time.Time

	mu      sync.Mutex
	buckets map[string]*tokenBucket
	lru     *list.List
	elems   map[string]*list.Element

	rejectedSinceSummary int
	lastSummary          time.Time
}

func newIPRateLimiter(cfg ipRateLimiterConfig, logger rateLimiterLogger, now func() time.Time) *ipRateLimiter {
	return &ipRateLimiter{
		cfg:     cfg,
		logger:  logger,
		now:     now,
		buckets: make(map[string]*tokenBucket),
		lru:     list.New(),
		elems:   make(map[string]*list.Element),
	}
}

// allow reports whether a new connection attempt from ip is permitted, consuming a
// token if so. Loopback is always allowed: this limiter targets remote floods, not
// local or test traffic.
func (l *ipRateLimiter) allow(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() {
		return true
	}
	key := ip.String()
	now := l.now()

	l.mu.Lock()
	bucket, ok := l.buckets[key]
	if !ok {
		bucket = &tokenBucket{tokens: l.cfg.burst, lastRefill: now}
		l.track(key, bucket)
	} else {
		l.touch(key)
	}
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed > 0 {
		bucket.tokens = min(l.cfg.burst, bucket.tokens+elapsed*l.cfg.rate)
		bucket.lastRefill = now
	}
	allowed := bucket.tokens >= 1
	if allowed {
		bucket.tokens--
	}
	l.mu.Unlock()

	if !allowed {
		l.recordRejection(key, now)
	}
	return allowed
}

func (l *ipRateLimiter) trackedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// track and touch must be called with l.mu held.
func (l *ipRateLimiter) track(key string, bucket *tokenBucket) {
	l.buckets[key] = bucket
	l.elems[key] = l.lru.PushFront(key)
	if l.lru.Len() <= l.cfg.maxTrackedIPs {
		return
	}
	oldest := l.lru.Back()
	if oldest == nil {
		return
	}
	oldestKey := oldest.Value.(string)
	l.lru.Remove(oldest)
	delete(l.elems, oldestKey)
	delete(l.buckets, oldestKey)
}

func (l *ipRateLimiter) touch(key string) {
	if elem, ok := l.elems[key]; ok {
		l.lru.MoveToFront(elem)
	}
}

// recordRejection logs every rejection at Trace (opt-in, silent by default) and
// flushes at most one Warn-level summary per summaryInterval. A flood is exactly the
// condition where per-event logging becomes a self-inflicted resource drain — log
// formatting and I/O scaling with an attacker-controlled request rate — so the
// visible-by-default log must stay periodic and silent in steady state.
func (l *ipRateLimiter) recordRejection(key string, now time.Time) {
	l.logger.Trace("[Caplin] Rejected inbound connection attempt (rate limited)", "ip", key)

	l.mu.Lock()
	l.rejectedSinceSummary++
	elapsed := now.Sub(l.lastSummary)
	if l.lastSummary.IsZero() {
		elapsed = l.cfg.summaryInterval // first-ever rejection always flushes immediately
	}
	if elapsed < l.cfg.summaryInterval {
		l.mu.Unlock()
		return
	}
	rejected := l.rejectedSinceSummary
	tracked := len(l.buckets)
	l.rejectedSinceSummary = 0
	l.lastSummary = now
	l.mu.Unlock()

	l.logger.Warn("[Caplin] P2P rate-limited inbound connection attempts",
		"rejected", rejected, "tracked_ips", tracked, "window", l.cfg.summaryInterval)
}
