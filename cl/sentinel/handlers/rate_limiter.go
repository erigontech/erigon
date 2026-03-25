// Copyright 2024 The Erigon Authors
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

package handlers

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/sentinel/communication"
)

// peerProtocolKey uniquely identifies a (peer, protocol) pair for rate limiting.
type peerProtocolKey struct {
	peerID   string
	protocol string
}

// tokenBucket implements a simple token bucket rate limiter.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func newTokenBucket(maxTokens float64, refillRate float64) *tokenBucket {
	return &tokenBucket{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// tryConsume attempts to consume one token. Returns true if allowed.
func (tb *tokenBucket) tryConsume() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// protocolRateConfig defines the rate limit for a protocol.
type protocolRateConfig struct {
	maxTokens  float64 // burst capacity
	refillRate float64 // tokens per second
}

// maxConcurrentRequestsPerPeer is the maximum number of concurrent in-flight
// requests a single peer can have across all protocols.
const maxConcurrentRequestsPerPeer = 5

// punishmentDuration is how long a peer is blocked after exceeding the rate limit.
const punishmentDuration = 30 * time.Second

// peerRateLimiter enforces per-peer, per-protocol rate limits and per-peer
// concurrent stream caps on incoming ReqResp handlers.
type peerRateLimiter struct {
	// protocolLimits maps protocol name -> rate config
	protocolLimits map[string]protocolRateConfig

	// buckets maps (peerID, protocol) -> token bucket
	buckets sync.Map // map[peerProtocolKey]*tokenBucket

	// concurrency maps peerID -> current in-flight request count
	concurrency sync.Map // map[string]*atomic.Int32

	// punished maps (peerID, protocol) -> punishment expiry time
	punished sync.Map // map[peerProtocolKey]time.Time
}

func newPeerRateLimiter() *peerRateLimiter {
	rl := &peerRateLimiter{
		protocolLimits: make(map[string]protocolRateConfig),
	}

	// Rate limits aligned with Lighthouse/Lodestar.
	// Format: maxTokens (burst), refillRate (tokens/second)
	// Lighthouse uses ~128 tokens per 10s for block requests, 5/15s for status, etc.
	blockRate := protocolRateConfig{maxTokens: 128, refillRate: 12.8}   // 128 per 10s
	blobRate := protocolRateConfig{maxTokens: 72, refillRate: 7.2}      // 72 per 10s
	dataColRate := protocolRateConfig{maxTokens: 128, refillRate: 12.8} // 128 per 10s
	lcRate := protocolRateConfig{maxTokens: 128, refillRate: 12.8}      // 128 per 10s
	pingRate := protocolRateConfig{maxTokens: 2, refillRate: 0.2}       // 2 per 10s
	goodbyeRate := protocolRateConfig{maxTokens: 1, refillRate: 0.1}    // 1 per 10s
	statusRate := protocolRateConfig{maxTokens: 5, refillRate: 0.33}    // 5 per 15s
	metadataRate := protocolRateConfig{maxTokens: 2, refillRate: 0.2}   // 2 per 10s

	rl.protocolLimits[communication.BeaconBlocksByRangeProtocolV2] = blockRate
	rl.protocolLimits[communication.BeaconBlocksByRootProtocolV2] = blockRate
	rl.protocolLimits[communication.BlobSidecarByRangeProtocolV1] = blobRate
	rl.protocolLimits[communication.BlobSidecarByRootProtocolV1] = blobRate
	rl.protocolLimits[communication.DataColumnSidecarsByRangeProtocolV1] = dataColRate
	rl.protocolLimits[communication.DataColumnSidecarsByRootProtocolV1] = dataColRate
	rl.protocolLimits[communication.PingProtocolV1] = pingRate
	rl.protocolLimits[communication.GoodbyeProtocolV1] = goodbyeRate
	rl.protocolLimits[communication.StatusProtocolV1] = statusRate
	rl.protocolLimits[communication.StatusProtocolV2] = statusRate
	rl.protocolLimits[communication.MetadataProtocolV1] = metadataRate
	rl.protocolLimits[communication.MetadataProtocolV2] = metadataRate
	rl.protocolLimits[communication.MetadataProtocolV3] = metadataRate
	rl.protocolLimits[communication.LightClientOptimisticUpdateProtocolV1] = lcRate
	rl.protocolLimits[communication.LightClientFinalityUpdateProtocolV1] = lcRate
	rl.protocolLimits[communication.LightClientBootstrapProtocolV1] = lcRate
	rl.protocolLimits[communication.LightClientUpdatesByRangeProtocolV1] = lcRate

	return rl
}

// allowRequest checks whether a request from peerID on the given protocol should be allowed.
// Returns true if the request is allowed, false if it should be rejected.
func (rl *peerRateLimiter) allowRequest(peerID string, protocol string) bool {
	key := peerProtocolKey{peerID: peerID, protocol: protocol}

	// Check punishment first.
	if expiry, ok := rl.punished.Load(key); ok {
		if time.Now().Before(expiry.(time.Time)) {
			return false
		}
		rl.punished.Delete(key)
	}

	// Check per-protocol token bucket.
	cfg, hasCfg := rl.protocolLimits[protocol]
	if hasCfg {
		bucketI, _ := rl.buckets.LoadOrStore(key, newTokenBucket(cfg.maxTokens, cfg.refillRate))
		bucket := bucketI.(*tokenBucket)
		if !bucket.tryConsume() {
			// Rate limit exceeded — punish the peer for this protocol.
			rl.punished.Store(key, time.Now().Add(punishmentDuration))
			return false
		}
	}

	return true
}

// acquireConcurrency tries to increment the in-flight count for a peer.
// Returns true if the request is allowed (under the concurrency cap).
func (rl *peerRateLimiter) acquireConcurrency(peerID string) bool {
	counterI, _ := rl.concurrency.LoadOrStore(peerID, &atomic.Int32{})
	counter := counterI.(*atomic.Int32)
	for {
		cur := counter.Load()
		if cur >= int32(maxConcurrentRequestsPerPeer) {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// releaseConcurrency decrements the in-flight count for a peer.
func (rl *peerRateLimiter) releaseConcurrency(peerID string) {
	if counterI, ok := rl.concurrency.Load(peerID); ok {
		counter := counterI.(*atomic.Int32)
		counter.Add(-1)
	}
}
