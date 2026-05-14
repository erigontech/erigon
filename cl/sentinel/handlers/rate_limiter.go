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

package handlers

import (
	"context"
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

// tryConsume attempts to consume n tokens. Returns true if allowed.
func (tb *tokenBucket) tryConsume(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed > 0 {
		tb.tokens += elapsed * tb.refillRate
		if tb.tokens > tb.maxTokens {
			tb.tokens = tb.maxTokens
		}
		tb.lastRefill = now
	}
	// If elapsed <= 0, lastRefill is in the future (set by punishment). No
	// tokens are refilled and lastRefill stays advanced until real time
	// catches up, preventing burst regeneration during punishment.

	cost := float64(n)
	if tb.tokens >= cost {
		tb.tokens -= cost
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

// cleanupInterval is how often the background goroutine sweeps stale entries.
const cleanupInterval = 5 * time.Minute

// bucketIdleExpiry is how long a token bucket must be unused before it is evicted.
const bucketIdleExpiry = 10 * time.Minute

// peerRateLimiter enforces per-peer, per-protocol rate limits and per-peer
// concurrent stream caps on incoming ReqResp handlers.
//
// Rate limits are counted per response item, aligned with Lighthouse's approach.
// The wrapper pre-consumes 1 token for admission; batch handlers (blocks-by-range,
// blobs-by-range, etc.) consume additional tokens proportional to the requested
// item count after decoding the request.
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
// cost is the number of tokens to consume (typically 1 for the initial admission check).
// Returns true if the request is allowed, false if it should be rejected.
func (rl *peerRateLimiter) allowRequest(peerID string, protocol string, cost int) bool {
	key := peerProtocolKey{peerID: peerID, protocol: protocol}

	// Check punishment first.
	if expiry, ok := rl.punished.Load(key); ok {
		if time.Now().Before(expiry.(time.Time)) {
			return false
		}
		rl.punished.Delete(key)
	}

	return rl.consumeTokens(peerID, protocol, cost)
}

// consumeTokens attempts to consume cost tokens from the bucket for the given
// peer and protocol. If the bucket is exhausted, the peer is punished and false
// is returned. Batch handlers call this after decoding the request to charge for
// the actual number of response items.
func (rl *peerRateLimiter) consumeTokens(peerID string, protocol string, cost int) bool {
	if cost <= 0 {
		return true
	}
	key := peerProtocolKey{peerID: peerID, protocol: protocol}
	cfg, hasCfg := rl.protocolLimits[protocol]
	if !hasCfg {
		return true
	}
	bucketI, ok := rl.buckets.Load(key)
	if !ok {
		bucketI, _ = rl.buckets.LoadOrStore(key, newTokenBucket(cfg.maxTokens, cfg.refillRate))
	}
	bucket := bucketI.(*tokenBucket)
	if !bucket.tryConsume(cost) {
		// Drain the bucket and suppress refill during punishment. Advance
		// lastRefill so only tokens accumulated *after* the punishment
		// expires are available. We leave a small grace window (enough for
		// 1 token) so the peer's first admission check can succeed rather
		// than triggering an infinite punishment loop.
		grace := time.Duration(float64(time.Second) / cfg.refillRate)
		bucket.mu.Lock()
		bucket.tokens = 0
		bucket.lastRefill = time.Now().Add(punishmentDuration - grace)
		bucket.mu.Unlock()

		rl.punished.Store(key, time.Now().Add(punishmentDuration))
		return false
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

// startCleanup runs a background goroutine that periodically evicts stale
// entries from the sync.Maps to prevent unbounded memory growth as peers
// connect and disconnect over time.
func (rl *peerRateLimiter) startCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.cleanup()
			}
		}
	}()
}

func (rl *peerRateLimiter) cleanup() {
	now := time.Now()

	// Remove expired punishments.
	rl.punished.Range(func(key, value any) bool {
		if now.After(value.(time.Time)) {
			rl.punished.Delete(key)
		}
		return true
	})

	// Remove idle token buckets (not accessed within bucketIdleExpiry).
	rl.buckets.Range(func(key, value any) bool {
		bucket := value.(*tokenBucket)
		bucket.mu.Lock()
		idle := now.Sub(bucket.lastRefill) > bucketIdleExpiry
		bucket.mu.Unlock()
		if idle {
			rl.buckets.Delete(key)
		}
		return true
	})

	// Concurrency entries are not cleaned up. Each is a single *atomic.Int32
	// per peer (~40 bytes), so even over the lifetime of a long-running node
	// the memory is negligible. Cleaning them up would require synchronization
	// with acquireConcurrency/releaseConcurrency that adds complexity for no
	// practical benefit.
}
