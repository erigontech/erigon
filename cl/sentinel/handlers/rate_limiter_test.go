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
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_AllowRequest(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.BeaconBlocksByRangeProtocolV2

	// First 128 single-token requests should be allowed (burst capacity).
	for i := 0; i < 128; i++ {
		require.True(t, rl.allowRequest(peer, proto, 1), "request %d should be allowed", i)
	}

	// 129th request should be rejected (burst exhausted).
	require.False(t, rl.allowRequest(peer, proto, 1), "request 129 should be rate-limited")

	// A different peer should still be allowed.
	require.True(t, rl.allowRequest("16Uiu2peer2", proto, 1), "different peer should be allowed")
}

func TestRateLimiter_PingLimit(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.PingProtocolV1

	// Ping limit is 2 burst.
	require.True(t, rl.allowRequest(peer, proto, 1))
	require.True(t, rl.allowRequest(peer, proto, 1))
	require.False(t, rl.allowRequest(peer, proto, 1), "3rd ping should be rate-limited")
}

func TestRateLimiter_Concurrency(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"

	// Acquire up to the limit.
	for i := 0; i < maxConcurrentRequestsPerPeer; i++ {
		require.True(t, rl.acquireConcurrency(peer), "acquire %d should succeed", i)
	}

	// One more should fail.
	require.False(t, rl.acquireConcurrency(peer), "should fail at concurrency limit")

	// Release one and try again.
	rl.releaseConcurrency(peer)
	require.True(t, rl.acquireConcurrency(peer), "should succeed after release")
}

func TestRateLimiter_PunishmentBlocksRequests(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.PingProtocolV1

	// Exhaust ping tokens to trigger punishment.
	rl.allowRequest(peer, proto, 1)
	rl.allowRequest(peer, proto, 1)
	require.False(t, rl.allowRequest(peer, proto, 1)) // triggers punishment

	// Subsequent requests should also be rejected (punishment in effect).
	require.False(t, rl.allowRequest(peer, proto, 1))

	// Different protocol from the same peer should still work.
	require.True(t, rl.allowRequest(peer, communication.StatusProtocolV1, 1))
}

func TestRateLimiter_UnknownProtocolAllowed(t *testing.T) {
	rl := newPeerRateLimiter()
	// Protocols not in the rate limit map should be allowed (no rate config = no limit).
	for i := 0; i < 1000; i++ {
		require.True(t, rl.allowRequest("peer1", "/unknown/protocol/v1", 1))
	}
}

func TestRateLimiter_PerItemCost(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.BeaconBlocksByRangeProtocolV2 // burst = 128

	// A single request consuming 96 tokens should succeed.
	require.True(t, rl.allowRequest(peer, proto, 1))   // wrapper admission: 1 token
	require.True(t, rl.consumeTokens(peer, proto, 95)) // handler: 95 more (96 total)
	// 128 - 96 = 32 tokens remaining.

	// Another request for 96 blocks should fail (only 32 tokens left).
	require.True(t, rl.allowRequest(peer, proto, 1))    // admission: 1 token (31 left)
	require.False(t, rl.consumeTokens(peer, proto, 95)) // 95 > 31, rejected + punished

	// Peer is now punished — even a single-token request is rejected.
	require.False(t, rl.allowRequest(peer, proto, 1))
}

func TestRateLimiter_ConsumeTokensZeroCost(t *testing.T) {
	rl := newPeerRateLimiter()
	// Zero and negative costs should always succeed.
	require.True(t, rl.consumeTokens("peer1", communication.PingProtocolV1, 0))
	require.True(t, rl.consumeTokens("peer1", communication.PingProtocolV1, -5))
}

func TestRateLimiter_PunishmentDrainsBucket(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.BeaconBlocksByRangeProtocolV2 // burst = 128, refill = 12.8/s

	// Exhaust tokens to trigger punishment.
	require.True(t, rl.allowRequest(peer, proto, 128))
	require.False(t, rl.allowRequest(peer, proto, 1)) // triggers punishment + drain

	// Verify the bucket was drained and lastRefill was advanced.
	key := peerProtocolKey{peerID: peer, protocol: proto}
	bucketI, ok := rl.buckets.Load(key)
	require.True(t, ok)
	bucket := bucketI.(*tokenBucket)
	bucket.mu.Lock()
	require.Equal(t, float64(0), bucket.tokens, "bucket should be drained to 0")
	require.True(t, bucket.lastRefill.After(time.Now().Add(punishmentDuration-2*time.Second)),
		"lastRefill should be advanced near the punishment expiry")
	bucket.mu.Unlock()

	// Simulate the punishment period elapsing: move lastRefill to just before
	// now (by the grace window), then clear the punishment entry.
	grace := time.Duration(float64(time.Second) / 12.8) // 1/refillRate
	bucket.mu.Lock()
	bucket.lastRefill = time.Now().Add(-grace)
	bucket.mu.Unlock()
	rl.punished.Delete(key)

	// After punishment, only ~1 token from the grace window is available.
	// A single admission succeeds but a large batch does not — the peer
	// cannot burst 128 items immediately like it could without the drain.
	require.True(t, rl.allowRequest(peer, proto, 1))    // ~1 token available
	require.False(t, rl.consumeTokens(peer, proto, 95)) // not enough for a full batch
}

func TestRateLimiter_Cleanup(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.PingProtocolV1

	// Exhaust tokens to create bucket + punishment entries.
	rl.allowRequest(peer, proto, 1)
	rl.allowRequest(peer, proto, 1)
	rl.allowRequest(peer, proto, 1) // triggers punishment

	// Also create a concurrency entry and release it.
	rl.acquireConcurrency(peer)
	rl.releaseConcurrency(peer)

	key := peerProtocolKey{peerID: peer, protocol: proto}

	// Verify entries exist.
	_, hasBucket := rl.buckets.Load(key)
	require.True(t, hasBucket)
	_, hasPunishment := rl.punished.Load(key)
	require.True(t, hasPunishment)
	_, hasConcurrency := rl.concurrency.Load(peer)
	require.True(t, hasConcurrency)

	// Run cleanup — bucket and punishment should survive (recently used / not expired).
	rl.cleanup()

	_, hasBucket = rl.buckets.Load(key)
	require.True(t, hasBucket, "recent bucket should survive cleanup")
	_, hasPunishment = rl.punished.Load(key)
	require.True(t, hasPunishment, "unexpired punishment should survive cleanup")

	// Concurrency entries are intentionally not cleaned up (negligible memory).
	_, hasConcurrency = rl.concurrency.Load(peer)
	require.True(t, hasConcurrency, "concurrency entries should persist")
}
