package handlers

import (
	"testing"

	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_AllowRequest(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.BeaconBlocksByRangeProtocolV2

	// First 128 requests should be allowed (burst capacity).
	for i := 0; i < 128; i++ {
		require.True(t, rl.allowRequest(peer, proto), "request %d should be allowed", i)
	}

	// 129th request should be rejected (burst exhausted).
	require.False(t, rl.allowRequest(peer, proto), "request 129 should be rate-limited")

	// A different peer should still be allowed.
	require.True(t, rl.allowRequest("16Uiu2peer2", proto), "different peer should be allowed")
}

func TestRateLimiter_PingLimit(t *testing.T) {
	rl := newPeerRateLimiter()
	peer := "16Uiu2peer1"
	proto := communication.PingProtocolV1

	// Ping limit is 2 burst.
	require.True(t, rl.allowRequest(peer, proto))
	require.True(t, rl.allowRequest(peer, proto))
	require.False(t, rl.allowRequest(peer, proto), "3rd ping should be rate-limited")
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
	rl.allowRequest(peer, proto)
	rl.allowRequest(peer, proto)
	require.False(t, rl.allowRequest(peer, proto)) // triggers punishment

	// Subsequent requests should also be rejected (punishment in effect).
	require.False(t, rl.allowRequest(peer, proto))

	// Different protocol from the same peer should still work.
	require.True(t, rl.allowRequest(peer, communication.StatusProtocolV1))
}

func TestRateLimiter_UnknownProtocolAllowed(t *testing.T) {
	rl := newPeerRateLimiter()
	// Protocols not in the rate limit map should be allowed (no rate config = no limit).
	for i := 0; i < 1000; i++ {
		require.True(t, rl.allowRequest("peer1", "/unknown/protocol/v1"))
	}
}
