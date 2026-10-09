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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type recordingLogger struct {
	warnMsgs  []string
	warnCtx   [][]any
	traceMsgs []string
}

func (l *recordingLogger) Warn(msg string, ctx ...any) {
	l.warnMsgs = append(l.warnMsgs, msg)
	l.warnCtx = append(l.warnCtx, ctx)
}

func (l *recordingLogger) Trace(msg string, ctx ...any) {
	l.traceMsgs = append(l.traceMsgs, msg)
}

func TestIPRateLimiterAllowsBurstThenRejects(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 1, burst: 3, maxTrackedIPs: 10, summaryInterval: time.Minute}, &recordingLogger{}, clock.Now)

	ip := net.ParseIP("203.0.113.5")
	for i := range 3 {
		require.True(t, limiter.allow(ip), "attempt %d should be within burst", i)
	}
	require.False(t, limiter.allow(ip), "fourth immediate attempt should exceed the burst")
}

func TestIPRateLimiterRefillsOverTime(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 2, burst: 1, maxTrackedIPs: 10, summaryInterval: time.Minute}, &recordingLogger{}, clock.Now)

	ip := net.ParseIP("203.0.113.5")
	require.True(t, limiter.allow(ip))
	require.False(t, limiter.allow(ip), "burst of 1 is exhausted")

	clock.advance(500 * time.Millisecond) // rate=2/s -> 1 token refilled
	require.True(t, limiter.allow(ip), "a token should have refilled after 500ms at 2/s")
	require.False(t, limiter.allow(ip))
}

func TestIPRateLimiterTracksEachIPIndependently(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 1, burst: 1, maxTrackedIPs: 10, summaryInterval: time.Minute}, &recordingLogger{}, clock.Now)

	ipA := net.ParseIP("203.0.113.5")
	ipB := net.ParseIP("203.0.113.6")
	require.True(t, limiter.allow(ipA))
	require.False(t, limiter.allow(ipA))
	require.True(t, limiter.allow(ipB), "a different source IP must not be affected by ipA's bucket")
}

func TestIPRateLimiterExemptsLoopback(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 1, burst: 1, maxTrackedIPs: 10, summaryInterval: time.Minute}, &recordingLogger{}, clock.Now)

	loopback := net.ParseIP("127.0.0.1")
	for i := range 50 {
		require.True(t, limiter.allow(loopback), "attempt %d from loopback must never be limited", i)
	}
}

func TestIPRateLimiterEvictsLeastRecentlyUsedWhenOverCapacity(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 1, burst: 1, maxTrackedIPs: 2, summaryInterval: time.Minute}, &recordingLogger{}, clock.Now)

	ipA := net.ParseIP("203.0.113.5")
	ipB := net.ParseIP("203.0.113.6")
	ipC := net.ParseIP("203.0.113.7")

	require.True(t, limiter.allow(ipA)) // tracked: [A]
	require.True(t, limiter.allow(ipB)) // tracked: [A, B], at capacity
	require.True(t, limiter.allow(ipC)) // evicts A (least recently used), tracked: [B, C]

	require.Equal(t, 2, limiter.trackedCount())
	require.True(t, limiter.allow(ipA), "ipA's bucket should have been evicted and reset, not still exhausted")
}

func TestIPRateLimiterLogsPeriodicSummaryNotPerRejection(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	logger := &recordingLogger{}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 0, burst: 1, maxTrackedIPs: 10, summaryInterval: 30 * time.Second}, logger, clock.Now)

	ip := net.ParseIP("203.0.113.5")
	require.True(t, limiter.allow(ip))
	for range 100 {
		require.False(t, limiter.allow(ip))
	}

	require.Len(t, logger.warnMsgs, 1, "100 rejections within one window must produce exactly one summary log line, not one per rejection")

	clock.advance(31 * time.Second)
	require.False(t, limiter.allow(ip))
	require.Len(t, logger.warnMsgs, 2, "a rejection in a new window must produce a new summary line")

	clock.advance(31 * time.Second)
	require.Len(t, logger.warnMsgs, 2, "no further rejections must not produce a new summary line")
}

func TestIPRateLimiterLogsEachRejectionAtTraceOnly(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	logger := &recordingLogger{}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 0, burst: 1, maxTrackedIPs: 10, summaryInterval: 30 * time.Second}, logger, clock.Now)

	ip := net.ParseIP("203.0.113.5")
	require.True(t, limiter.allow(ip))
	for range 5 {
		require.False(t, limiter.allow(ip))
	}

	require.Len(t, logger.traceMsgs, 5, "every individual rejection should still be visible at Trace level")
	require.Len(t, logger.warnMsgs, 1, "but only one summary line at Warn level")
}

func TestIPRateLimiterSummaryOmittedWhenNothingRejected(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	logger := &recordingLogger{}
	limiter := newIPRateLimiter(ipRateLimiterConfig{rate: 100, burst: 100, maxTrackedIPs: 10, summaryInterval: time.Millisecond}, logger, clock.Now)

	ip := net.ParseIP("203.0.113.5")
	for range 20 {
		require.True(t, limiter.allow(ip))
		clock.advance(time.Millisecond)
	}
	require.Empty(t, logger.warnMsgs, "steady state with nothing rejected must stay silent")
}
