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

package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeBasedRateLimiterRefreshesLimitEachWindow(t *testing.T) {
	limit := 2
	callbackCalls := 0
	limiter := newTimeBasedRateLimiter(time.Hour, func() int {
		callbackCalls++
		return limit
	})

	require.True(t, limiter.tryAcquire())
	require.True(t, limiter.tryAcquire())
	require.False(t, limiter.tryAcquire())
	require.Equal(t, 1, callbackCalls)

	limit = 3
	require.False(t, limiter.tryAcquire())
	require.Equal(t, 1, callbackCalls)
	limiter.start = time.Now().Add(-2 * time.Hour)
	require.True(t, limiter.tryAcquire())
	require.True(t, limiter.tryAcquire())
	require.True(t, limiter.tryAcquire())
	require.False(t, limiter.tryAcquire())
	require.Equal(t, 2, callbackCalls)

	limit = 1
	require.False(t, limiter.tryAcquire())
	limiter.start = time.Now().Add(-2 * time.Hour)
	require.True(t, limiter.tryAcquire())
	require.False(t, limiter.tryAcquire())
	require.Equal(t, 3, callbackCalls)
}

func TestTokenBucketRateLimiterSweepKeepsActiveBuckets(t *testing.T) {
	limiter := newTokenBucketRateLimiter(0.001, 1)
	require.True(t, limiter.acquire("a"))
	require.False(t, limiter.acquire("a"))

	limiter.lastSweep = time.Time{}
	require.False(t, limiter.acquire("a"))
	require.Contains(t, limiter.limiter, "a")

	limiter.limiter["a"].lastAccess = time.Now().Add(-4 * time.Minute)
	require.True(t, limiter.acquire("b"))
	require.Contains(t, limiter.limiter, "a")

	limiter.lastSweep = time.Time{}
	require.True(t, limiter.acquire("c"))
	require.NotContains(t, limiter.limiter, "a")
}
