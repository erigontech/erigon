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

package lru

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// deadlineOf reads the deadline the cache stored for a key.
func deadlineOf[K comparable, V any](t *testing.T, c *CacheWithTTL[K, V], k K) time.Time {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache.Peek(k)
	require.True(t, ok, "no entry held for the key")
	return e.expiresAt
}

// setDeadline rewrites the deadline the cache stored for a key, so a test can hand Get an entry
// that is past its deadline without waiting on the wall clock. The rewrite goes through Add, which
// also makes the key the most recently used; tests that care about recency order it explicitly.
func setDeadline[K comparable, V any](t *testing.T, c *CacheWithTTL[K, V], k K, at time.Time) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache.Peek(k)
	require.True(t, ok, "no entry held for the key")
	e.expiresAt = at
	c.cache.Add(k, e)
}

// resident reports whether the cache still holds a key, without the expiry check Get applies.
func resident[K comparable, V any](c *CacheWithTTL[K, V], k K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.cache.Peek(k)
	return ok
}

func TestCacheWithTTLGetHitsALiveEntry(t *testing.T) {
	c := NewWithTTL[string, int]("hits_live", 8, time.Hour)
	c.Add("a", 1)

	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)
	require.Equal(t, 1, c.Len())
}

func TestCacheWithTTLGetDropsAnExpiredEntry(t *testing.T) {
	c := NewWithTTL[string, *int]("drops_expired", 8, time.Hour)
	one := 1
	c.Add("a", &one)
	setDeadline(t, c, "a", time.Now().Add(-time.Second))
	require.Equal(t, 1, c.Len(), "an expired entry stays resident until it is read")

	v, ok := c.Get("a")
	require.False(t, ok)
	require.Nil(t, v, "a miss returns the zero value, not the expired one")
	require.Equal(t, 0, c.Len())
	require.False(t, resident(c, "a"))
}

func TestCacheWithTTLExpiredBoundary(t *testing.T) {
	deadline := time.Unix(1_700_000_000, 0)
	e := ttlEntry[int]{expiresAt: deadline}

	require.False(t, e.expired(deadline.Add(-time.Nanosecond)))
	require.False(t, e.expired(deadline), "the deadline itself is still live")
	require.True(t, e.expired(deadline.Add(time.Nanosecond)))
}

func TestCacheWithTTLDeadlineStartsAtAdd(t *testing.T) {
	const ttl = time.Hour
	c := NewWithTTL[string, int]("deadline_at_add", 8, ttl)

	before := time.Now()
	c.Add("a", 1)
	after := time.Now()

	deadline := deadlineOf(t, c, "a")
	require.False(t, deadline.Before(before.Add(ttl)))
	require.False(t, deadline.After(after.Add(ttl)))
}

func TestCacheWithTTLAddRenewsTheDeadline(t *testing.T) {
	c := NewWithTTL[string, int]("renews", 8, time.Hour)
	c.Add("a", 1)

	t.Run("before expiry", func(t *testing.T) {
		setDeadline(t, c, "a", time.Now().Add(time.Minute))
		c.Add("a", 2)

		require.Equal(t, 1, c.Len())
		require.True(t, deadlineOf(t, c, "a").After(time.Now().Add(30*time.Minute)))
		v, ok := c.Get("a")
		require.True(t, ok)
		require.Equal(t, 2, v)
	})

	t.Run("after expiry", func(t *testing.T) {
		setDeadline(t, c, "a", time.Now().Add(-time.Second))
		c.Add("a", 3)

		require.Equal(t, 1, c.Len())
		require.True(t, deadlineOf(t, c, "a").After(time.Now()))
		v, ok := c.Get("a")
		require.True(t, ok)
		require.Equal(t, 3, v)
	})
}

func TestCacheWithTTLGetDoesNotExtendTheDeadline(t *testing.T) {
	c := NewWithTTL[string, int]("no_sliding", 8, time.Hour)
	c.Add("a", 1)
	// Half the ttl away, so a Get that re-stamped the deadline could not land on the same instant.
	setDeadline(t, c, "a", time.Now().Add(30*time.Minute))
	deadline := deadlineOf(t, c, "a")

	_, ok := c.Get("a")
	require.True(t, ok)
	require.True(t, deadlineOf(t, c, "a").Equal(deadline))
}

func TestCacheWithTTLSizeEvictionIsLeastRecentlyUsed(t *testing.T) {
	c := NewWithTTL[string, int]("lru_eviction", 2, time.Hour)
	require.False(t, c.Add("a", 1))
	require.False(t, c.Add("b", 2))

	_, ok := c.Get("a") // a is now the most recently used; b is the oldest
	require.True(t, ok)
	require.True(t, c.Add("c", 3), "adding past the cap evicts")

	require.Equal(t, 2, c.Len())
	require.True(t, resident(c, "a"))
	require.False(t, resident(c, "b"))
	require.True(t, resident(c, "c"))
}

func TestCacheWithTTLSizeCapBoundsExpiredEntriesToo(t *testing.T) {
	c := NewWithTTL[int, int]("cap_bounds_expired", 4, time.Hour)
	for i := range 4 {
		c.Add(i, i)
		setDeadline(t, c, i, time.Now().Add(-time.Second))
	}
	require.Equal(t, 4, c.Len(), "expired entries stay resident until read")

	for i := 4; i < 100; i++ {
		c.Add(i, i)
	}
	require.Equal(t, 4, c.Len(), "the cap, not the ttl, bounds residency")
	for i := range 4 {
		require.False(t, resident(c, i))
	}
}

func TestCacheWithTTLRemove(t *testing.T) {
	c := NewWithTTL[string, int]("remove", 8, time.Hour)
	c.Add("a", 1)

	require.True(t, c.Remove("a"))
	require.False(t, c.Remove("a"))
	require.Equal(t, 0, c.Len())
	_, ok := c.Get("a")
	require.False(t, ok)
}

func TestCacheWithTTLNoTTLNeverExpires(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		c := NewWithTTL[string, int]("no_ttl", 8, ttl)
		c.Add("a", 1)

		require.True(t, deadlineOf(t, c, "a").IsZero())
		c.mu.Lock()
		e, ok := c.cache.Peek("a")
		c.mu.Unlock()
		require.True(t, ok)
		require.False(t, e.expired(time.Now().Add(100*365*24*time.Hour)))

		v, ok := c.Get("a")
		require.True(t, ok)
		require.Equal(t, 1, v)
	}
}

func TestCacheWithTTLInvalidSizePanics(t *testing.T) {
	for _, size := range []int{0, -1} {
		require.Panics(t, func() { NewWithTTL[string, int]("invalid_size", size, time.Hour) })
	}
}

func TestCacheWithTTLMetrics(t *testing.T) {
	// Counters are process-global and keyed by name, so this cache gets its own.
	c := NewWithTTL[string, int]("metrics_"+t.Name(), 8, time.Hour)
	hits, misses := c.metricTTLHit.GetValue(), c.metricTTLMiss.GetValue()

	c.Add("a", 1)
	c.Get("a") // live hit
	require.Equal(t, hits+1, c.metricTTLHit.GetValue())
	require.Equal(t, misses, c.metricTTLMiss.GetValue())

	c.Get("absent") // miss on a key never stored
	require.Equal(t, hits+1, c.metricTTLHit.GetValue())
	require.Equal(t, misses+1, c.metricTTLMiss.GetValue())

	setDeadline(t, c, "a", time.Now().Add(-time.Second))
	c.Get("a") // miss on an expired key
	require.Equal(t, hits+1, c.metricTTLHit.GetValue())
	require.Equal(t, misses+2, c.metricTTLMiss.GetValue())
}

func TestCacheWithTTLConcurrentUse(t *testing.T) {
	c := NewWithTTL[int, int]("concurrent", 32, time.Millisecond)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				k := (g + i) % 64
				switch i % 4 {
				case 0:
					c.Add(k, i)
				case 1:
					c.Get(k)
				case 2:
					c.Remove(k)
				default:
					c.Len()
				}
			}
		})
	}
	wg.Wait()

	require.LessOrEqual(t, c.Len(), 32)
}
