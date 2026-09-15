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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// surviving reports how many goroutines are still running once the runtime has had a chance to
// schedule the ones that are on their way out.
func surviving(t *testing.T) int {
	t.Helper()
	var count int
	for i := 0; i < 100; i++ {
		runtime.Gosched()
		count = runtime.NumGoroutine()
		time.Sleep(time.Millisecond)
	}
	return count
}

// deadlineOf reads the deadline the cache stored for a key, so a test can sweep at an exact
// instant rather than race the wall clock.
func deadlineOf[K comparable, V any](t *testing.T, c *CacheWithTTL[K, V], k K) time.Time {
	t.Helper()
	e, ok := c.cache.Peek(k)
	require.True(t, ok, "no entry held for the key")
	return e.expiresAt
}

func TestCacheWithTTLCloseStopsSweep(t *testing.T) {
	before := surviving(t)

	const caches = 8
	for i := 0; i < caches; i++ {
		c := NewWithTTL[uint64, uint64]("close_stops_sweep", 16, time.Hour)
		c.Add(uint64(i), uint64(i))
		c.Close()
	}

	require.LessOrEqual(t, surviving(t), before, "closed caches left sweep goroutines behind")
}

func TestCacheWithTTLCloseIsIdempotent(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("close_idempotent", 16, time.Hour)
	c.Close()
	require.NotPanics(t, c.Close)
}

func TestCacheWithTTLReadsAfterClose(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("reads_after_close", 16, time.Hour)
	c.Add(1, 1)
	c.Close()

	v, ok := c.Get(1)
	require.True(t, ok, "a closed cache must still serve what it holds")
	require.Equal(t, uint64(1), v)

	c.Add(2, 2)
	v, ok = c.Get(2)
	require.True(t, ok, "closing stops the sweep, not the cache")
	require.Equal(t, uint64(2), v)
}

func TestCacheWithTTLStartsNoSweepWithoutTTL(t *testing.T) {
	for _, tt := range []struct {
		name string
		ttl  time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := surviving(t)

			c := NewWithTTL[uint64, uint64]("no_ttl_"+tt.name, 16, tt.ttl)
			t.Cleanup(c.Close)
			c.Add(1, 1)

			require.LessOrEqual(t, surviving(t), before, "a cache without a ttl started a sweep goroutine")

			v, ok := c.Get(1)
			require.True(t, ok, "an entry in a cache without a ttl must not expire")
			require.Equal(t, uint64(1), v)

			c.removeExpired(time.Now().Add(100 * 365 * 24 * time.Hour))
			require.Equal(t, 1, c.Len(), "an entry without a deadline must survive any sweep")
		})
	}
}

// TestCacheWithTTLGetMissesExpiredEntry (control): read semantics do not depend on the sweep and
// hold both before and after the change.
func TestCacheWithTTLGetMissesExpiredEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("get_misses_expired", 16, 10*time.Millisecond)
	t.Cleanup(c.Close)

	c.Add(1, 42)
	v, ok := c.Get(1)
	require.True(t, ok)
	require.Equal(t, uint64(42), v)

	time.Sleep(50 * time.Millisecond)

	_, ok = c.Get(1)
	require.False(t, ok, "an expired entry must read as a miss")
}

// TestCacheWithTTLEvictsBySize (control): size bounding is unchanged by the switch to an
// owned expiry sweep.
func TestCacheWithTTLEvictsBySize(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("evicts_by_size", 2, time.Hour)
	t.Cleanup(c.Close)

	require.False(t, c.Add(1, 1))
	require.False(t, c.Add(2, 2))
	require.True(t, c.Add(3, 3), "adding past the size must report an eviction")

	require.Equal(t, 2, c.Len())
	_, ok := c.Get(1)
	require.False(t, ok, "the oldest entry must be evicted once the size is exceeded")
}

// TestCacheWithTTLAddRenewsExpiry (control): re-adding a key resets its deadline, as it did
// when the dependency owned expiry.
func TestCacheWithTTLAddRenewsExpiry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("add_renews_expiry", 16, 60*time.Millisecond)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	time.Sleep(40 * time.Millisecond)
	c.Add(1, 2)
	time.Sleep(40 * time.Millisecond)

	v, ok := c.Get(1)
	require.True(t, ok, "re-adding a key must renew its deadline")
	require.Equal(t, uint64(2), v)
}

// TestCacheWithTTLRemove (control): Remove reports whether the key was held, as before.
func TestCacheWithTTLRemove(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("remove", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	require.True(t, c.Remove(1))
	require.False(t, c.Remove(1), "removing a key the cache does not hold must report false")
	require.Equal(t, 0, c.Len())
}

func TestCacheWithTTLSweepReclaimsExpiredEntries(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims", 16, 100*time.Millisecond)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	require.Equal(t, 1, c.Len())

	require.Eventually(t, func() bool { return c.Len() == 0 }, 5*time.Second, 10*time.Millisecond,
		"the sweep must reclaim an expired entry without a Get on it")
}

func TestCacheWithTTLRemoveExpired(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("remove_expired", 16, time.Hour)
	t.Cleanup(c.Close)

	c.removeExpired(time.Now())
	require.Equal(t, 0, c.Len(), "sweeping an empty cache must be a no-op")

	c.Add(1, 1)
	c.Add(2, 2)

	c.removeExpired(time.Now())
	require.Equal(t, 2, c.Len(), "unexpired entries must survive a sweep")

	c.removeExpired(time.Now().Add(2 * time.Hour))
	require.Equal(t, 0, c.Len(), "entries past their deadline must be reclaimed")
}

// TestCacheWithTTLSweepReclaimsPromotedEntry pins the case the eviction order hides: a Get moves an
// entry to the front without renewing its deadline, so it expires while a newer, still live entry
// sits behind it at the tail. The sweep has to reach it wherever it sits.
func TestCacheWithTTLSweepReclaimsPromotedEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims_promoted", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	first := deadlineOf(t, c, 1)
	// Separate the two deadlines by more than any clock's resolution, so the instant swept at
	// below is unambiguously between them.
	time.Sleep(time.Millisecond)
	c.Add(2, 2)
	require.True(t, deadlineOf(t, c, 2).After(first), "the later entry must hold the later deadline")

	// A Get promotes the older entry to the front, leaving the newer, still live one at the tail.
	_, ok := c.Get(1)
	require.True(t, ok)

	// Past the promoted entry's deadline, well before the other's.
	c.removeExpired(first.Add(time.Nanosecond))

	require.Equal(t, 1, c.Len(), "a promoted entry past its deadline must still be reclaimed")
	_, ok = c.Get(1)
	require.False(t, ok, "the reclaimed entry must be the expired one")
	_, ok = c.Get(2)
	require.True(t, ok, "the entry still within its deadline must be kept")
}

// TestCacheWithTTLSweepKeepsRecency (control): the sweep reads every entry it walks, and reading
// must not count as use, or a sweep would keep the tail alive and break size eviction.
func TestCacheWithTTLSweepKeepsRecency(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_keeps_recency", 2, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	c.Add(2, 2)
	c.removeExpired(time.Now())

	require.True(t, c.Add(3, 3), "adding past the size must still report an eviction after a sweep")
	_, ok := c.Get(1)
	require.False(t, ok, "a sweep must not make the oldest entry look recently used")
}

func TestCacheWithTTLGetReclaimsExpiredEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("get_reclaims", 16, 10*time.Millisecond)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	time.Sleep(50 * time.Millisecond)

	_, ok := c.Get(1)
	require.False(t, ok)
	require.Equal(t, 0, c.Len(), "a Get that finds an entry expired must drop it")
}

func TestCacheWithTTLExpiryAtExactDeadline(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("expiry_at_exact_deadline", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	deadline := deadlineOf(t, c, 1)

	c.removeExpired(deadline.Add(-time.Nanosecond))
	require.Equal(t, 1, c.Len(), "one tick before its deadline an entry must live")

	c.removeExpired(deadline)
	require.Equal(t, 1, c.Len(), "at exactly its deadline an entry must live")

	c.removeExpired(deadline.Add(time.Nanosecond))
	require.Equal(t, 0, c.Len(), "one tick past its deadline an entry must be reclaimed")
}

func TestCacheWithTTLPointerValueZeroOnMiss(t *testing.T) {
	type entry struct{ n int }
	c := NewWithTTL[uint64, *entry]("pointer_value_zero_on_miss", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, &entry{n: 7})
	hit, ok := c.Get(1)
	require.True(t, ok)
	require.NotNil(t, hit)
	require.Equal(t, 7, hit.n)

	miss, ok := c.Get(2)
	require.False(t, ok)
	require.Nil(t, miss, "a miss on a pointer value must read as nil")
}

func TestSweepInterval(t *testing.T) {
	require.Equal(t, 10*time.Millisecond, sweepInterval(time.Second))
	require.Equal(t, minSweepInterval, sweepInterval(sweepsPerTTL*minSweepInterval),
		"an interval equal to the floor must not be used")
	require.Equal(t, 2*minSweepInterval, sweepInterval(2*sweepsPerTTL*minSweepInterval),
		"an interval one step past the floor must be used")
	require.Equal(t, minSweepInterval, sweepInterval(time.Nanosecond),
		"a ttl too short to divide must not produce a non-positive ticker interval")
}
