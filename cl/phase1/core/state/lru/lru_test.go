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

// sweepStopped reports whether the cache's sweep goroutine has returned: it closes stopped on its
// way out, and a cache without a ttl starts none and closes it up front.
func sweepStopped[K comparable, V any](c *CacheWithTTL[K, V]) bool {
	select {
	case <-c.stopped:
		return true
	default:
		return false
	}
}

// deadlineOf reads the deadline the cache stored for a key, so a test can sweep at an exact
// instant rather than race the wall clock.
func deadlineOf[K comparable, V any](t *testing.T, c *CacheWithTTL[K, V], k K) time.Time {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache.Peek(k)
	require.True(t, ok, "no entry held for the key")
	require.NotNil(t, e.node, "entry held without a deadline")
	return e.node.expiresAt
}

// held reports whether the cache still holds a key, read under the lock the sweep takes. Going
// through Get would reclaim an expired entry itself and hide what the sweep did.
func held[K comparable, V any](c *CacheWithTTL[K, V], k K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.cache.Peek(k)
	return ok
}

// expire moves a key's deadline into the past under the cache lock, so a test can hand Get an
// entry that is expired but still linked without racing the sweep. The caches that use it have an
// hour-long ttl, so no tick falls due during the test.
func expire[K comparable, V any](t *testing.T, c *CacheWithTTL[K, V], k K) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache.Peek(k)
	require.True(t, ok, "no entry held for the key")
	require.NotNil(t, e.node, "entry held without a deadline")
	e.node.expiresAt = time.Now().Add(-time.Second)
}

func TestCacheWithTTLCloseStopsSweep(t *testing.T) {
	const caches = 8
	for i := range caches {
		c := NewWithTTL[uint64, uint64]("close_stops_sweep", 16, time.Hour)
		c.Add(uint64(i), uint64(i))
		require.False(t, sweepStopped(c), "the sweep must be running until Close")
		c.Close()
		require.True(t, sweepStopped(c), "Close returned before the sweep goroutine had stopped")
	}
}

func TestCacheWithTTLCloseIsIdempotent(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("close_idempotent", 16, time.Hour)
	c.Close()
	require.NotPanics(t, c.Close)
}

// TestCacheWithTTLCloseWaitsForRunningSweep pins that Close returns only once the sweep goroutine
// has stopped. The sweep is driven by a test-owned tick and held, outside the cache lock, between
// receiving that tick and reclaiming: Close closes done, but the sweep is past its select and has
// to finish the tick before it can see that, so nothing but the wait on stopped can hold Close.
func TestCacheWithTTLCloseWaitsForRunningSweep(t *testing.T) {
	ticks := make(chan time.Time)
	c := newWithTTL[uint64, uint64]("close_waits_for_sweep", 16, time.Hour, ticks)
	c.Add(1, 1)

	entered := make(chan struct{})
	release := make(chan struct{})
	c.mu.Lock()
	c.beforeSweep = func() {
		close(entered)
		<-release
	}
	c.mu.Unlock()

	ticks <- time.Now()
	<-entered

	returned := make(chan struct{})
	go func() {
		c.Close()
		close(returned)
	}()

	select {
	case <-returned:
		t.Fatal("Close returned while the sweep was still running")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-c.stopped:
		t.Fatal("stopped closed while the sweep was still running")
	default:
	}

	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return once the sweep finished")
	}
	select {
	case <-c.stopped:
	default:
		t.Fatal("Close returned before the sweep goroutine had stopped")
	}

	require.NotPanics(t, c.Close, "Close must stay safe to call again after it has waited")
}

// TestCacheWithTTLCloseReturnsWithoutTTL pins that Close does not wait on a sweep that was never
// started: a cache without a ttl has no goroutine to finish.
func TestCacheWithTTLCloseReturnsWithoutTTL(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("close_without_ttl", 16, 0)

	returned := make(chan struct{})
	go func() {
		c.Close()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on a cache that never started a sweep")
	}
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
			c := NewWithTTL[uint64, uint64]("no_ttl_"+tt.name, 16, tt.ttl)
			t.Cleanup(c.Close)
			c.Add(1, 1)

			require.True(t, sweepStopped(c), "a cache without a ttl must start no sweep goroutine")

			v, ok := c.Get(1)
			require.True(t, ok, "an entry in a cache without a ttl must not expire")
			require.Equal(t, uint64(1), v)

			c.removeExpired(time.Now().Add(100 * 365 * 24 * time.Hour))
			require.Equal(t, 1, c.Len(), "an entry without a deadline must survive any sweep")
		})
	}
}

func TestCacheWithTTLGetMissesExpiredEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("get_misses_expired", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 42)
	v, ok := c.Get(1)
	require.True(t, ok)
	require.Equal(t, uint64(42), v)

	expire(t, c, 1)

	_, ok = c.Get(1)
	require.False(t, ok, "an expired entry must read as a miss")
}

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

func TestCacheWithTTLAddRenewsExpiry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("add_renews_expiry", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	first := deadlineOf(t, c, 1)
	time.Sleep(2 * time.Millisecond)
	c.Add(1, 2)
	second := deadlineOf(t, c, 1)
	require.True(t, second.After(first), "re-adding a key must renew its deadline")

	c.removeExpired(first.Add(time.Nanosecond))
	v, ok := c.Get(1)
	require.True(t, ok, "the old deadline must not reclaim a renewed entry")
	require.Equal(t, uint64(2), v)

	c.removeExpired(second.Add(time.Nanosecond))
	require.False(t, held(c, 1))
}

// TestCacheWithTTLAddDeadlineStartsAtInsertion pins that an entry gets its full ttl in the cache.
// Time spent waiting for the cache lock is not the entry's, and charging it there means a long
// enough wait stores a value that is already expired and reads back as a miss.
func TestCacheWithTTLAddDeadlineStartsAtInsertion(t *testing.T) {
	const ttl = time.Hour
	c := NewWithTTL[uint64, uint64]("add_deadline_at_insertion", 16, ttl)
	t.Cleanup(c.Close)

	c.mu.Lock()

	added := make(chan struct{})
	go func() {
		c.Add(1, 1)
		close(added)
	}()

	// Hold the lock long enough that the Add is waiting on it, then note when it was released: a
	// deadline taken before the wait would fall short of released+ttl by the time spent waiting.
	time.Sleep(20 * time.Millisecond)
	released := time.Now()
	c.mu.Unlock()

	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("Add did not complete once the lock was released")
	}

	require.False(t, deadlineOf(t, c, 1).Before(released.Add(ttl)),
		"an entry that waited for the lock must still get its full ttl from the moment it was stored")
}

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

// TestCacheWithTTLSweepLeavesEvictionOrderAlone pins that a sweep which removes nothing leaves the
// cache's choice of eviction victim where it was: the sweep reads the deadline order only and never
// touches an entry's recency, so the oldest entry is still the one size eviction takes.
func TestCacheWithTTLSweepLeavesEvictionOrderAlone(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_leaves_eviction_order", 2, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	c.Add(2, 2)
	c.removeExpired(time.Now())

	require.True(t, c.Add(3, 3), "adding past the size must still report an eviction after a sweep")
	require.False(t, held(c, 1), "the oldest entry must still be the one size eviction takes")
	require.True(t, held(c, 2))
	require.True(t, held(c, 3))
}

func TestCacheWithTTLGetReclaimsExpiredEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("get_reclaims", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	expire(t, c, 1)

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
	require.Equal(t, 10*time.Millisecond, sweepInterval(time.Second),
		"a ttl well above the floor must be swept sweepsPerTTL times over its length")
	require.Equal(t, 2*minSweepInterval, sweepInterval(2*sweepsPerTTL*minSweepInterval))

	// Below the floor the divided interval is shorter than minSweepInterval, and for a short
	// enough ttl it is zero, which time.NewTicker panics on.
	require.Equal(t, minSweepInterval, sweepInterval(sweepsPerTTL*minSweepInterval/2),
		"an interval below the floor must be raised to it")
	require.Equal(t, minSweepInterval, sweepInterval(time.Nanosecond))
	require.Positive(t, sweepInterval(0), "the ticker interval must never be non-positive")
}

// TestCacheWithTTLSweepReclaimsInteriorEntry pins that reclamation follows deadline order and not
// eviction order: a Get on the middle entry leaves the eviction order oldest-first as 1, 3, 2 while
// the deadline order is still 1, 2, 3, so a sweep between the second and third deadlines removes
// the first two nodes of the deadline order and keeps the third, whatever the eviction order says.
func TestCacheWithTTLSweepReclaimsInteriorEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims_interior", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	time.Sleep(time.Millisecond)
	c.Add(2, 2)
	time.Sleep(time.Millisecond)
	c.Add(3, 3)

	second := deadlineOf(t, c, 2)
	require.True(t, second.After(deadlineOf(t, c, 1)))
	require.True(t, deadlineOf(t, c, 3).After(second))

	_, ok := c.Get(2)
	require.True(t, ok)

	c.removeExpired(second.Add(time.Nanosecond))

	require.False(t, held(c, 1), "the oldest expired entry must be reclaimed")
	require.False(t, held(c, 2), "an expired entry behind a live one must still be reached")
	require.True(t, held(c, 3), "the entry within its deadline must be kept")
}

// TestCacheWithTTLSweepReclaimsExpiredTail is the reverse order of the case above: the newest
// entry is the promoted one and the expired entry is the oldest, so deadline order and eviction
// order agree. The case has to keep holding whichever order the entries were added in.
func TestCacheWithTTLSweepReclaimsExpiredTail(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims_tail", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	first := deadlineOf(t, c, 1)
	time.Sleep(time.Millisecond)
	c.Add(2, 2)

	_, ok := c.Get(2)
	require.True(t, ok)

	c.removeExpired(first.Add(time.Nanosecond))

	require.False(t, held(c, 1))
	require.True(t, held(c, 2))
}

// TestCacheWithTTLAddAfterCloseIsNotSwept pins what Close means for writes: it stops the sweep, so
// an entry added afterwards is still held and still reads as a miss once past its deadline, but
// nothing reclaims it in the background.
func TestCacheWithTTLAddAfterCloseIsNotSwept(t *testing.T) {
	const ttl = 10 * time.Millisecond
	c := NewWithTTL[uint64, uint64]("add_after_close", 16, ttl)
	c.Close()

	c.Add(1, 1)
	time.Sleep(10 * sweepInterval(ttl))

	require.True(t, held(c, 1), "nothing sweeps a closed cache")
	_, ok := c.Get(1)
	require.False(t, ok, "an expired entry still reads as a miss")
	require.Equal(t, 0, c.Len(), "the read is what reclaims it")
}

// TestCacheWithTTLShortTTLSweeps drives the ticker itself at a ttl far below the sweep cadence:
// the interval floor has to keep it positive and the sweep has to run and reclaim. On the
// dependency's own cadence the same ttl divides to a non-positive interval, which panics.
func TestCacheWithTTLShortTTLSweeps(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("short_ttl", 16, time.Nanosecond)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	require.Eventually(t, func() bool { return c.Len() == 0 }, 5*time.Second, time.Millisecond,
		"a ttl below the sweep cadence must still be swept")
}

// TestCacheWithTTLConcurrentUseDuringSweep runs readers and writers against a live sweep. Every
// exported method takes the lock the sweep takes, so under -race this pins the locking the sweep
// depends on, including that the expiry order is only ever touched under the cache's own lock.
func TestCacheWithTTLConcurrentUseDuringSweep(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("concurrent_use_during_sweep", 128, 5*time.Millisecond)
	t.Cleanup(c.Close)

	const workers = 4
	const ops = 200

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range ops {
				k := uint64(w*ops + i)
				c.Add(k, k)
				c.Get(k)
				c.Len()
				c.Remove(k)
			}
		})
	}
	wg.Wait()
}

// TestCacheWithTTLTickerReclaimsPromotedEntry drives the promoted-entry case through the ticker
// rather than by calling removeExpired directly, which is the only path production code ever
// takes: nothing outside this package calls removeExpired. The ordering between the two entries
// is pinned by TestCacheWithTTLSweepReclaimsPromotedEntry at controlled deadlines; here only
// eventual reclamation is asserted, so a scheduler pause cannot fail correct code.
func TestCacheWithTTLTickerReclaimsPromotedEntry(t *testing.T) {
	const ttl = 200 * time.Millisecond
	c := NewWithTTL[uint64, uint64]("ticker_reclaims_promoted", 16, ttl)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	c.Add(2, 2)

	// A Get promotes the older entry to the front without renewing it.
	_, ok := c.Get(1)
	require.True(t, ok)

	require.Eventually(t, func() bool { return c.Len() == 0 }, 10*time.Second, 5*time.Millisecond,
		"the background sweep must reclaim a promoted entry as well as the one behind it")
}

// TestCacheWithTTLCloseDuringLiveSweep races Close against the sweep goroutine it stops: the
// ticker fires on the interval floor here, so the close lands while a sweep is running or between
// two of its ticks. Under -race this pins that the guarded close, the wait for the goroutine and
// the sweep's own select do not race each other, from several callers at once.
func TestCacheWithTTLCloseDuringLiveSweep(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("close_during_sweep", 64, 5*time.Millisecond)

	for i := range 64 {
		c.Add(uint64(i), uint64(i))
	}

	// Panics are collected rather than asserted in the workers: a failed require in a goroutine
	// that is not the test's is not something testing supports.
	const closers = 4
	panics := make(chan any, closers)
	var wg sync.WaitGroup
	for range closers {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					panics <- r
				}
			}()
			c.Close()
		})
	}
	wg.Wait()
	close(panics)
	for r := range panics {
		t.Fatalf("closing a cache whose sweep is running must be safe, got panic: %v", r)
	}
	require.True(t, sweepStopped(c))

	// The cache is still usable once its sweep is stopped, just no longer reclaimed in background.
	// Peek rather than Get: with a 5 ms ttl a Get right after the Add would race the deadline, and
	// Len is whatever the sweep left before it stopped.
	c.Add(1, 1)
	require.True(t, held(c, 1), "a closed cache must still store entries")
}

// TestCacheWithTTLSweepReclaimsWholeCacheWithinOneTick pins the reclamation window: every entry
// past its deadline is gone after one sweep, however large the cache. 100,000 entries with a
// 30-minute ttl, swept at 31m30s.
func TestCacheWithTTLSweepReclaimsWholeCacheWithinOneTick(t *testing.T) {
	const entries = 100_000
	c := NewWithTTL[uint64, uint64]("sweep_whole_cache_one_tick", entries, 30*time.Minute)
	t.Cleanup(c.Close)

	for i := range entries {
		c.Add(uint64(i), uint64(i))
	}
	require.Equal(t, entries, c.Len())

	c.removeExpired(time.Now().Add(31*time.Minute + 30*time.Second))
	require.Equal(t, 0, c.Len(), "one sweep must reclaim every expired entry, not a chunk of them")
}

// TestCacheWithTTLSweepIgnoresPromotion pins that a Get between sweeps cannot hide an expired
// entry. Reclamation follows insertion order, which a Get does not change, so what a Get does to
// the eviction list is irrelevant to what the sweep visits.
func TestCacheWithTTLSweepIgnoresPromotion(t *testing.T) {
	const entries = 1025
	c := NewWithTTL[uint64, uint64]("sweep_ignores_promotion", entries, time.Hour)
	t.Cleanup(c.Close)

	// Key 0 is the oldest by a clear margin, so it is the only entry due at first+1ms.
	c.Add(0, 0)
	c.mu.Lock()
	first := c.expiryHead.expiresAt
	c.mu.Unlock()
	time.Sleep(2 * time.Millisecond)
	for i := 1; i < entries; i++ {
		c.Add(uint64(i), uint64(i))
	}

	// A reader promotes a live key in the eviction list between two sweeps.
	_, ok := c.Get(512)
	require.True(t, ok)

	c.removeExpired(first.Add(time.Millisecond))
	require.False(t, held(c, 0), "the oldest entry must be reclaimed whatever a Get did to the eviction order")
	require.Equal(t, entries-1, c.Len())
	require.True(t, held(c, 512))
	require.True(t, held(c, entries-1))
}

// TestCacheWithTTLAddRenewsMovesRecord pins a renewal: the entry lives to its new deadline, and the
// key's one node moves to the tail with that deadline. Nothing is left behind for the old one.
func TestCacheWithTTLAddRenewsMovesRecord(t *testing.T) {
	c := NewWithTTL[string, int]("readd_moves_record", 8, time.Hour)
	t.Cleanup(c.Close)

	c.Add("k", 1)
	c.mu.Lock()
	first := c.expiryHead.expiresAt
	c.mu.Unlock()

	time.Sleep(2 * time.Millisecond)
	c.Add("k", 2)
	require.Equal(t, 1, c.expiryLen(), "a renewal moves the key's node; it does not add one")
	c.mu.Lock()
	second := c.expiryHead.expiresAt
	c.mu.Unlock()
	require.True(t, second.After(first))

	c.removeExpired(first.Add(time.Millisecond))
	v, ok := c.Get("k")
	require.True(t, ok, "the old deadline must not reclaim a renewed entry")
	require.Equal(t, 2, v)

	c.removeExpired(second.Add(time.Nanosecond))
	require.False(t, held(c, "k"))
	require.Equal(t, 0, c.expiryLen())
}

// TestCacheWithTTLRenewalMovesNodeBehindOthers pins the ordering a renewal must keep: the renewed
// key's node goes to the tail, behind entries added after it, so a sweep between the other entry's
// deadline and the renewed one reclaims the other entry and stops at the renewed one.
func TestCacheWithTTLRenewalMovesNodeBehindOthers(t *testing.T) {
	c := NewWithTTL[string, int]("renewal_moves_node_behind_others", 8, time.Hour)
	t.Cleanup(c.Close)

	c.Add("a", 1)
	time.Sleep(2 * time.Millisecond)
	c.Add("b", 2)
	time.Sleep(2 * time.Millisecond)
	c.Add("a", 3)

	require.Equal(t, 2, c.expiryLen(), "one node per live entry, renewal included")
	require.True(t, deadlineOf(t, c, "a").After(deadlineOf(t, c, "b")),
		"the renewed key's deadline must be the later one")

	c.removeExpired(deadlineOf(t, c, "b").Add(time.Nanosecond))
	require.False(t, held(c, "b"), "the entry due first must be reclaimed")
	require.True(t, held(c, "a"), "the renewed entry must survive the sweep that reclaims the other")
	require.Equal(t, 1, c.expiryLen())
	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 3, v)
}

// TestCacheWithTTLSweepBoundsLockHold pins the pacing that remains: one lock hold reclaims at most
// sweepChunk records, and removeExpired keeps taking the lock until nothing due is left, so a large
// burst of expiries is reclaimed in full at this tick without holding readers off for all of it.
func TestCacheWithTTLSweepBoundsLockHold(t *testing.T) {
	const entries = 3*sweepChunk - 36
	c := NewWithTTL[uint64, uint64]("sweep_bounds_lock_hold", entries, time.Hour)
	t.Cleanup(c.Close)

	for i := range entries {
		c.Add(uint64(i), uint64(i))
	}
	past := time.Now().Add(2 * time.Hour)

	require.True(t, c.removeExpiredUpTo(past, sweepChunk), "a full chunk with more due must report more")
	require.Equal(t, entries-sweepChunk, c.Len())
	require.True(t, c.removeExpiredUpTo(past, sweepChunk))
	require.Equal(t, entries-2*sweepChunk, c.Len())
	require.False(t, c.removeExpiredUpTo(past, sweepChunk), "the last hold finds nothing further due")
	require.Equal(t, 0, c.Len())
	require.False(t, c.removeExpiredUpTo(past, sweepChunk), "an empty queue reports nothing to do")
}

// TestCacheWithTTLExpiryBookkeepingBoundedByCache pins that the expiry order holds one node per
// live entry, whatever the rate of Add: a renewal moves a node, and a removal or a size eviction
// unlinks one, so a size-1 cache updated 100,000 times holds one node and a size-64 cache fed
// 10,000 distinct keys holds 64.
func TestCacheWithTTLExpiryBookkeepingBoundedByCache(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("expiry_bounded_renewal", 1, 30*time.Minute)
	t.Cleanup(c.Close)
	for i := range 100_000 {
		c.Add(7, uint64(i))
	}
	require.Equal(t, 1, c.Len())
	require.Equal(t, 1, c.expiryLen(), "100,000 renewals of one key must leave one record")

	d := NewWithTTL[uint64, uint64]("expiry_bounded_eviction", 64, 30*time.Minute)
	t.Cleanup(d.Close)
	for i := range 10_000 {
		d.Add(uint64(i), uint64(i))
	}
	require.Equal(t, 64, d.Len())
	require.Equal(t, 64, d.expiryLen(), "size eviction must unlink the evicted key's record")

	require.True(t, d.Remove(9_999))
	require.Equal(t, 63, d.expiryLen(), "Remove must unlink the key's record")
	require.False(t, d.Remove(9_999), "removing an absent key changes nothing")
	require.Equal(t, 63, d.expiryLen())
}

// TestCacheWithTTLDrainedQueueRetainsNothing pins that once every entry is reclaimed no expiry
// bookkeeping survives: the order is a linked list with no shared backing array, each node is
// collectable the moment it is unlinked, and none is left linked after a burst is swept.
func TestCacheWithTTLDrainedQueueRetainsNothing(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("expiry_drained", 200_000, time.Hour)
	t.Cleanup(c.Close)
	for i := range 115_456 {
		c.Add(uint64(i), uint64(i))
	}
	require.Equal(t, 115_456, c.expiryLen())

	c.removeExpired(time.Now().Add(2 * time.Hour))
	require.Equal(t, 0, c.Len())
	require.Equal(t, 0, c.expiryLen())
	c.mu.Lock()
	head, tail := c.expiryHead, c.expiryTail
	c.mu.Unlock()
	require.Nil(t, head)
	require.Nil(t, tail)
}

// TestCacheWithTTLGetDropsExpiredRecord pins that an expired entry dropped by a Get takes its
// record with it: the sweep is not the only consumer.
func TestCacheWithTTLGetDropsExpiredRecord(t *testing.T) {
	c := NewWithTTL[string, int]("get_drops_record", 8, time.Hour)
	t.Cleanup(c.Close)
	c.Add("k", 1)
	require.Equal(t, 1, c.expiryLen())
	expire(t, c, "k")
	_, ok := c.Get("k")
	require.False(t, ok)
	require.Equal(t, 0, c.Len(), "a Get that finds an entry expired must drop it")
	require.Equal(t, 0, c.expiryLen(), "a Get that drops an expired entry must unlink its record")
}

// TestCacheWithTTLNoRecordsAfterClose pins that after Close nothing consumes the expiry order, so it
// is dropped and churn records nothing, while Add and Remove keep working and a Get still drops an
// expired entry it reads.
func TestCacheWithTTLNoRecordsAfterClose(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("no_records_after_close", 64, time.Hour)
	for i := range 32 {
		c.Add(uint64(i), uint64(i))
	}
	require.Equal(t, 32, c.expiryLen())
	c.Close()
	require.Equal(t, 0, c.expiryLen(), "Close drops the order nothing will consume")

	for i := range 100_000 {
		c.Add(uint64(i%128), uint64(i))
	}
	require.Equal(t, 64, c.Len())
	require.Equal(t, 0, c.expiryLen(), "post-Close churn must record nothing")
	require.True(t, c.Remove(1))
	require.Equal(t, 63, c.Len())

	expire(t, c, 2)
	_, ok := c.Get(2)
	require.False(t, ok, "a closed cache still treats an expired entry as a miss")
	require.Equal(t, 62, c.Len())
}

// TestCacheWithTTLNoTTLKeepsNoRecords pins that a cache without a ttl, which runs no sweep, also
// keeps no expiry queue: nothing is recorded, so nothing can ever be reclaimed by time.
func TestCacheWithTTLNoTTLKeepsNoRecords(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("no_ttl_no_records", 8, 0)
	t.Cleanup(c.Close)
	for i := range 8 {
		c.Add(uint64(i), uint64(i))
	}
	require.Equal(t, 0, c.expiryLen(), "no ttl, no expiry order")
	c.removeExpired(time.Now().Add(100 * 365 * 24 * time.Hour))
	require.Equal(t, 8, c.Len())
}

// TestCacheWithTTLRejectsNonPositiveSize pins that a non-positive size is refused up front, as New
// refuses it, rather than building a cache that can hold nothing.
func TestCacheWithTTLRejectsNonPositiveSize(t *testing.T) {
	require.Panics(t, func() { NewWithTTL[uint64, uint64]("nonpositive_size", 0, time.Second) })
}
