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

// TestCacheWithTTLSweepReclaimsInteriorEntry drives the same walk as the promoted-entry test from
// the other side: with three entries the sweep has to keep going past a live entry, not just reach
// the first one. A Get on the middle entry leaves the eviction order oldest-first as 1, 3, 2 while
// the deadline order is still 1, 2, 3, so a sweep between the second and third deadlines has to
// remove the oldest entry, step over the live one behind it and still reclaim the promoted one.
func TestCacheWithTTLSweepReclaimsInteriorEntry(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims_interior", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	time.Sleep(time.Millisecond)
	c.Add(2, 2)
	time.Sleep(time.Millisecond)
	c.Add(3, 3)

	second := deadlineOf(t, c, 2)
	require.True(t, second.After(deadlineOf(t, c, 1)), "the deadlines must be ordered by insertion")
	require.True(t, deadlineOf(t, c, 3).After(second), "the deadlines must be ordered by insertion")

	// Promoting the middle entry puts a live entry between the two expired ones.
	_, ok := c.Get(2)
	require.True(t, ok)

	c.removeExpired(second.Add(time.Nanosecond))

	require.Equal(t, 1, c.Len(), "a sweep must not stop at the first live entry it walks")
	_, ok = c.Get(1)
	require.False(t, ok, "the oldest entry was past its deadline")
	_, ok = c.Get(2)
	require.False(t, ok, "the promoted entry was past its deadline")
	_, ok = c.Get(3)
	require.True(t, ok, "the entry still within its deadline must be kept")
}

// TestCacheWithTTLSweepReclaimsExpiredTail (control): the reverse order of the case above — the
// newest entry is the promoted one and the expired entry is the oldest, so deadline order and
// eviction order agree. A tail-draining sweep reclaims this one too, so the case must keep holding
// whichever way the walk runs.
func TestCacheWithTTLSweepReclaimsExpiredTail(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("sweep_reclaims_tail", 16, time.Hour)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	first := deadlineOf(t, c, 1)
	time.Sleep(time.Millisecond)
	c.Add(2, 2)

	// Promoting the newer entry leaves the expired one at the tail, where any sweep reaches it.
	_, ok := c.Get(2)
	require.True(t, ok)

	c.removeExpired(first.Add(time.Nanosecond))

	require.Equal(t, 1, c.Len())
	_, ok = c.Get(1)
	require.False(t, ok, "an expired entry at the tail must be reclaimed")
	_, ok = c.Get(2)
	require.True(t, ok)
}

// TestCacheWithTTLAddAfterCloseIsNotSwept pins what Close means for writes: it stops the sweep, so
// an entry added afterwards is still held and still read as a miss once past its deadline, but
// nothing reclaims it in the background. The behaviour is a consequence of Close existing at all,
// so it cannot be stated against a cache that has no Close.
func TestCacheWithTTLAddAfterCloseIsNotSwept(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("add_after_close", 16, 10*time.Millisecond)
	c.Close()

	c.Add(1, 1)
	require.Equal(t, 1, c.Len())

	// Several sweep intervals' worth of a cache whose sweep is stopped.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, c.Len(), "a closed cache must not reclaim in the background")

	_, ok := c.Get(1)
	require.False(t, ok, "the entry is still past its deadline, so a read must miss")
	require.Equal(t, 0, c.Len(), "the read is what reclaims it once the sweep is stopped")
}

// TestCacheWithTTLShortTTLSweeps drives the ticker itself at a ttl far below the sweep cadence:
// the interval floor has to keep it positive and the sweep has to run and reclaim. On the
// dependency's own cadence the same ttl divides to a non-positive interval and panics, which is why
// the floor is here.
func TestCacheWithTTLShortTTLSweeps(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("short_ttl", 16, time.Nanosecond)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	require.Eventually(t, func() bool { return c.Len() == 0 }, 5*time.Second, time.Millisecond,
		"a ttl shorter than the sweep cadence must still sweep")
}

// TestCacheWithTTLConcurrentUseDuringSweep (control): every exported method takes the same lock the
// sweep holds, so readers and writers running against a live sweep stay consistent. Run under -race
// this pins the locking the owned sweep depends on.
func TestCacheWithTTLConcurrentUseDuringSweep(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("concurrent_use", 64, 5*time.Millisecond)
	t.Cleanup(c.Close)

	const workers = 4
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 200 {
				k := uint64(w*200 + i)
				c.Add(k, k)
				c.Get(k)
				c.Len()
				c.Remove(k)
			}
		}(w)
	}
	wg.Wait()

	c.removeExpired(time.Now().Add(time.Hour))
	require.Equal(t, 0, c.Len(), "every key written was removed or swept")
}

// TestCacheWithTTLTickerReclaimsPromotedEntry drives the promoted-entry case through the ticker
// rather than by calling removeExpired directly, which is the only path production code ever takes:
// nothing outside this package calls removeExpired, so a sweep that stops at the first live entry
// has to be caught through the goroutine NewWithTTL starts. The entries are spaced so there is a
// full second in which the promoted entry is past its deadline and the entry behind it is not.
func TestCacheWithTTLTickerReclaimsPromotedEntry(t *testing.T) {
	const ttl = 2 * time.Second
	c := NewWithTTL[uint64, uint64]("ticker_reclaims_promoted", 16, ttl)
	t.Cleanup(c.Close)

	c.Add(1, 1)
	time.Sleep(ttl / 2)
	c.Add(2, 2)

	// A Get promotes the older entry to the front without renewing it, so the still live entry is
	// the one the eviction order reaches first.
	_, ok := c.Get(1)
	require.True(t, ok)

	// Between the two deadlines the sweep must have reclaimed the promoted entry and nothing else.
	require.Eventually(t, func() bool { return c.Len() == 1 }, ttl, 5*time.Millisecond,
		"the background sweep must reclaim a promoted entry it has to walk past a live one to reach")

	// Peek under the lock the sweep holds: reading the entries directly would otherwise race it,
	// and going through Get would reclaim the expired entry itself and hide what the sweep did.
	c.mu.Lock()
	_, held1 := c.cache.Peek(1)
	_, held2 := c.cache.Peek(2)
	c.mu.Unlock()

	require.False(t, held1, "the promoted entry was past its deadline when the sweep ran")
	require.True(t, held2, "the entry still within its deadline must survive the sweep")
}

// TestCacheWithTTLCloseDuringLiveSweep (control): Close races the sweep goroutine it stops — the
// ticker fires on the interval floor here, so close(done) lands while the walk is running or
// between two of its ticks. Run under -race this pins that the once-guarded close and the sweep's
// select do not race, and that a closed cache stays readable.
func TestCacheWithTTLCloseDuringLiveSweep(t *testing.T) {
	c := NewWithTTL[uint64, uint64]("close_during_sweep", 64, 5*time.Millisecond)

	for i := range 64 {
		c.Add(uint64(i), uint64(i))
	}

	const closers = 4
	var wg sync.WaitGroup
	for range closers {
		wg.Go(func() {
			require.NotPanics(t, c.Close, "closing a cache whose sweep is running must be safe")
		})
	}
	wg.Wait()

	// The cache is still usable once its sweep is stopped, just no longer reclaimed in background.
	c.Add(1, 1)
	v, ok := c.Get(1)
	require.True(t, ok, "a closed cache must still serve reads")
	require.Equal(t, uint64(1), v)
}
