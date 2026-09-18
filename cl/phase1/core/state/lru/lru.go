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

package lru

import (
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// Cache is a wrapper around hashicorp lru but with metric for Get
type Cache[K comparable, V any] struct {
	*lru.Cache[K, V]
	metricName string
	// metrics
	metricHit, metricMiss metrics.Counter
}

func New[K comparable, V any](metricName string, size int) (*Cache[K, V], error) {
	v, err := lru.NewWithEvict[K, V](size, nil)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{
		Cache:      v,
		metricName: metricName,
		metricHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}, nil
}

func (c *Cache[K, V]) Get(k K) (V, bool) {
	v, ok := c.Cache.Get(k)
	if ok {
		c.metricHit.Inc()
	} else {
		c.metricMiss.Inc()
	}
	return v, ok
}

// sweepsPerTTL matches the cleanup cadence of the expirable cache this used to rely on.
const sweepsPerTTL = 100

// minSweepInterval keeps a very short ttl from producing a non-positive ticker interval.
const minSweepInterval = time.Millisecond

// sweepChunk caps how many expired entries one lock hold reclaims. A sweep keeps taking the lock
// until nothing due is left, so the cap bounds how long readers wait, not how much a tick reclaims.
const sweepChunk = 512

// expiryCompactMin is the queue capacity below which spent records are not worth reclaiming.
const expiryCompactMin = 1024

// ttlEntry is what the cache stores. expiresAt is set by Add and by nothing else: the expiry queue
// records the same deadline, and the two are compared to tell a live record from a stale one.
type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func (e ttlEntry[V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// expiryRecord is one Add, in the order it happened.
type expiryRecord[K comparable] struct {
	key       K
	expiresAt time.Time
}

// CacheWithTTL is a size- and time-bounded cache whose expiry sweep can be stopped. The sweep runs
// on a goroutine owned by the cache; Close stops it and waits for it to finish. A cache that
// outlives the process needs no Close; one built per request, per peer or per test does.
type CacheWithTTL[K comparable, V any] struct {
	// The cache is built without a ttl of its own: the cleanup goroutine the dependency starts
	// for an expiring cache cannot be stopped, so expiry is owned here instead.
	cache  *expirable.LRU[K, ttlEntry[V]]
	ttl    time.Duration
	metric string
	// metrics
	metricTTLHit, metricTTLMiss metrics.Counter

	mu sync.Mutex
	// expiry holds one record per Add, oldest first. Every entry shares the one ttl and Add stamps
	// its deadline under mu, so deadlines are non-decreasing along the queue and the head is always
	// the entry due soonest. A sweep pops from the head until it meets a deadline still in the
	// future and never looks past it: its cost is the number of entries due, not the size of the
	// cache, and a Get, which moves an entry in the eviction list but not here, cannot hide one.
	// A record outlives its entry when the key is renewed, removed or evicted by size; it is then
	// skipped when its deadline comes, since the entry it described is gone or carries a later
	// deadline. If entries ever get individual ttls this must become a heap.
	expiry []expiryRecord[K]

	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
}

func NewWithTTL[K comparable, V any](metricName string, size int, ttl time.Duration) *CacheWithTTL[K, V] {
	c := &CacheWithTTL[K, V]{
		cache:         expirable.NewLRU[K, ttlEntry[V]](size, nil, 0),
		ttl:           ttl,
		metric:        metricName,
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
		metricTTLHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricTTLMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}
	if ttl > 0 {
		go c.sweep(sweepInterval(ttl))
	} else {
		// No sweep runs, so nothing will ever close stopped: Close must not wait on it.
		close(c.stopped)
	}
	return c
}

func sweepInterval(ttl time.Duration) time.Duration {
	if interval := ttl / sweepsPerTTL; interval > minSweepInterval {
		return interval
	}
	return minSweepInterval
}

// Close stops the background sweep and waits for it to finish, so the sweep reclaims nothing once
// Close has returned. Get still treats an entry past its deadline as a miss and drops it; only the
// sweep stops. Close is safe to call more than once and from several goroutines at once.
func (c *CacheWithTTL[K, V]) Close() {
	c.closeOnce.Do(func() { close(c.done) })
	<-c.stopped
}

func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The deadline is taken under the lock: time spent waiting for it would otherwise be charged
	// to the entry's ttl, and a long enough wait would store a value that is already expired.
	// Taking it under the lock is also what keeps the expiry queue ordered.
	e := ttlEntry[V]{value: v}
	if c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
		c.expiry = append(c.expiry, expiryRecord[K]{key: k, expiresAt: e.expiresAt})
	}
	return c.cache.Add(k, e)
}

func (c *CacheWithTTL[K, V]) Get(k K) (V, bool) {
	c.mu.Lock()
	e, ok := c.cache.Get(k)
	if ok && e.expired(time.Now()) {
		c.cache.Remove(k)
		ok = false
	}
	c.mu.Unlock()

	if !ok {
		c.metricTTLMiss.Inc()
		var zero V
		return zero, false
	}
	c.metricTTLHit.Inc()
	return e.value, true
}

func (c *CacheWithTTL[K, V]) Remove(k K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Remove(k)
}

func (c *CacheWithTTL[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Len()
}

func (c *CacheWithTTL[K, V]) sweep(interval time.Duration) {
	defer close(c.stopped)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.removeExpired(time.Now())
		}
	}
}

// removeExpired reclaims every entry past its deadline as of now. The work is done in lock holds of
// at most sweepChunk records each, so a large burst of expiries is reclaimed in full at this tick
// without holding readers off for the whole burst.
func (c *CacheWithTTL[K, V]) removeExpired(now time.Time) {
	for c.removeExpiredUpTo(now, sweepChunk) {
	}
}

// removeExpiredUpTo pops up to limit due records off the expiry queue under one lock hold and drops
// the entries they still describe. It reports whether it stopped at the limit with more still due.
func (c *CacheWithTTL[K, V]) removeExpiredUpTo(now time.Time, limit int) (more bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for i < len(c.expiry) && i < limit {
		rec := c.expiry[i]
		if !now.After(rec.expiresAt) {
			// The head is not due, and nothing behind it is either.
			break
		}
		i++
		// Peek reads an entry without moving it, so a sweep does not make anything look recently
		// used and size eviction still reaches the genuinely oldest entry. A deadline that differs
		// from the record's means the key was renewed after this record was written: the newer
		// record, later in the queue, owns it now.
		if e, ok := c.cache.Peek(rec.key); ok && e.expiresAt.Equal(rec.expiresAt) {
			c.cache.Remove(rec.key)
		}
	}
	if i == 0 {
		return false
	}
	more = i == limit && i < len(c.expiry) && now.After(c.expiry[i].expiresAt)

	// Release the consumed prefix. Re-slicing is O(1) but keeps the backing array; once most of it
	// is spent, copy the live tail into a right-sized array so a drained queue does not hold the
	// memory of everything ever added.
	c.expiry = c.expiry[i:]
	if cap(c.expiry) > 2*len(c.expiry)+expiryCompactMin {
		c.expiry = append(make([]expiryRecord[K], 0, len(c.expiry)), c.expiry...)
	}
	return more
}
