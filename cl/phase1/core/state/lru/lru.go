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

// sweepChunk caps how many entries one tick inspects before dropping the lock, so the sweep never
// blocks readers for a whole walk. A cache larger than this is covered across consecutive ticks:
// the walk resumes where it stopped rather than restarting, so every entry is still reached.
const sweepChunk = 512

type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func (e ttlEntry[V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// CacheWithTTL is a size- and time-bounded cache whose expiry sweep can be stopped. The sweep runs
// on a goroutine owned by the cache, and Close stops it and waits for it to finish. A cache that
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
	// sweepCursor is the key the next chunk resumes from, and holds only between the ticks of one
	// walk. It is read and written under mu.
	sweepCursor K
	sweeping    bool

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

// Close stops the expiry sweep and waits for it to finish, so no entry is reclaimed once Close has
// returned. It is safe to call more than once and from several goroutines at once.
func (c *CacheWithTTL[K, V]) Close() {
	c.closeOnce.Do(func() { close(c.done) })
	<-c.stopped
}

func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The deadline is taken under the lock: time spent waiting for it would otherwise be charged
	// to the entry's ttl, and a long enough wait would store a value that is already expired.
	e := ttlEntry[V]{value: v}
	if c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
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

// removeExpired drops entries past their deadline, inspecting at most sweepChunk of them. Deadlines
// are not ordered by position: a Get moves an entry to the front of the eviction list without
// renewing it, so an expired entry can sit anywhere and the whole key set has to be covered. A
// cache too large for one chunk is covered over the ticks that follow, each resuming from the key
// the last one stopped at.
func (c *CacheWithTTL[K, V]) removeExpired(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Keys is a snapshot, oldest first, so removing while walking it is safe.
	keys := c.cache.Keys()
	start := 0
	if c.sweeping {
		// Resume at the cursor. If the key it named is gone the walk restarts, which costs a
		// repeat of entries already inspected this pass, never a missed one.
		for i, k := range keys {
			if k == c.sweepCursor {
				start = i
				break
			}
		}
	}

	end := min(start+sweepChunk, len(keys))
	for _, k := range keys[start:end] {
		// Peek reads an entry without moving it, so a sweep does not make anything look
		// recently used and size eviction still reaches the genuinely oldest entry.
		if e, ok := c.cache.Peek(k); ok && e.expired(now) {
			c.cache.Remove(k)
		}
	}

	if end < len(keys) {
		c.sweepCursor, c.sweeping = keys[end], true
		return
	}
	var zero K
	c.sweepCursor, c.sweeping = zero, false
}
