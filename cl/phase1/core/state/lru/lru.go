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

type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func (e ttlEntry[V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// CacheWithTTL is a size- and time-bounded cache. Its expiry sweep runs on a goroutine that
// lives until Close is called, so a cache built per request, per peer or per test must be closed.
type CacheWithTTL[K comparable, V any] struct {
	// The cache is built without a ttl of its own: the cleanup goroutine the dependency starts
	// for an expiring cache cannot be stopped, so expiry is owned here instead.
	cache  *expirable.LRU[K, ttlEntry[V]]
	ttl    time.Duration
	metric string
	// metrics
	metricTTLHit, metricTTLMiss metrics.Counter

	mu        sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
}

func NewWithTTL[K comparable, V any](metricName string, size int, ttl time.Duration) *CacheWithTTL[K, V] {
	c := &CacheWithTTL[K, V]{
		cache:         expirable.NewLRU[K, ttlEntry[V]](size, nil, 0),
		ttl:           ttl,
		metric:        metricName,
		done:          make(chan struct{}),
		metricTTLHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricTTLMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}
	if ttl > 0 {
		go c.sweep(sweepInterval(ttl))
	}
	return c
}

func sweepInterval(ttl time.Duration) time.Duration {
	if interval := ttl / sweepsPerTTL; interval > minSweepInterval {
		return interval
	}
	return minSweepInterval
}

// Close stops the expiry sweep. It is safe to call more than once.
func (c *CacheWithTTL[K, V]) Close() {
	c.closeOnce.Do(func() { close(c.done) })
}

func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	e := ttlEntry[V]{value: v}
	if c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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

// removeExpired drops the expired tail. An entry that a Get moved back to the front outlives the
// sweep and is reclaimed by the Get that finds it expired, or once it reaches the tail again.
func (c *CacheWithTTL[K, V]) removeExpired(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		k, e, ok := c.cache.GetOldest()
		if !ok || !e.expired(now) {
			return
		}
		c.cache.Remove(k)
	}
}
