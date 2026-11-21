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
	"github.com/hashicorp/golang-lru/v2/simplelru"

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

// ttlEntry is what the cache stores: the value and its deadline, zero when the cache has no ttl.
type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func (e ttlEntry[V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// expiredTailDrops is how many expired entries an Add reclaims from the least recently used end.
// Two per Add keeps ahead of the one entry an Add inserts, so a backlog of expired entries drains
// while the cache is written to.
const expiredTailDrops = 2

// CacheWithTTL is a size-bounded cache with lazy expiry. For a positive ttl, Add sets a fresh
// deadline and Get treats an entry past its deadline as a miss and removes it. An entry that is
// never read again reaches the least recently used end in deadline order, where an Add reclaims
// it, so a cache that is written to holds roughly a ttl's worth of entries rather than filling to
// its size cap. An entry a Get promoted is reclaimed once everything less recently used than it
// has gone, and the size cap bounds it until then. A ttl of zero or less disables expiry. The
// cache starts no goroutine.
type CacheWithTTL[K comparable, V any] struct {
	ttl time.Duration
	// metrics
	metricTTLHit, metricTTLMiss metrics.Counter

	mu sync.Mutex
	// cache is not goroutine-safe on its own; every access is under mu.
	cache *simplelru.LRU[K, ttlEntry[V]]
}

// NewWithTTL builds a cache of at most size entries that each live for ttl after their last Add. A
// ttl of zero or less disables expiry. size must be positive, as for New; an invalid size panics.
func NewWithTTL[K comparable, V any](metricName string, size int, ttl time.Duration) *CacheWithTTL[K, V] {
	cache, err := simplelru.NewLRU[K, ttlEntry[V]](size, nil)
	if err != nil {
		panic(fmt.Sprintf("lru: NewWithTTL(%q, %d, %v): %v", metricName, size, ttl, err))
	}
	return &CacheWithTTL[K, V]{
		ttl:           ttl,
		cache:         cache,
		metricTTLHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricTTLMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}
}

// Add stores v under k with a fresh deadline, replacing any entry k had, expired or not, and
// reclaims expired entries from the least recently used end.
func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := ttlEntry[V]{value: v}
	if c.ttl > 0 {
		// The deadline is taken under the lock: time spent waiting for it would otherwise be
		// charged to the entry's ttl, and a long enough wait would store a value that is already
		// expired.
		now := time.Now()
		e.expiresAt = now.Add(c.ttl)
		c.dropExpiredTail(now)
	}
	return c.cache.Add(k, e)
}

// dropExpiredTail removes up to expiredTailDrops expired entries from the least recently used end,
// stopping at the first live one. c.mu is held.
func (c *CacheWithTTL[K, V]) dropExpiredTail(now time.Time) {
	for range expiredTailDrops {
		k, e, ok := c.cache.GetOldest()
		if !ok || !e.expired(now) {
			return
		}
		c.cache.Remove(k)
	}
}

// Get returns k's value if it is present and not past its deadline. An expired entry is a miss and
// is removed. A hit does not extend the deadline.
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

// Len counts the resident entries, including any past their deadline that no Get or Add has
// reclaimed yet.
func (c *CacheWithTTL[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Len()
}
