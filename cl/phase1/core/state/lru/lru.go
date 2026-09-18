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

// sweepsPerTTL sets the nominal sweep cadence, ttl/100 floored at minSweepInterval: how often the
// sweep looks, not a bound on when an entry is reclaimed.
const sweepsPerTTL = 100

// minSweepInterval keeps a very short ttl from producing a non-positive ticker interval.
const minSweepInterval = time.Millisecond

// sweepChunk caps how many expired entries one lock hold reclaims. A sweep keeps taking the lock
// until nothing due is left, so the cap limits the work per lock acquisition, not how much a tick
// reclaims.
const sweepChunk = 512

// expiryNode is one live entry's place in the expiry order, linked only while the entry is live and
// the cache is open.
type expiryNode[K comparable] struct {
	key        K
	expiresAt  time.Time
	prev, next *expiryNode[K]
}

// ttlEntry is what the cache stores: the value and the entry's expiry node, nil when the cache has
// no ttl. The node carries the deadline, so entry and expiry order can never disagree.
type ttlEntry[K comparable, V any] struct {
	value V
	node  *expiryNode[K]
}

func (e ttlEntry[K, V]) expired(now time.Time) bool {
	return e.node != nil && now.After(e.node.expiresAt)
}

// CacheWithTTL is a size- and time-bounded cache whose expiry sweep can be stopped. The sweep runs
// on a goroutine owned by the cache; Close stops it and waits for it to finish. A cache that lives
// for the whole process need not be closed; one built per request, per peer or per test must be.
type CacheWithTTL[K comparable, V any] struct {
	ttl    time.Duration
	metric string
	// metrics
	metricTTLHit, metricTTLMiss metrics.Counter

	mu sync.Mutex
	// cache is not goroutine-safe on its own; every access is under mu.
	cache *simplelru.LRU[K, ttlEntry[K, V]]
	// The expiry order: one node per live entry, sorted by deadline. Add stamps deadlines under mu
	// with the cache's one ttl, so appending keeps the order sorted; a renewal moves the key's node
	// to the tail; the eviction hook unlinks a node when its entry is removed or evicted by size.
	// The sweep pops due nodes from the head and stops at the first live one.
	expiryHead, expiryTail *expiryNode[K]
	// After Close nothing consumes the list, so it is dropped and no node is linked again.
	closed bool

	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
}

// NewWithTTL builds a cache of at most size entries that each live for ttl after their last Add. A
// ttl of zero or less disables expiry. size must be positive, as for New; an invalid size panics.
func NewWithTTL[K comparable, V any](metricName string, size int, ttl time.Duration) *CacheWithTTL[K, V] {
	c := &CacheWithTTL[K, V]{
		ttl:           ttl,
		metric:        metricName,
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
		metricTTLHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricTTLMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}
	// The hook runs synchronously on the goroutine that called into the cache, which already
	// holds c.mu on every path that can evict. It touches only the expiry order and never calls
	// back into the cache.
	cache, err := simplelru.NewLRU[K, ttlEntry[K, V]](size, func(_ K, e ttlEntry[K, V]) { c.detach(e.node) })
	if err != nil {
		panic(fmt.Sprintf("lru: NewWithTTL(%q, %d, %v): %v", metricName, size, ttl, err))
	}
	c.cache = cache
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
// sweep stops, and with it the expiry order, which nothing would consume. Close is safe to call
// more than once and from several goroutines at once.
func (c *CacheWithTTL[K, V]) Close() {
	c.mu.Lock()
	c.closed = true
	for n := c.expiryHead; n != nil; {
		next := n.next
		n.prev, n.next = nil, nil
		n = next
	}
	c.expiryHead, c.expiryTail = nil, nil
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.done) })
	<-c.stopped
}

func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := ttlEntry[K, V]{value: v}
	if c.ttl > 0 {
		// The deadline is taken under the lock: time spent waiting for it would otherwise be
		// charged to the entry's ttl, and a long enough wait would store a value that is already
		// expired. Taking it under the lock is also what keeps the expiry order sorted.
		// A renewal keeps the key's node and moves it; the cache updates the value in place
		// without calling the hook, so the node is ours to move.
		if old, ok := c.cache.Peek(k); ok && old.node != nil {
			e.node = old.node
			c.detach(e.node)
		} else {
			e.node = &expiryNode[K]{key: k}
		}
		e.node.expiresAt = time.Now().Add(c.ttl)
	}
	// A size eviction inside Add unlinks the evicted key through the hook; it can never be k.
	evicted = c.cache.Add(k, e)
	if e.node != nil && !c.closed {
		c.link(e.node)
	}
	return evicted
}

func (c *CacheWithTTL[K, V]) Get(k K) (V, bool) {
	c.mu.Lock()
	e, ok := c.cache.Get(k)
	if ok && e.expired(time.Now()) {
		c.cache.Remove(k) // the hook unlinks it
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
	return c.cache.Remove(k) // the hook unlinks it
}

func (c *CacheWithTTL[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Len()
}

// link appends a detached node at the tail of the expiry order. Called under mu.
func (c *CacheWithTTL[K, V]) link(n *expiryNode[K]) {
	n.prev, n.next = c.expiryTail, nil
	if c.expiryTail != nil {
		c.expiryTail.next = n
	} else {
		c.expiryHead = n
	}
	c.expiryTail = n
}

// detach splices n out of the expiry order if it is linked; a nil or already-detached node is a
// no-op. Called under mu, directly or through the eviction hook.
func (c *CacheWithTTL[K, V]) detach(n *expiryNode[K]) {
	if n == nil || (n.prev == nil && n.next == nil && c.expiryHead != n) {
		return
	}
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		c.expiryHead = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		c.expiryTail = n.prev
	}
	n.prev, n.next = nil, nil
}

// expiryLen reports how many nodes the expiry order holds. While the cache is open with a ttl, that
// must equal the number of live entries.
func (c *CacheWithTTL[K, V]) expiryLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for x := c.expiryHead; x != nil; x = x.next {
		n++
	}
	if c.ttl > 0 && !c.closed && n != c.cache.Len() {
		panic(fmt.Sprintf("lru: expiry order holds %d nodes but the cache holds %d entries", n, c.cache.Len()))
	}
	return n
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
// at most sweepChunk entries each, so a large burst of expiries is reclaimed in full at this tick
// without holding readers off for the whole burst.
func (c *CacheWithTTL[K, V]) removeExpired(now time.Time) {
	for c.removeExpiredUpTo(now, sweepChunk) {
	}
}

// removeExpiredUpTo drops up to limit due entries from the head of the expiry order under one lock
// hold. It reports whether it stopped at the limit with more still due.
func (c *CacheWithTTL[K, V]) removeExpiredUpTo(now time.Time, limit int) (more bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for i < limit {
		n := c.expiryHead
		if n == nil || !now.After(n.expiresAt) {
			// The head is not due, and nothing behind it is either.
			break
		}
		i++
		// Remove unlinks the node through the hook; a node is linked only while its entry is live.
		c.cache.Remove(n.key)
		if c.expiryHead == n {
			c.detach(n)
		}
	}
	return i == limit && c.expiryHead != nil && now.After(c.expiryHead.expiresAt)
}
