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

// ttlEntry is what the cache stores. expiresAt is set by Add and by nothing else, and the entry's
// expiry node carries the same deadline: the two are kept in step by Add alone.
type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func (e ttlEntry[V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// expiryNode is one live entry's place in the expiry order.
type expiryNode[K comparable] struct {
	key        K
	expiresAt  time.Time
	prev, next *expiryNode[K]
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
	// The expiry order: one node per live entry, sorted by deadline, indexed by key. Add stamps
	// deadlines under mu with the cache's one ttl, so appending keeps the order sorted; a renewal
	// moves the key's node to the tail; the eviction hook unlinks a node when its entry is removed
	// or evicted by size. The sweep pops due nodes from the head and stops at the first live one.
	expiryHead, expiryTail *expiryNode[K]
	expiryByKey            map[K]*expiryNode[K]
	// After Close nothing consumes the list, so it is dropped and nothing more is recorded.
	closed bool

	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
}

func NewWithTTL[K comparable, V any](metricName string, size int, ttl time.Duration) *CacheWithTTL[K, V] {
	c := &CacheWithTTL[K, V]{
		ttl:           ttl,
		metric:        metricName,
		expiryByKey:   make(map[K]*expiryNode[K]),
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
		metricTTLHit:  metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_hit{%s=%q}`, "cache", metricName)),
		metricTTLMiss: metrics.GetOrCreateCounter(fmt.Sprintf(`golang_ttl_lru_cache_miss{%s=%q}`, "cache", metricName)),
	}
	// The hook runs synchronously on the goroutine that called into the cache, which already
	// holds c.mu on every path that can evict. It touches only the expiry order and never calls
	// back into the cache.
	c.cache = expirable.NewLRU[K, ttlEntry[V]](size, func(k K, _ ttlEntry[V]) { c.unlink(k) }, 0)
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
// sweep stops, and with it the expiry bookkeeping, which nothing would consume. Close is safe to
// call more than once and from several goroutines at once.
func (c *CacheWithTTL[K, V]) Close() {
	c.mu.Lock()
	c.closed = true
	c.expiryHead, c.expiryTail = nil, nil
	c.expiryByKey = nil
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.done) })
	<-c.stopped
}

func (c *CacheWithTTL[K, V]) Add(k K, v V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The deadline is taken under the lock: time spent waiting for it would otherwise be charged
	// to the entry's ttl, and a long enough wait would store a value that is already expired.
	// Taking it under the lock is also what keeps the expiry order sorted.
	e := ttlEntry[V]{value: v}
	if c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
	}
	// A size eviction inside Add unlinks the evicted key through the hook; it can never be k.
	evicted = c.cache.Add(k, e)
	if c.ttl > 0 {
		c.record(k, e.expiresAt)
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

// record places k at the tail of the expiry order with its new deadline, moving its node there on
// a renewal so a key never holds more than one. Called under mu. After Close nothing is recorded:
// no sweep will consume it, and Get still drops an expired entry it reads.
func (c *CacheWithTTL[K, V]) record(k K, expiresAt time.Time) {
	if c.closed {
		return
	}
	n := c.expiryByKey[k]
	if n == nil {
		n = &expiryNode[K]{key: k}
		c.expiryByKey[k] = n
	} else {
		c.detach(n)
	}
	n.expiresAt = expiresAt
	n.prev, n.next = c.expiryTail, nil
	if c.expiryTail != nil {
		c.expiryTail.next = n
	} else {
		c.expiryHead = n
	}
	c.expiryTail = n
}

// unlink forgets k's place in the expiry order, if it has one. Called under mu, directly or
// through the eviction hook.
func (c *CacheWithTTL[K, V]) unlink(k K) {
	if n := c.expiryByKey[k]; n != nil {
		delete(c.expiryByKey, k)
		c.detach(n)
	}
}

// detach splices n out of the list without touching the index.
func (c *CacheWithTTL[K, V]) detach(n *expiryNode[K]) {
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

// expiryLen reports how many entries the expiry order holds. The index and the list must agree.
func (c *CacheWithTTL[K, V]) expiryLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for x := c.expiryHead; x != nil; x = x.next {
		n++
	}
	if n != len(c.expiryByKey) {
		panic(fmt.Sprintf("lru: expiry list holds %d nodes but the index holds %d", n, len(c.expiryByKey)))
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
		// Remove unlinks the node through the hook. Peek is not needed: a node exists only
		// while its entry does, with the entry's own deadline.
		c.cache.Remove(n.key)
		if c.expiryHead == n {
			// Only if the hook did not run, which the dependency guarantees it does.
			c.unlink(n.key)
		}
	}
	return i == limit && c.expiryHead != nil && now.After(c.expiryHead.expiresAt)
}
