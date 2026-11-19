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

package jsonrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/concurrent"
)

type pruneFloorValue[T any] struct {
	floor     T
	expiresAt time.Time
}

type pruneFloorCacheKey struct {
	head                   uint64
	dbViewID               uint64
	historyFilesGeneration uint64
	blockFilesGeneration   uint64
}

type pruneFloorCacheEntry[T any] struct {
	key     pruneFloorCacheKey
	value   atomic.Pointer[pruneFloorValue[T]]
	refresh concurrent.CachedValue[T]
}

const (
	defaultPruneFloorCacheTTL = time.Second
	pruneFloorCacheSize       = 64
)

// pruneFloorCache caches successful floor reads and coalesces concurrent loads
// by key. Different pinned file views can coexist at one head, so keys include
// the MDBX view and file generations where available. Mapping history txNums to
// blocks also depends on block files. The TTL bounds staleness from physical changes
// not represented by the key.
type pruneFloorCache[T any] struct {
	values sync.Map // pruneFloorCacheKey -> *pruneFloorCacheEntry[T]
	// Evict in insertion order so hits do not write shared cache bookkeeping.
	mu      sync.Mutex
	entries [pruneFloorCacheSize]*pruneFloorCacheEntry[T]
	next    int
	ttl     time.Duration
	now     func() time.Time
}

func (c *pruneFloorCache[T]) timeNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *pruneFloorCache[T]) cacheTTL() time.Duration {
	if c.ttl > 0 {
		return c.ttl
	}
	return defaultPruneFloorCacheTTL
}

func (c *pruneFloorCache[T]) getForKey(ctx context.Context, key pruneFloorCacheKey, read func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	cell := c.valueForKey(key)
	if value := cell.value.Load(); value != nil && c.timeNow().Before(value.expiresAt) {
		return value.floor, nil
	}
	floor, ran, err := cell.refresh.Produce(ctx, func() (T, bool, error) {
		// Another producer may have refreshed the value before we claimed this load.
		if value := cell.value.Load(); value != nil && c.timeNow().Before(value.expiresAt) {
			return value.floor, false, nil
		}
		floor, err := read()
		// Publish only successful reads. CachedValue handles shared refreshes,
		// while the atomic value keeps fresh hits free of its mutex.
		if err == nil {
			cell.value.Store(&pruneFloorValue[T]{floor: floor, expiresAt: c.timeNow().Add(c.cacheTTL())})
		}
		return floor, false, err
	})
	if err != nil && !ran && ctx.Err() == nil {
		// A shared failure may belong to the producer's context or transaction.
		// Retry once on our own view, outside the coalescer so failures do not
		// serialize callers. Both reads stay within their caller's transaction lifetime.
		floor, err = read()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return zero, ctxErr
	}
	if err != nil {
		return zero, err
	}
	return floor, nil
}

func (c *pruneFloorCache[T]) valueForKey(key pruneFloorCacheKey) *pruneFloorCacheEntry[T] {
	if value, ok := c.values.Load(key); ok {
		return value.(*pruneFloorCacheEntry[T])
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, ok := c.values.Load(key); ok {
		return value.(*pruneFloorCacheEntry[T])
	}
	if oldest := c.entries[c.next]; oldest != nil {
		c.values.Delete(oldest.key)
	}
	value := &pruneFloorCacheEntry[T]{key: key}
	c.values.Store(key, value)
	c.entries[c.next] = value
	c.next = (c.next + 1) % pruneFloorCacheSize
	return value
}
