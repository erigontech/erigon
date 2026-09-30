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
	"time"

	"github.com/erigontech/erigon/common/concurrent"
	"github.com/erigontech/erigon/common/lru"
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
	mu     sync.Mutex
	values *lru.BasicLRU[pruneFloorCacheKey, *concurrent.CachedValue[pruneFloorValue[T]]]
	ttl    time.Duration
	now    func() time.Time
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
	// CachedValue measures freshness from the last attempt, including failures.
	// Only a successful read may extend this floor's lifetime.
	if value, observed, _ := cell.Load(); observed && c.timeNow().Before(value.expiresAt) {
		return value.floor, nil
	}
	value, ran, err := cell.Produce(ctx, func() (pruneFloorValue[T], bool, error) {
		// Another producer may have refreshed the value before we claimed this load.
		if value, observed, _ := cell.Load(); observed && c.timeNow().Before(value.expiresAt) {
			return value, false, nil
		}
		floor, err := read()
		return pruneFloorValue[T]{floor: floor, expiresAt: c.timeNow().Add(c.cacheTTL())}, true, err
	})
	floor := value.floor
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

func (c *pruneFloorCache[T]) valueForKey(key pruneFloorCacheKey) *concurrent.CachedValue[pruneFloorValue[T]] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		values := lru.NewBasicLRU[pruneFloorCacheKey, *concurrent.CachedValue[pruneFloorValue[T]]](pruneFloorCacheSize)
		c.values = &values
	}
	if value, ok := c.values.Get(key); ok {
		return value
	}
	value := new(concurrent.CachedValue[pruneFloorValue[T]])
	c.values.Add(key, value)
	return value
}
