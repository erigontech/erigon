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

type pruneFloorValue struct {
	floor     uint64
	expiresAt time.Time
}

type pruneFloorCacheKey struct {
	head               uint64
	snapshotGeneration uint64
}

const (
	defaultPruneFloorCacheTTL = time.Second
	pruneFloorCacheSize       = 64
)

// pruneFloorCache caches successful floor reads and coalesces concurrent loads
// by key. Every key contains the exact chain head; local block-floor keys also
// contain the pinned snapshot generation because visible files can change
// without a new head. The TTL bounds staleness from physical changes the key
// cannot identify.
type pruneFloorCache struct {
	mu     sync.Mutex
	values *lru.BasicLRU[pruneFloorCacheKey, *concurrent.CachedValue[pruneFloorValue]]
	ttl    time.Duration
	now    func() time.Time
}

func (c *pruneFloorCache) timeNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *pruneFloorCache) cacheTTL() time.Duration {
	if c.ttl > 0 {
		return c.ttl
	}
	return defaultPruneFloorCacheTTL
}

func (c *pruneFloorCache) get(ctx context.Context, head uint64, read func() (uint64, error)) (uint64, error) {
	return c.getForKey(ctx, pruneFloorCacheKey{head: head}, read)
}

func (c *pruneFloorCache) getForKey(ctx context.Context, key pruneFloorCacheKey, read func() (uint64, error)) (uint64, error) {
	cell := c.valueForKey(key)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// CachedValue measures freshness from the last attempt, including failures.
		// Only a successful read may extend this floor's lifetime.
		if value, observed, _ := cell.Load(); observed && c.timeNow().Before(value.expiresAt) {
			return value.floor, nil
		}
		// Produce runs read synchronously so it cannot outlive the caller's
		// transaction. Waiters can cancel without interrupting that read.
		value, ran, err := cell.Produce(ctx, func() (pruneFloorValue, bool, error) {
			// Another producer may have refreshed the value before we claimed this load.
			if value, observed, _ := cell.Load(); observed && c.timeNow().Before(value.expiresAt) {
				return value, false, nil
			}
			floor, err := read()
			return pruneFloorValue{floor: floor, expiresAt: c.timeNow().Add(c.cacheTTL())}, true, err
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if err == nil {
			return value.floor, nil
		}
		if ran {
			return 0, err
		}
		// A shared failure may belong to the producer's context or transaction.
		// Retry through this caller's read instead of inheriting that failure.
	}
}

func (c *pruneFloorCache) valueForKey(key pruneFloorCacheKey) *concurrent.CachedValue[pruneFloorValue] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		values := lru.NewBasicLRU[pruneFloorCacheKey, *concurrent.CachedValue[pruneFloorValue]](pruneFloorCacheSize)
		c.values = &values
	}
	if value, ok := c.values.Get(key); ok {
		return value
	}
	value := new(concurrent.CachedValue[pruneFloorValue])
	c.values.Add(key, value)
	return value
}
