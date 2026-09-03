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
)

type pruneFloorValue struct {
	floor     uint64
	expiresAt time.Time
}

type pruneFloorLoad struct {
	done  chan struct{}
	floor uint64
	err   error
}

type pruneFloorCacheKey struct {
	head               uint64
	snapshotGeneration uint64
}

const defaultPruneFloorCacheTTL = time.Second

// pruneFloorCache caches successful floor reads and coalesces concurrent loads
// by key. Every key contains the exact chain head; local block-floor keys also
// contain the pinned snapshot generation because visible files can change
// without a new head. The TTL bounds staleness from physical changes the key
// cannot identify.
type pruneFloorCache struct {
	mu     sync.Mutex
	values map[pruneFloorCacheKey]pruneFloorValue
	loads  map[pruneFloorCacheKey]*pruneFloorLoad
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
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		floor, cached, load, leader := c.join(key)
		if cached {
			return floor, nil
		}
		if leader {
			floor, err := read()
			c.finish(key, load, floor, err)
			if err != nil {
				return 0, err
			}
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return floor, nil
		}

		select {
		case <-load.done:
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if load.err != nil {
				// The leader's failure may be specific to its context. Retry this
				// caller's read instead of inheriting that failure.
				continue
			}
			return load.floor, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (c *pruneFloorCache) join(key pruneFloorCacheKey) (floor uint64, cached bool, load *pruneFloorLoad, leader bool) {
	now := c.timeNow()
	c.mu.Lock()
	defer c.mu.Unlock()

	if value, ok := c.values[key]; ok && now.Before(value.expiresAt) {
		return value.floor, true, nil, false
	}
	if load := c.loads[key]; load != nil {
		return 0, false, load, false
	}
	if c.loads == nil {
		c.loads = make(map[pruneFloorCacheKey]*pruneFloorLoad)
	}
	load = &pruneFloorLoad{done: make(chan struct{})}
	c.loads[key] = load
	return 0, false, load, true
}

func (c *pruneFloorCache) finish(key pruneFloorCacheKey, load *pruneFloorLoad, floor uint64, err error) {
	now := c.timeNow()
	c.mu.Lock()
	load.floor, load.err = floor, err
	delete(c.loads, key)
	if err == nil {
		if c.values == nil {
			c.values = make(map[pruneFloorCacheKey]pruneFloorValue)
		}
		for cachedKey, value := range c.values {
			if !now.Before(value.expiresAt) {
				delete(c.values, cachedKey)
			}
		}
		c.values[key] = pruneFloorValue{floor: floor, expiresAt: now.Add(c.cacheTTL())}
	}
	c.mu.Unlock()
	close(load.done)
}
