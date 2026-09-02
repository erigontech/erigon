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
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type pruneFloorAtHead struct {
	head      uint64
	floor     uint64
	expiresAt time.Time
}

const defaultPruneFloorCacheTTL = time.Second

// pruneFloorCache briefly caches successful floor reads for an exact chain head
// and coalesces concurrent requests. The lifetime also bounds staleness when the
// available files change without moving the head.
type pruneFloorCache struct {
	value atomic.Pointer[pruneFloorAtHead]
	load  singleflight.Group
	ttl   time.Duration
	now   func() time.Time
}

func (c *pruneFloorCache) cached(head uint64) (uint64, bool) {
	cached := c.value.Load()
	if cached == nil || cached.head != head || !c.timeNow().Before(cached.expiresAt) {
		return 0, false
	}
	return cached.floor, true
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
	for {
		if floor, ok := c.cached(head); ok {
			return floor, nil
		}

		if err := ctx.Err(); err != nil {
			return 0, err
		}
		_, err, _ := c.load.Do("floor", func() (any, error) {
			if floor, ok := c.cached(head); ok {
				return floor, nil
			}
			floor, err := read()
			if err != nil {
				return nil, err
			}
			c.value.Store(&pruneFloorAtHead{
				head:      head,
				floor:     floor,
				expiresAt: c.timeNow().Add(c.cacheTTL()),
			})
			return floor, nil
		})
		if err != nil {
			return 0, err
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
}
