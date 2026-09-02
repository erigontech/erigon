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

	"golang.org/x/sync/singleflight"
)

type pruneFloorAtHead struct {
	head  uint64
	floor uint64
}

// pruneFloorCache caches successful floor reads for an exact chain head and
// coalesces concurrent requests. A different head triggers a fresh read.
type pruneFloorCache struct {
	value atomic.Pointer[pruneFloorAtHead]
	load  singleflight.Group
}

func (c *pruneFloorCache) get(ctx context.Context, head uint64, read func() (uint64, error)) (uint64, error) {
	for {
		if cached := c.value.Load(); cached != nil && cached.head == head {
			return cached.floor, nil
		}

		if err := ctx.Err(); err != nil {
			return 0, err
		}
		_, err, _ := c.load.Do("floor", func() (any, error) {
			if cached := c.value.Load(); cached != nil && cached.head == head {
				return cached.floor, nil
			}
			floor, err := read()
			if err != nil {
				return nil, err
			}
			c.value.Store(&pruneFloorAtHead{head: head, floor: floor})
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
