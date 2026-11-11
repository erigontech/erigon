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

package fork_graph

import (
	"sync"

	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

const replayStateCacheSize = 4

type replayStateCache struct {
	mu     sync.Mutex
	states map[common.Hash]*state.CachingBeaconState
	order  []common.Hash
}

func (c *replayStateCache) get(root common.Hash) (*state.CachingBeaconState, bool) {
	c.mu.Lock()
	cached, ok := c.states[root]
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	copy, err := cached.Copy()
	if err != nil {
		return nil, false
	}
	return copy, true
}

func (c *replayStateCache) add(root common.Hash, bs *state.CachingBeaconState) {
	copy, err := bs.Copy()
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil {
		c.states = make(map[common.Hash]*state.CachingBeaconState, replayStateCacheSize)
	}
	if _, ok := c.states[root]; ok {
		for i, cachedRoot := range c.order {
			if cachedRoot == root {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	} else if len(c.order) == replayStateCacheSize {
		delete(c.states, c.order[0])
		c.order = c.order[1:]
	}
	c.states[root] = copy
	c.order = append(c.order, root)
}

func (c *replayStateCache) clear() {
	c.mu.Lock()
	c.states = nil
	c.order = nil
	c.mu.Unlock()
}

func (c *replayStateCache) remove(root common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.states[root]; !ok {
		return
	}
	delete(c.states, root)
	for i, cachedRoot := range c.order {
		if cachedRoot == root {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}
