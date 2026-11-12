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
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const replayStateCacheSize = 1

type replayStateCache struct {
	once   sync.Once
	states *lru.Cache[common.Hash, *state.CachingBeaconState]
}

func (c *replayStateCache) getCache() *lru.Cache[common.Hash, *state.CachingBeaconState] {
	c.once.Do(func() {
		var err error
		c.states, err = lru.New[common.Hash, *state.CachingBeaconState]("fork_graph_replayed_states", replayStateCacheSize)
		if err != nil {
			panic(err)
		}
	})
	return c.states
}

func (c *replayStateCache) get(root common.Hash) (*state.CachingBeaconState, bool) {
	return c.getCache().Get(root)
}

func (c *replayStateCache) add(root common.Hash, bs *state.CachingBeaconState) {
	c.getCache().Add(root, bs)
}

func (c *replayStateCache) clear() {
	c.getCache().Purge()
}
