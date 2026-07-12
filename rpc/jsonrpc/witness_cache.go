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
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
)

const witnessCacheMaxBlocks = 96

// witnessResultCache maps a canonical block hash to its pre-marshaled legacy-mode
// witness. Keying by hash makes reorgs self-evicting — a reorged hash is never
// requested again and ages out via the LRU — so no reconcile step is needed. It also
// carries the push feed both impls share: a non-nil cache always has a non-nil feed,
// and store is the only insert path, so every cached witness is also published.
type witnessResultCache struct {
	lru  *lru.Cache[common.Hash, *ExecutionWitnessResult]
	feed *witnessFeed
}

func newWitnessResultCache(blocks uint) *witnessResultCache {
	if blocks > witnessCacheMaxBlocks {
		blocks = witnessCacheMaxBlocks
	}
	c, err := lru.New[common.Hash, *ExecutionWitnessResult](int(blocks))
	if err != nil {
		panic(err)
	}
	return &witnessResultCache{lru: c, feed: newWitnessFeed()}
}

func (c *witnessResultCache) Get(hash common.Hash) (*ExecutionWitnessResult, bool) {
	return c.lru.Get(hash)
}

func (c *witnessResultCache) Contains(hash common.Hash) bool { return c.lru.Contains(hash) }

func (c *witnessResultCache) Len() int { return c.lru.Len() }

func (c *witnessResultCache) store(num uint64, hash common.Hash, enc []byte) {
	c.lru.Add(hash, &ExecutionWitnessResult{cachedJSON: enc})
	c.feed.publish(witnessPush{num: num, hash: hash, json: enc})
}

func (c *witnessResultCache) subscribe() chan witnessPush     { return c.feed.subscribe() }
func (c *witnessResultCache) unsubscribe(ch chan witnessPush) { c.feed.unsubscribe(ch) }
