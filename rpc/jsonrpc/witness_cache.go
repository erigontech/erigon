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
// requested again and ages out via the LRU — so no reconcile step is needed.
type witnessResultCache = lru.Cache[common.Hash, *ExecutionWitnessResult]

// WitnessCacheCapacity is the number of witnesses the cache actually holds for a
// requested block count, after clamping to witnessCacheMaxBlocks.
func WitnessCacheCapacity(blocks uint) uint {
	if blocks > witnessCacheMaxBlocks {
		return witnessCacheMaxBlocks
	}
	return blocks
}

func newWitnessResultCache(blocks uint) *witnessResultCache {
	c, err := lru.New[common.Hash, *ExecutionWitnessResult](int(WitnessCacheCapacity(blocks)))
	if err != nil {
		panic(err)
	}
	return c
}
