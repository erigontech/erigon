// Copyright 2024 The Erigon Authors
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

package bodydownload

import (
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

type PrefetchedBlocks struct {
	blocks *lru.Cache[common.Hash, types.RawBlock]
}

func NewPrefetchedBlocks() *PrefetchedBlocks {
	// Setting this to 2500 as `erigon import` imports blocks in batches of 2500
	// and the import command makes use of PrefetchedBlocks.
	cache, err := lru.New[common.Hash, types.RawBlock](2500)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &PrefetchedBlocks{blocks: cache}
}

func (pb *PrefetchedBlocks) Get(hash common.Hash) (*types.Header, *types.RawBody) {
	if block, ok := pb.blocks.Get(hash); ok {
		return block.Header, block.Body
	}
	return nil, nil
}

func (pb *PrefetchedBlocks) Add(h *types.Header, b *types.RawBody) {
	if b == nil {
		return
	}
	hash := h.Hash()
	pb.blocks.ContainsOrAdd(hash, types.RawBlock{Header: h, Body: b})
}
