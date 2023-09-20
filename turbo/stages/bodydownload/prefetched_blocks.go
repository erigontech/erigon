package bodydownload

import (
	"github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
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
