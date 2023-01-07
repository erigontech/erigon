package bodydownload

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"

	lru "github.com/hashicorp/golang-lru"
)

type PrefetchedBlocks struct {
	blocks *lru.Cache
}

func NewPrefetchedBlocks() *PrefetchedBlocks {
	// Setting this to 2500 as `erigon import` imports blocks in batches of 2500
	// and the import command makes use of PrefetchedBlocks.
	cache, err := lru.New(2500)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &PrefetchedBlocks{blocks: cache}
}

func (pb *PrefetchedBlocks) Get(hash common.Hash) (*types.Header, *types.RawBody) {
	if val, ok := pb.blocks.Get(hash); ok && val != nil {
		if headerAndBody, ok := val.(types.HeaderAndBody); ok {
			return headerAndBody.Header, headerAndBody.Body
		}
	}
	return nil, nil
}

func (pb *PrefetchedBlocks) Add(h *types.Header, b *types.RawBody) {
	if b == nil {
		return
	}
	hash := h.Hash()
	pb.blocks.ContainsOrAdd(hash, types.HeaderAndBody{Header: h, Body: b})
}
