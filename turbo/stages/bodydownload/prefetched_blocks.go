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

func (pb *PrefetchedBlocks) Pop(hash common.Hash) *types.Block {
	if val, ok := pb.blocks.Get(hash); ok && val != nil {
		pb.blocks.Remove(hash)
		if block, ok := val.(*types.Block); ok {
			return block
		}
	}
	return nil
}

func (pb *PrefetchedBlocks) Add(b *types.Block) {
	if b == nil {
		return
	}
	hash := b.Hash()
	pb.blocks.ContainsOrAdd(hash, b)
}
