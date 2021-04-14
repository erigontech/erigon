package bodydownload

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"

	lru "github.com/hashicorp/golang-lru"
)

type PrefetchedBlocks struct {
	blocks *lru.Cache
}

func NewPrefetchedBlocks() *PrefetchedBlocks {
	cache, err := lru.New(1000)
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
