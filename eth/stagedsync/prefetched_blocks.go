package stagedsync

import (
	"fmt"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type PrefetchedBlocks struct {
	blocks *sync.Map
}

func NewPrefetchedBlocks() *PrefetchedBlocks {
	return &PrefetchedBlocks{blocks: &sync.Map{}}
}

func (pb *PrefetchedBlocks) GetBlockByHash(hash common.Hash) *types.Block {
	fmt.Println("PrefetchedBlocks.GetBlockByHash", hash.Hex())
	if val, ok := pb.blocks.Load(hash); ok && val != nil {
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
	pb.blocks.Store(hash, b)
}
