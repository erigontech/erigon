package adapter

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func NewBlockGetter(tx ethdb.Tx) *blockGetter {
	return &blockGetter{tx}
}

type blockGetter struct {
	tx ethdb.Tx
}

func (g *blockGetter) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	return rawdb.ReadBlockByHash(g.tx, hash)
}

func (g *blockGetter) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(g.tx, hash, number)
}
