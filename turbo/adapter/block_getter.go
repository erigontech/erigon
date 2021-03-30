package adapter

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func NewBlockGetter(dbReader ethdb.Getter) *blockGetter {
	return &blockGetter{dbReader}
}

type blockGetter struct {
	dbReader ethdb.Getter
}

func (g *blockGetter) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	return rawdb.ReadBlockByHash(g.dbReader, hash)
}

func (g *blockGetter) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(g.dbReader, hash, number)
}
