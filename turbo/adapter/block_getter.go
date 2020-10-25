package adapter

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func NewBlockGetter(dbReader rawdb.DatabaseReader) *blockGetter {
	return &blockGetter{dbReader}
}

type blockGetter struct {
	dbReader rawdb.DatabaseReader
}

func (g *blockGetter) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	return rawdb.ReadBlockByHash(g.dbReader, hash)
}

func (g *blockGetter) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(g.dbReader, hash, number)
}
