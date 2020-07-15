package main

import (
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type BlockFeeder interface {
	Close()
	GetHeaderByHash(hash common.Hash) *types.Header
	GetHeaderByNumber(number uint64) *types.Header
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetTdByNumber(number uint64) *big.Int
	TotalDifficulty() *big.Int
	LastBlock() *types.Block
	Genesis() *types.Block
}
