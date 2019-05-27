package main

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type BlockFeeder interface {
	Close()
	GetHeaderByHash(hash common.Hash) *types.Header
	GetHeaderByNumber(number uint64) *types.Header
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	TotalDifficulty() *big.Int
	LastBlock() *types.Block
}
