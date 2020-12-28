package commands

import (
	"sync"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

var (
	_chainConfig   *params.ChainConfig
	_genesis       *types.Block
	genesisSetOnce sync.Once
)

func getChainConfig(db ethdb.Database) (*params.ChainConfig, error) {
	cfg, _, err := getChainConfigWithGenesis(db)
	return cfg, err
}

func getChainConfigWithGenesis(db ethdb.Database) (*params.ChainConfig, *types.Block, error) {
	if _chainConfig != nil {
		return _chainConfig, _genesis, nil
	}

	genesisBlock, err := rawdb.ReadBlockByNumber(db, 0)
	if err != nil {
		return nil, nil, err
	}
	cc, err := rawdb.ReadChainConfig(db, genesisBlock.Hash())
	if err != nil {
		return nil, nil, err
	}
	if cc != nil && genesisBlock != nil {
		genesisSetOnce.Do(func() {
			_genesis = genesisBlock
			_chainConfig = cc
		})
	}
	return cc, genesisBlock, nil
}
