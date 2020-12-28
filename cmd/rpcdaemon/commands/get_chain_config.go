package commands

import (
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

var chainConfig *params.ChainConfig
var genesis *types.Block
var chainConfigLock sync.RWMutex

func getChainConfig(db ethdb.Database) (*params.ChainConfig, error) {
	cfg, _, err := getChainConfigWithGenesis(db)
	return cfg, err
}

func getChainConfigWithGenesis(db ethdb.Database) (*params.ChainConfig, common.Hash, error) {
	chainConfigLock.RLock()
	defer chainConfigLock.RUnlock()
	if chainConfig == nil {
		chainConfigLock.Lock()
		defer chainConfigLock.Unlock()
		genesisBlock, err := rawdb.ReadBlockByNumber(db, 0)
		if err != nil {
			return nil, common.Hash{}, err
		}
		genesis = genesisBlock
		cc, err := rawdb.ReadChainConfig(db, genesis.Hash())
		if err != nil {
			return nil, common.Hash{}, err
		}
		chainConfig = cc
	}

	return chainConfig, genesis.Hash(), nil
}
