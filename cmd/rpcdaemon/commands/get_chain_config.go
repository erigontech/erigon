package commands

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func getChainConfig(db rawdb.DatabaseReader) *params.ChainConfig {
	cfg, _ := getChainConfigWithGenesis(db)
	return cfg
}

func getChainConfigWithGenesis(db rawdb.DatabaseReader) (*params.ChainConfig, common.Hash) {
	genesisHash := rawdb.ReadBlockByNumber(db, 0).Hash()
	return rawdb.ReadChainConfig(db, genesisHash), genesisHash
}
