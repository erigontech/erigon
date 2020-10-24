package commands

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func getChainConfig(db rawdb.DatabaseReader) (*params.ChainConfig, error) {
	cfg, _, err := getChainConfigWithGenesis(db)
	return cfg, err
}

func getChainConfigWithGenesis(db rawdb.DatabaseReader) (*params.ChainConfig, common.Hash, error) {
	genesis, err := rawdb.ReadBlockByNumber(db, 0)
	if err != nil {
		return nil, common.Hash{}, err
	}
	genesisHash := genesis.Hash()
	cc, err := rawdb.ReadChainConfig(db, genesisHash)
	if err != nil {
		return nil, common.Hash{}, err
	}
	return cc, genesisHash, nil
}
