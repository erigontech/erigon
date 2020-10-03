package commands

import (
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func getChainConfig(db rawdb.DatabaseReader) *params.ChainConfig {
	genesisHash := rawdb.ReadBlockByNumber(db, 0).Hash()
	return rawdb.ReadChainConfig(db, genesisHash)
}
