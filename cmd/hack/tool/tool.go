package tool

import (
	"context"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/params"
)

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func ParseFloat64(str string) float64 {
	v, _ := strconv.ParseFloat(str, 64)
	return v
}

func ChainConfig(tx kv.Tx) *params.ChainConfig {
	genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
	Check(err)
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
	Check(err)
	return chainConfig
}

func ChainConfigFromDB(db kv.RoDB) (cc *params.ChainConfig) {
	err := db.View(context.Background(), func(tx kv.Tx) error {
		cc = ChainConfig(tx)
		return nil
	})
	Check(err)
	return cc
}
