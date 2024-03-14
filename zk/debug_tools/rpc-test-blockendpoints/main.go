package main

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
)

func main() {
	ctx := context.Background()
	client, err := ethclient.Dial("http://localhost:8545")
	if err != nil {
		panic(err)
	}

	blockNumber, err := client.BlockNumber(ctx)
	if err != nil {
		panic(err)
	}

	log.Warn("Check starts for block range", "from", 0, "to", blockNumber)
	defer log.Warn("Check finished")

	for i := uint64(0); i <= blockNumber; i++ {

		if i%100000 == 0 {
			log.Warn("Checking block", "blockNumber", i)
		}
		blockByNumber, err := client.BlockByNumber(ctx, new(big.Int).SetUint64(i))
		if err != nil {
			panic(err)
		}

		blockByHash, err := client.BlockByHash(ctx, blockByNumber.Hash())
		if err != nil {
			panic(err)
		}

		if !debug_tools.CompareBlocks(ctx, false, blockByNumber, blockByHash, client, client) {
			log.Warn("Blocks mismatch", "blockNumber", i)
		}

	}
}
