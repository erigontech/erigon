package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
)

type Forks struct {
	Hash   [4]byte
	Passed []uint64
	Next   uint64
}

// returns forkID hash, sorted list of already passed forks and next fork block
func (api *APIImpl) Forks(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (Forks, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return Forks{}, err
	}

	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesisHash := getChainConfigWithGenesis(tx)

	forkID := forkid.NewID(chainConfig, genesisHash, blockNumber)

	forksBlocks := forkid.GatherForks(chainConfig)

	passedForks := make([]uint64, 0, len(forksBlocks))
	for _, num := range forksBlocks {
		if num <= blockNumber {
			passedForks = append(passedForks, num)
		}
	}
	return Forks{forkID.Hash, passedForks, forkID.Next}, nil
}
