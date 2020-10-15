package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
)

type ID struct {
	Hash     [4]byte
	NextFork uint64
}

// returns forkID hash also returns next fork block
func (api *APIImpl) ForkID(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (ID, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return ID{}, err
	}

	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return ID{}, err
	}
	defer tx.Rollback()

	chainConfig, genesisHash := getChainConfigWithGenesis(tx)

	forkID := forkid.NewID(chainConfig, genesisHash, blockNumber)

	return ID{forkID.Hash, forkID.Next}, nil
}

type Forks struct {
	Current []uint64
	Next    uint64
}

//
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

	chainConfig := getChainConfig(tx)
	forksBlocks := forkid.GatherForks(chainConfig)

	lastAddedIdx := -1
	passedForks := make([]uint64, 0, len(forksBlocks))
	for i, num := range forksBlocks {
		if num <= blockNumber {
			passedForks = append(passedForks, num)
			lastAddedIdx = i
		}
	}

	var nextFork uint64
	if len(forksBlocks) > lastAddedIdx+1 {
		nextFork = forksBlocks[lastAddedIdx+1]
	}
	return Forks{passedForks, nextFork}, nil
}
