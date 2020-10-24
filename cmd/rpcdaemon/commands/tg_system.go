package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
)

type Forks struct {
	GenesisHash common.Hash `json:"genesis"`
	Passed      []uint64    `json:"passed"`
	Next        *uint64     `json:"next,omitempty"`
}

// Forks returns forkID hash, sorted list of already passed forks and next fork block
func (api *TgImpl) Forks(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (Forks, error) {
	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return Forks{}, err
	}

	tx, err := api.dbReader.Begin(ctx, false)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesisHash, err := getChainConfigWithGenesis(tx)
	if err != nil {
		return Forks{}, err
	}
	forksBlocks := forkid.GatherForks(chainConfig)

	lastAddedIdx := -1
	passedForks := make([]uint64, 0, len(forksBlocks))
	for i, num := range forksBlocks {
		if num <= blockNumber {
			passedForks = append(passedForks, num)
			lastAddedIdx = i
		}
	}

	var nextFork *uint64
	if len(forksBlocks) > lastAddedIdx+1 {
		nextFork = new(uint64)
		*nextFork = forksBlocks[lastAddedIdx+1]
	}
	return Forks{genesisHash, passedForks, nextFork}, nil
}
