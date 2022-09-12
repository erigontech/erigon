package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// Forks is a data type to record a list of forks passed by this node
type Forks struct {
	GenesisHash common.Hash `json:"genesis"`
	Forks       []uint64    `json:"forks"`
}

// Forks implements erigon_forks. Returns the genesis block hash and a sorted list of all forks block numbers
func (api *ErigonImpl) Forks(ctx context.Context) (Forks, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesis, err := api.chainConfigWithGenesis(tx)
	if err != nil {
		return Forks{}, err
	}
	forksBlocks := forkid.GatherForks(chainConfig)

	return Forks{genesis.Hash(), forksBlocks}, nil
}

// Post the merge eth_blockNumber will return latest forkChoiceHead block number
// erigon_executedBlockNumber will return latest executed block number
func (api *ErigonImpl) ExecutedBlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	blockNum, err := rpchelper.GetLatestExecutedBlockNumber(tx)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(blockNum), nil
}
