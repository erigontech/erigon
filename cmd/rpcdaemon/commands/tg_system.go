package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Forks is a data type to record a list of forks passed by this node
type Forks struct {
	GenesisHash common.Hash `json:"genesis"`
	Forks       []uint64    `json:"forks"`
}

// Forks implements tg_forks. Returns the genesis block hash and a sorted list of all forks block numbers
func (api *TgImpl) Forks(ctx context.Context) (Forks, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesisHash, err := getChainConfigWithGenesis(tx)
	if err != nil {
		return Forks{}, err
	}
	forksBlocks := forkid.GatherForks(chainConfig)

	return Forks{genesisHash, forksBlocks}, nil
}
