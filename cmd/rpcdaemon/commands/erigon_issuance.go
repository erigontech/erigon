package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

// BlockReward returns the block reward for this block
// func (api *ErigonImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.db.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "block") // nolint goconst
//}

// UncleReward returns the uncle reward for this block
// func (api *ErigonImpl) UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.db.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "uncle") // nolint goconst
//}

// Issuance implements erigon_issuance. Returns the total issuance (block reward plus uncle reward) for the given block.
func (api *ErigonImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (types.BlockIssuance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.BlockIssuance{}, err
	}
	defer tx.Rollback()

	return rawdb.ReadIssuance(tx, uint64(blockNr.Int64()))
}

func (api *ErigonImpl) getBlockByRPCNumber(tx kv.Tx, blockNr rpc.BlockNumber) (*types.Block, error) {
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}
	return api.blockByNumberWithSenders(tx, blockNum)
}

// Issuance structure to return information about issuance
type Issuance struct {
	BlockReward string `json:"blockReward,omitempty"`
	UncleReward string `json:"uncleReward,omitempty"`
	Issuance    string `json:"issuance,omitempty"`
}
