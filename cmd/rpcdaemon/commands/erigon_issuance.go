package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
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
func (api *ErigonImpl) WatchTheBurn(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Issuance{}, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return Issuance{}, err
	}
	if chainConfig.Ethash == nil {
		// Clique for example has no issuance
		return Issuance{}, nil
	}
	hash, err := rawdb.ReadCanonicalHash(tx, uint64(blockNr))
	if err != nil {
		return Issuance{}, err
	}
	header := rawdb.ReadHeader(tx, hash, uint64(blockNr))
	if header == nil {
		return Issuance{}, fmt.Errorf("could not find block header")
	}

	body := rawdb.ReadBodyWithTransactions(tx, hash, uint64(blockNr))
	if body == nil {
		return Issuance{}, fmt.Errorf("could not find block body")
	}

	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, header, body.Uncles)
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	var ret Issuance
	ret.BlockReward = minerReward.ToBig()
	ret.Issuance = issuance.ToBig()
	issuance.Sub(&issuance, &minerReward)
	ret.UncleReward = issuance.ToBig()
	// Compute how much was burnt
	if header.BaseFee != nil {
		ret.Burnt = header.BaseFee
		ret.Burnt.Mul(ret.Burnt, big.NewInt(int64(len(body.Transactions))))
		ret.Burnt.Mul(ret.Burnt, big.NewInt(int64(header.GasUsed)))
	} else {
		ret.Burnt = common.Big0
	}
	// Compute totalIssued, totalBurnt and the supply of eth
	ret.TotalIssued, err = rawdb.ReadTotalIssued(tx, uint64(blockNr))
	if err != nil {
		return Issuance{}, err
	}
	ret.TotalBurnt, err = rawdb.ReadTotalBurnt(tx, uint64(blockNr))
	if err != nil {
		return Issuance{}, err
	}
	if uint64(blockNr) == 0 {
		ret.Issuance.Set(ret.TotalIssued)
	}
	return ret, nil
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
	BlockReward *big.Int `json:"blockReward"` // Block reward for given block
	UncleReward *big.Int `json:"uncleReward"` // Uncle reward for gived block
	Issuance    *big.Int `json:"issuance"`    // Total amount of wei created in the block
	Burnt       *big.Int `json:"burnt"`       // Total amount of wei burned in the block
	TotalIssued *big.Int `json:"totalIssued"` // Total amount of wei created in total so far
	TotalBurnt  *big.Int `json:"totalBurnt"`  // Total amount of wei burnt so far
}
