package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// BlockReward returns the block reward for this block
func (api *TraceAPIImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	return api.rewardCalc(blockNr, "block") // nolint goconst
}

// UncleReward returns the uncle reward for this block
func (api *TraceAPIImpl) UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	return api.rewardCalc(blockNr, "uncle") // nolint goconst
}

// Issuance returns the issuance for this block
func (api *TraceAPIImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	return api.rewardCalc(blockNr, "issuance")
}

func (api *TraceAPIImpl) rewardCalc(blockNr rpc.BlockNumber, which string) (Issuance, error) {
	genesisHash := rawdb.ReadBlockByNumber(api.dbReader, 0).Hash()
	chainConfig := rawdb.ReadChainConfig(api.dbReader, genesisHash)
	block, err := api.getBlockByRPCNumber(blockNr)
	if err != nil {
		return Issuance{}, err
	}
	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	var ret Issuance
	switch which {
	case "block": // nolint goconst
		ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
		return ret, nil
	case "uncle": // nolint goconst
		issuance.Sub(&issuance, &minerReward)
		ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
		return ret, nil
	case "issuance":
		ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
		ret.Issuance = hexutil.EncodeBig(issuance.ToBig())
		issuance.Sub(&issuance, &minerReward)
		ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
		return ret, nil
	}
	return Issuance{}, fmt.Errorf("should not happen in rewardCalc")
}

// Issuance structure to return information about issuance
type Issuance struct {
	BlockReward string `json:"blockReward,omitempty"`
	UncleReward string `json:"uncleReward,omitempty"`
	Issuance    string `json:"issuance,omitempty"`
}
