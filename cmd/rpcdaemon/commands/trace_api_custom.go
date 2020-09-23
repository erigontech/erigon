package commands

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/rpc"
)

// BlockReward returns the block reward for this block
func (api *TraceAPIImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error) {
	return big.Int{}, nil
}

// UncleRewards returns the uncle reward for this block
func (api *TraceAPIImpl) UncleRewards(ctx context.Context, blockNr rpc.BlockNumber) ([]big.Int, error) {
	return []big.Int{}, nil
}

// Issuance returns the issuance for this block
func (api *TraceAPIImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error) {
	return big.Int{}, nil
}
