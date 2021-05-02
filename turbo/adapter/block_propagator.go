package adapter

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/core/types"
)

type BlockPropagator interface {
	BroadcastNewBlock(ctx context.Context, block *types.Block, td *big.Int)
}
