package adapter

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type BlockPropagator interface {
	PropagateNewBlockHashes(ctx context.Context, hash common.Hash, number uint64)
	BroadcastNewBlock(ctx context.Context, block *types.Block, td *big.Int)
}
