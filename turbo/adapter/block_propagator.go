package adapter

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
)

type BlockPropagator func(ctx context.Context, block *types.Block, td *big.Int)
