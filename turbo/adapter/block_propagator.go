package adapter

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
)

type BlockPropagator func(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int)
