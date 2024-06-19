package bridge

import (
	"context"

	"github.com/ledgerwatch/erigon/core/types"
)

type PolygonBridge interface {
	ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error
	Synchronize(ctx context.Context, tip *types.Header) error
	Unwind(ctx context.Context, tip *types.Header) error
	GetEvents(ctx context.Context, blockNum uint64) ([]*types.Message, error)
}

type Service interface {
	PolygonBridge
	Run(ctx context.Context) error
}
