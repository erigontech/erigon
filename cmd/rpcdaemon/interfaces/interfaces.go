package interfaces

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

type BlockReader interface {
	BlockWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
}

type FullBlockReader interface {
	BlockReader
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error)
}
