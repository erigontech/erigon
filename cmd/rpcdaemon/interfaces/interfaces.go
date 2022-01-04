package interfaces

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type BlockReader interface {
	BlockWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error)
	CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error)
}

type BodyReader interface {
	Body(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (body *types.Body, err error)
	BodyRlp(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error)
}

type FullBlockReader interface {
	BlockReader
	BodyReader
	HeaderReader
}
