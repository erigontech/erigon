package services

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type All struct {
	BlockReader FullBlockReader
}

type BlockReader interface {
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (block *types.Block, senders []libcommon.Address, err error)
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash libcommon.Hash) (*types.Header, error)
}

type CanonicalReader interface {
	CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (libcommon.Hash, error)
}

type BodyReader interface {
	BodyWithTransactions(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, err error)
	BodyRlp(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error)
	Body(ctx context.Context, tx kv.Getter, hash libcommon.Hash, blockHeight uint64) (body *types.Body, txAmount uint32, err error)
}

type TxnReader interface {
	TxnLookup(ctx context.Context, tx kv.Getter, txnHash libcommon.Hash) (uint64, bool, error)
	TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error)
}
type HeaderAndCanonicalReader interface {
	HeaderReader
	CanonicalReader
}

type BlockAndTxnReader interface {
	BlockReader
	//HeaderReader
	TxnReader
}

type FullBlockReader interface {
	BlockReader
	BodyReader
	HeaderReader
	TxnReader
	CanonicalReader
}
