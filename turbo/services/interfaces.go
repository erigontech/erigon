package services

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rlp"
)

type All struct {
	BlockReader FullBlockReader
}

type BlockReader interface {
	BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error)
	BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error)
	CurrentBlock(db kv.Tx) (*types.Block, error)
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (block *types.Block, senders []common.Address, err error)
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error)
}

type CanonicalReader interface {
	CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, error)
}

type BodyReader interface {
	BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, err error)
	BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bodyRlp rlp.RawValue, err error)
	Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, txAmount uint32, err error)
	HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error)
}

type TxnReader interface {
	TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error)
	TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error)
	RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error)
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

	TxsV3Enabled() bool
	Snapshots() BlockSnapshots
}

type BlockSnapshots interface {
	Cfg() ethconfig.Snapshot
	BlocksAvailable() uint64
}

/*
type HeaderWriter interface {
	WriteHeader(tx kv.RwTx, header *types.Header) error
	WriteHeaderRaw(tx kv.StatelessRwTx, number uint64, hash libcommon.Hash, headerRlp []byte, skipIndexing bool) error
	WriteCanonicalHash(tx kv.RwTx, hash libcommon.Hash, number uint64) error
	WriteTd(db kv.Putter, hash libcommon.Hash, number uint64, td *big.Int) error
	// [from,to)
	FillHeaderNumberIndex(logPrefix string, tx kv.RwTx, tmpDir string, from, to uint64, ctx context.Context, logger log.Logger) error
}
type BlockWriter interface {
	HeaderWriter
	WriteRawBodyIfNotExists(tx kv.RwTx, hash libcommon.Hash, number uint64, body *types.RawBody) (ok bool, lastTxnNum uint64, err error)
	WriteBody(tx kv.RwTx, hash libcommon.Hash, number uint64, body *types.Body) error
}
*/
