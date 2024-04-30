package services

import (
	"context"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
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
	IterateFrozenBodies(f func(blockNum, baseTxNum, txAmount uint64) error) error
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error)
	ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64)

	// HeadersRange - TODO: change it to `iter`
	HeadersRange(ctx context.Context, walker func(header *types.Header) error) error
	Integrity(ctx context.Context) error
}

type BorEventReader interface {
	LastEventId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	EventLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error)
	EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error)
	BorStartEventID(ctx context.Context, tx kv.Tx, hash common.Hash, blockNum uint64) (uint64, error)
	LastFrozenEventId() uint64
}

type BorSpanReader interface {
	Span(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error)
	LastSpanId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	LastFrozenSpanId() uint64
}

type BorMilestoneReader interface {
	LastMilestoneId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	Milestone(ctx context.Context, tx kv.Getter, milestoneId uint64) ([]byte, error)
}

type BorCheckpointReader interface {
	LastCheckpointId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	Checkpoint(ctx context.Context, tx kv.Getter, checkpointId uint64) ([]byte, error)
}

type CanonicalReader interface {
	CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, error)
	BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error)
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
	FirstTxnNumNotInSnapshots() uint64
}

type HeaderAndCanonicalReader interface {
	HeaderReader
	CanonicalReader
}

type BlockAndTxnReader interface {
	BlockReader
	TxnReader
}

type FullBlockReader interface {
	BlockReader
	BodyReader
	HeaderReader
	BorEventReader
	BorSpanReader
	BorMilestoneReader
	BorCheckpointReader
	TxnReader
	CanonicalReader

	FrozenBlocks() uint64
	FrozenBorBlocks() uint64
	FrozenFiles() (list []string)
	FreezingCfg() ethconfig.BlocksFreezing
	CanPruneTo(currentBlockInDB uint64) (canPruneBlocksTo uint64)

	Snapshots() BlockSnapshots
	BorSnapshots() BlockSnapshots

	AllTypes() []snaptype.Type
}

type BlockSnapshots interface {
	LogStat(label string)
	ReopenFolder() error
	SegmentsMax() uint64
	SegmentsMin() uint64
	Types() []snaptype.Type
	Close()
}

// BlockRetire - freezing blocks: moving old data from DB to snapshot files
type BlockRetire interface {
	PruneAncientBlocks(tx kv.RwTx, limit int) error
	RetireBlocksInBackground(ctx context.Context, miBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []DownloadRequest) error, onDelete func(l []string) error)
	HasNewFrozenFiles() bool
	BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier DBEventNotifier, cc *chain.Config) error
	SetWorkers(workers int)
}

type DBEventNotifier interface {
	OnNewSnapshot()
}

type DownloadRequest struct {
	Version     uint8
	Path        string
	TorrentHash string
}

func NewDownloadRequest(path string, torrentHash string) DownloadRequest {
	return DownloadRequest{Path: path, TorrentHash: torrentHash}
}

type Range struct {
	From, To uint64
}
