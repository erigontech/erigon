// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package services

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

type All struct {
	BlockReader FullBlockReader
}

type BlockReader interface {
	BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error)
	BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error)
	CurrentBlock(db kv.Tx) (*types.Block, error)
	BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (block *types.Block, senders []common.Address, err error)
	IterateFrozenBodies(f func(blockNum, baseTxNum, txCount uint64) error) error
}

type HeaderReader interface {
	Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error)
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
	HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error)
	HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error)
	ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64)

	// HeadersRange - TODO: change it to `stream`
	HeadersRange(ctx context.Context, walker func(header *types.Header) error) error
	Integrity(ctx context.Context) error
}

type BorEventReader interface {
	LastEventId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	EventLookup(ctx context.Context, tx kv.Tx, txnHash common.Hash) (uint64, bool, error)
	EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error)
	BorStartEventId(ctx context.Context, tx kv.Tx, hash common.Hash, blockNum uint64) (uint64, error)
	LastFrozenEventId() uint64
	LastFrozenEventBlockNum() uint64
}

type BorSpanReader interface {
	Span(ctx context.Context, tx kv.Tx, spanId uint64) (*heimdall.Span, bool, error)
	LastSpanId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	LastFrozenSpanId() uint64
}

type BorMilestoneReader interface {
	LastMilestoneId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	Milestone(ctx context.Context, tx kv.Tx, milestoneId uint64) (*heimdall.Milestone, bool, error)
}

type BorCheckpointReader interface {
	LastCheckpointId(ctx context.Context, tx kv.Tx) (uint64, bool, error)
	Checkpoint(ctx context.Context, tx kv.Tx, checkpointId uint64) (*heimdall.Checkpoint, bool, error)
}

type CanonicalReader interface {
	CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (h common.Hash, ok bool, err error)
	IsCanonical(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error)
	BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error)
}

type BodyReader interface {
	BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, err error)
	BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bodyRlp rlp.RawValue, err error)
	Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (body *types.Body, txCount uint32, err error)
	CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (body *types.BodyForStorage, err error)
	HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (bool, error)
}

type TxnReader interface {
	TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (blockNum uint64, txNum uint64, ok bool, err error)
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

	Snapshots() snapshotsync.BlockSnapshots
	BorSnapshots() snapshotsync.BlockSnapshots

	Ready(ctx context.Context) <-chan error

	AllTypes() []snaptype.Type
}

// BlockRetire - freezing blocks: moving old data from DB to snapshot files
type BlockRetire interface {
	PruneAncientBlocks(tx kv.RwTx, limit int) (deleted int, err error)
	RetireBlocksInBackground(ctx context.Context, miBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error, onFinishRetire func() error)
	BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier DBEventNotifier, cc *chain.Config) error
	SetWorkers(workers int)
	GetWorkers() int
}

type DBEventNotifier interface {
	OnNewSnapshot()
}

type Range struct {
	From, To uint64
}
