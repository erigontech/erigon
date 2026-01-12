package state

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

type EncToBytesI = kv.EncToBytesI

// Freezer takes hot data (e.g. from db) and transforms it
// to snapshot cold data.
type Freezer interface {
	// baseNumFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) (NumMetadata, error)
}

type MetadataSetter interface {
	SetMetadata(metadata []byte)
}

type Collector interface {
	Add(key, value []byte) error
}

//type Collector

/** index building **/

type AccessorIndexBuilder interface {
	Build(ctx context.Context, decomp *seg.Decompressor, from, to RootNum, p *background.Progress) (*recsplit.Index, error)
}

// why we need temporal + db + snapshot txs
// db, snapshot txs separate is low-level api. Sometimes I just want to query only db or only snapshot
// for example...
// temporal tx is about consistency...if i want to query snapshot first and then db
// and use separate tx..it can lead to inconsistency (due to db data being pruned)
// so temporaltx is needed here...

// no need to take mdbx tx
// common methods for files api
type ForkableFilesTxI interface {
	GetFromFiles(entityNum Num) (v Bytes, found bool, fileIdx int, err error) // snapshot only
	Close()
	VisibleFilesMaxRootNum() RootNum
	VisibleFilesMaxNum() Num

	VisibleFiles() VisibleFiles
	vfs() visibleFiles
	GetFromFile(entityNum Num, idx int) (v Bytes, found bool, err error)
	StepSize() uint64

	Garbage(merged *FilesItem) (outs []*FilesItem)
}

type ForkableDbCommonTxI interface {
	Prune(ctx context.Context, to RootNum, limit uint64, logEvery *time.Ticker, tx kv.RwTx) (ForkablePruneStat, error)
	Unwind(ctx context.Context, from RootNum, tx kv.RwTx) (ForkablePruneStat, error)
	HasRootNumUpto(ctx context.Context, to RootNum, tx kv.Tx) (bool, error)
	Id() kv.ForkableId
	Close()
}

// common methods across all forkables
type ForkableBaseTxI interface {
	ForkableDbCommonTxI
	Get(num Num, tx kv.Tx) (Bytes, error)
	Progress(tx kv.Tx) (Num, error)
}

type ForkableDebugAPI[T ForkableDbCommonTxI] interface {
	DebugFiles() ForkableFilesTxI
	DebugDb() T
}

// marked
type MarkedDbTxI interface {
	ForkableDbCommonTxI
	GetDb(num Num, hash []byte, tx kv.Tx) (Bytes, error) // db only (hash==nil => canonical value)
}

type MarkedTxI interface {
	ForkableBaseTxI
	ForkableDebugAPI[MarkedDbTxI]
	Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error
}

// unmarked
type UnmarkedDbTxI interface {
	ForkableDbCommonTxI
	GetDb(num Num, tx kv.Tx) (Bytes, error)
}

type UnmarkedTxI interface {
	ForkableBaseTxI
	ForkableDebugAPI[UnmarkedDbTxI]
	Append(entityNum Num, value Bytes, tx kv.RwTx) error
}

/////////////////// config

// A non-generic interface that any Forkable can implement
type ForkableConfig interface {
	SetFreezer(freezer Freezer)
	SetIndexBuilders(builders ...AccessorIndexBuilder)
	SetPruneFrom(pruneFrom Num)
	UpdateCanonicalTbl()
	// Any other option setters you need
}
