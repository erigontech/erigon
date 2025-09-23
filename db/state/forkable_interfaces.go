package state

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
)

type EncToBytesI = kv.EncToBytesI

// Freezer takes hot data (e.g. from db) and transforms it
// to snapshot cold data.
type Freezer interface {
	// baseNumFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) error
}

type Collector func(values []byte) error

/** index building **/

type AccessorIndexBuilder interface {
	Build(ctx context.Context, from, to RootNum, p *background.Progress) (*recsplit.Index, error)
	AllowsOrdinalLookupByNum() bool
}

// generator for the two interfaces
// T: temporal interface (db + files)
// D: db interface
// we don't need a separate interface for files...
// since tx is provided separately anyway.
type StartRoTx[T ForkableBaseTxI] interface {
	BeginTemporalTx() T
	BeginNoFilesTx() T
}

// why we need temporal + db + snapshot txs
// db, snapshot txs separate is low-level api. Sometimes I just want to query only db or only snapshot
// for example...
// temporal tx is about consistency...if i want to query snapshot first and then db
// and use separate tx..it can lead to inconsistency (due to db data being pruned)
// so temporaltx is needed here...

type ForkableTemporalCommonTxI interface {
	Close()
	Type() kv.CanonicityStrategy
}

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

	Garbage(merged *FilesItem) (outs []*FilesItem)
}

type ForkableDbCommonTxI interface {
	Prune(ctx context.Context, to RootNum, limit uint64, logEvery *time.Ticker, tx kv.RwTx) (ForkablePruneStat, error)
	Unwind(ctx context.Context, from RootNum, tx kv.RwTx) (ForkablePruneStat, error)
	HasRootNumUpto(ctx context.Context, to RootNum, tx kv.Tx) (bool, error)
	Close()
}

// common methods across all forkables
type ForkableBaseTxI interface {
	ForkableDbCommonTxI
	ForkableTemporalCommonTxI
	Get(num Num, tx kv.Tx) (Bytes, error)
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

// buffer values before writing to db supposed to store only canonical values
// Note that values in buffer are not reflected in Get call.
type BufferedDbTxI interface {
	ForkableDbCommonTxI
	GetDb(Num, kv.Tx) (Bytes, error)
	Put(Num, Bytes) error
	Flush(context.Context, kv.RwTx) error
}

type BufferedTxI interface {
	ForkableBaseTxI
	ForkableDebugAPI[BufferedDbTxI]
	Get(Num, kv.Tx) (Bytes, error)
	Put(Num, Bytes) error
	Flush(context.Context, kv.RwTx) error
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
