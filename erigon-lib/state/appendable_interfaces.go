package state

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/recsplit"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
)

type RootNum = ae.RootNum
type Num = ae.Num
type Id = ae.Id
type EncToBytesI = ae.EncToBytesI
type AppendableId = ae.AppendableId
type Bytes = ae.Bytes

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
type StartRoTx[T AppendableBaseTxI] interface {
	BeginFilesTx() T
	BeginNoFilesTx() T
}

// why we need temporal + db + snapshot txs
// db, snapshot txs separate is low-level api. Sometimes I just want to query only db or only snapshot
// for example...
// temporal tx is about consistency...if i want to query snapshot first and then db
// and use separate tx..it can lead to inconsistency (due to db data being pruned)
// so temporaltx is needed here...

type AppendableTemporalCommonTxI interface {
	Close()
	Type() CanonicityStrategy
}

// no need to take mdbx tx
// common methods for files api
type AppendableFilesTxI interface {
	GetFromFiles(entityNum Num) (v Bytes, found bool, fileIdx int, err error) // snapshot only
	Close()
	VisibleFilesMaxRootNum() RootNum
	VisibleFilesMaxNum() Num

	Files() []FilesItem
	GetFromFile(entityNum Num, idx int) (v Bytes, found bool, err error)
}

type AppendableDbCommonTxI interface {
	Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (uint64, error)
	Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error
	Close()
}

// common methods across all appendables
type AppendableBaseTxI interface {
	AppendableDbCommonTxI
	AppendableTemporalCommonTxI
	Get(num Num, tx kv.Tx) (Bytes, error)
	DebugFiles() AppendableFilesTxI
}

// marked
type MarkedDbTxI interface {
	AppendableDbCommonTxI
	GetDb(num Num, hash []byte, tx kv.Tx) (Bytes, error) // db only (hash==nil => canonical value)
	Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error
}

type MarkedTxI interface {
	AppendableBaseTxI
	Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error
	DebugDb() MarkedDbTxI
}

// unmarked
type UnmarkedDbTxI interface {
	AppendableDbCommonTxI
	GetDb(num Num, tx kv.Tx) (Bytes, error)
	Append(entityNum Num, value Bytes, tx kv.RwTx) error
}

type UnmarkedTxI interface {
	AppendableBaseTxI
	Append(entityNum Num, value Bytes, tx kv.RwTx) error
	DebugDb() UnmarkedDbTxI
}

// buffer values before writing to db supposed to store only canonical values
// Note that values in buffer are not reflected in Get call.
type BufferedDbTxI interface {
	AppendableDbCommonTxI
	GetDb(Num, kv.Tx) (Bytes, error)
	Put(Num, Bytes) error
	Flush(context.Context, kv.RwTx) error
}

type BufferedTxI interface {
	AppendableBaseTxI
	Get(Num, kv.Tx) (Bytes, error)
	Put(Num, Bytes) error
	Flush(context.Context, kv.RwTx) error
	DebugDb() BufferedDbTxI
}

type CanonicityStrategy uint8

const (
	// canonicalTbl & valsTbl
	Marked CanonicityStrategy = iota

	/*
		valsTbl; storing only canonical values
		unwinds are rare or values arrive far apart
		and so unwind doesn't need to be very performant.
	*/
	Unmarked
	Buffered
)

/////////////////// config

// A non-generic interface that any Appendable can implement
type AppendableConfig interface {
	SetFreezer(freezer Freezer)
	SetIndexBuilders(builders ...AccessorIndexBuilder)
	SetTs4Bytes(ts4Bytes bool)
	SetPruneFrom(pruneFrom Num)
	// Any other option setters you need
}
