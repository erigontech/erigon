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
// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// baseNumFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, from, to RootNum, db kv.RoDB) error
	SetCollector(coll Collector)
}

type Collector func(values []byte) error

/** index building **/

type AccessorIndexBuilder interface {
	Build(ctx context.Context, from, to RootNum, p *background.Progress) (*recsplit.Index, error)
	AllowsOrdinalLookupByNum() bool
}

type StartRoTx[T EntityTxI] interface {
	BeginFilesRo() T
}

type EntityTxI interface {
	// value, value from snapshot?, error
	Get(entityNum Num, tx kv.Tx) (Bytes, bool, error)
	Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) (uint64, error)
	Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error
	Close()
	Type() CanonicityStrategy

	VisibleFilesMaxRootNum() RootNum
	VisibleFilesMaxNum() Num
}

type MarkedTxI interface {
	EntityTxI
	GetNc(num Num, hash []byte, tx kv.Tx) ([]byte, error)
	Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error
}

type UnmarkedTxI interface {
	EntityTxI
	Append(entityNum Num, value Bytes, tx kv.RwTx) error
}

type AppendingTxI interface {
	EntityTxI
	// db only
	GetNc(entityId Id, tx kv.Tx) (Bytes, error)
	Append(entityId Id, value Bytes, tx kv.RwTx) error

	// sequence apis
	IncrementSequence(amount uint64, tx kv.RwTx) (uint64, error)
	ReadSequence(tx kv.Tx) (uint64, error)
	ResetSequence(value uint64, tx kv.RwTx) error
}

/*
buffer values before writing to db supposed to store only canonical values
Note that values in buffer are not reflected in Get call.
*/
type BufferedTxI interface {
	EntityTxI
	Put(Num, Bytes) error
	Flush(context.Context, kv.RwTx) error
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
