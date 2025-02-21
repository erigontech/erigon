package state

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/recsplit"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
)

type RootNum = ae.RootNum
type Num = ae.Num
type Id = ae.Id
type EntityId = ae.EntityId
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
	Prune(ctx context.Context, to RootNum, limit uint64, tx kv.RwTx) error
	Unwind(ctx context.Context, from RootNum, tx kv.RwTx) error
	Close()
}

type MarkedTxI interface {
	EntityTxI
	Get(num Num, tx kv.Tx) (Bytes, error)
	GetNc(num Num, hash []byte, tx kv.Tx) (Bytes, error)
	Put(num Num, hash []byte, value Bytes, tx kv.RwTx) error
}

type RangedTxI interface {
	EntityTxI
	Get(entityNum Num, tx kv.Tx) (Bytes, error)
	Append(entityNum Num, value Bytes, tx kv.RwTx) error

	// when you don't need "write then read" pattern, one can use RangedEntityWriter
	// this collects and sorts data in memory and then writes to db in single tx.
	// which can be more efficient than calling Append() multiple times.
	NewWriter() *RangedEntityWriter
}

// type checks
var _ RangedTxI = (*RangedEntityTx)(nil)
var _ MarkedTxI = (*MarkerTx)(nil)
var _ StartRoTx[RangedTxI] = (*RangedEntity)(nil)
var _ StartRoTx[MarkedTxI] = (*MarkedEntity)(nil)
