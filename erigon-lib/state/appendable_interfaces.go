package state

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"
)

type RootNum = ae.RootNum
type Num = ae.Num
type Id = ae.Id
type AppendableId = ae.AppendableId
type Bytes = ae.Bytes

// Freezer takes hot data (e.g. from db) and transforms it
// to snapshot cold data.
// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// baseNumFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, from, to RootNum, tx kv.Tx) error
	SetCollector(coll Collector)
}

type Collector func(values []byte) error

/** index building **/

type AccessorIndexBuilder interface {
	Build(ctx context.Context, from, to RootNum, tmpDir string, p *background.ProgressSet, lvl log.Lvl, logger log.Logger) (*recsplit.Index, error)
	AllowsOrdinalLookupByNum() bool
}
