package forkables

import (
	"math"
	"path"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

var RcacheForkable kv.ForkableId = math.MaxUint16

func NewRcacheForkable(pre snapcfg.PreverifiedItems, dirs datadir.Dirs, stepSize uint64, logger log.Logger) (*state.Forkable[state.UnmarkedTxI], error) {
	if !state.Registry.Exists(RcacheForkable) {
		// register forkable
		schema := RCacheSnapSchema(dirs, stepSize)
		config := state.NewSnapshotConfig(state.E3SnapCreationConfig(stepSize), schema)
		RcacheForkable = state.RegisterForkable("Rcache", dirs, pre,
			state.WithSnapshotConfig(config),
			state.WithSaltFile(path.Join(dirs.Snap, "salt-state.txt")),
		)
	}
	schema := statecfg.Schema.RCacheForkable

	f, err := state.NewUnmarkedForkable(RcacheForkable, schema.ValsTbl, state.IdentityRootRelationInstance, logger)
	if err != nil {
		return nil, err
	}

	// app opts: default freezer is fine;
	// index builder: default is fine as well..

	return f, nil
}

func RCacheSnapSchema(dirs datadir.Dirs, stepSize uint64) state.SnapNameSchema {
	return state.NewForkableSnapSchema(statecfg.Schema.RCacheForkable, stepSize, dirs)
}
