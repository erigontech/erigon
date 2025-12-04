package snapshotsync

import (
	"fmt"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
)

type CaplinSchema struct {
	blockAndBlobs map[snaptype.Enum]state.SnapNameSchema
	state         map[string]state.SnapNameSchema
}

func NewCaplinSchema(dirs datadir.Dirs, stepSize uint64, stateTypes SnapshotTypes) CaplinSchema {
	blockAndBlobs := make(map[snaptype.Enum]state.SnapNameSchema)
	for _, snapt := range snaptype.CaplinSnapshotTypes {
		dataVer := snapt.Versions()
		accessorVer := snapt.Indexes()[0].Version
		snaptSchemaVersion := state.NewE2SnapSchemaVersion(dataVer, accessorVer)
		blockAndBlobs[snapt.Enum()] = state.NewE2SnapSchemaWithStepAndDir(dirs.Snap, snapt.Name(), []string{snapt.Indexes()[0].Name}, stepSize, snaptSchemaVersion)
	}

	statemp := make(map[string]state.SnapNameSchema)
	dataVer := snaptype.BeaconBlocks.Versions()
	accessorVer := snaptype.BeaconBlocks.Indexes()[0].Version
	stateSchemaVersion := state.NewE2SnapSchemaVersion(dataVer, accessorVer)
	for table := range stateTypes.KeyValueGetters {
		statemp[table] = state.NewE2SnapSchemaWithStepAndDir(dirs.SnapCaplin, table, []string{table}, stepSize, stateSchemaVersion)
	}

	return CaplinSchema{blockAndBlobs: blockAndBlobs, state: statemp}
}

func (s CaplinSchema) Get(snapt snaptype.Enum) state.SnapNameSchema {
	v, ok := s.blockAndBlobs[snapt]
	if !ok {
		panic(fmt.Sprintf("Caplin schema: unknown snap type %s", snapt.Type().Name()))
	}
	return v
}

func (s CaplinSchema) GetState(table string) state.SnapNameSchema {
	v, ok := s.state[table]
	if !ok {
		panic(fmt.Sprintf("Caplin schema: unknown state table %s", table))
	}
	return v
}
