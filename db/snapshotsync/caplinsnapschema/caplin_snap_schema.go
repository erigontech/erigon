package caplinsnapschema

import (
	"fmt"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
)

type CaplinSchema struct {
	blockAndBlobs map[snaptype.Enum]state.SnapNameSchema
	state         map[string]state.SnapNameSchema
}

func NewCaplinSchema(dirs datadir.Dirs, stepSize uint64, stateTypes snapshotsync.SnapshotTypes) CaplinSchema {
	blockAndBlobs := make(map[snaptype.Enum]state.SnapNameSchema)
	for _, snapt := range snaptype.CaplinSnapshotTypes {
		dataVer := snapt.Versions()
		accessorVer := snapt.Indexes()[0].Version
		snaptSchemaVersion := state.NewE2SnapSchemaVersion(dataVer, accessorVer)
		blockAndBlobs[snapt.Enum()] = state.NewE2SnapSchemaWithStepAndDir(dirs.Snap, snapt.Name(), []string{snapt.Indexes()[0].Name}, stepSize, snaptSchemaVersion)
	}

	statemp := make(map[string]state.SnapNameSchema)
	for table := range stateTypes.KeyValueGetters {
		enum, ok := snaptype.ParseEnum(table)
		if !ok || enum < snaptype.MinCaplinEnum+2 || enum >= snaptype.MinBorEnum {
			panic(fmt.Sprintf("Caplin schema: unknown state table %s", table))
		}
		snapt := enum.Type()
		if snapt == nil || len(snapt.Indexes()) == 0 {
			panic(fmt.Sprintf("Caplin schema: state table %s has no registered index", table))
		}

		indexTags := make([]string, len(snapt.Indexes()))
		for i, index := range snapt.Indexes() {
			indexTags[i] = index.Name
		}
		stateSchemaVersion := state.NewE2SnapSchemaVersion(snapt.Versions(), snapt.Indexes()[0].Version)
		statemp[table] = state.NewE2SnapSchemaWithStepAndDir(dirs.SnapCaplin, snapt.Name(), indexTags, stepSize, stateSchemaVersion)
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
