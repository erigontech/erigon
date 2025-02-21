package state_test

import (
	"testing"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon/core/snaptype"
	"github.com/stretchr/testify/require"
)

// test marked appendable
func TestMarkedAppendableRegistration(t *testing.T) {
	// just registration goes fine
	dirs := datadir.New(t.TempDir())
	blockId := registerEntity(dirs, "blocks")
	require.Equal(t, ae.EntityId(0), blockId)
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, ae.EntityId(1), headerId)
}

func registerEntity(dirs datadir.Dirs, name string) ae.EntityId {
	preverified := snapcfg.Mainnet
	return ae.RegisterEntity(name, dirs, preverified, ae.WithSnapshotCreationConfig(
		&ae.SnapshotConfig{
			SnapshotCreationConfig: &ae.SnapshotCreationConfig{
				EntitiesPerStep: 1000,
				MergeStages:     []uint64{1000, 20000, 600000},
				MinimumSize:     1000000,
				SafetyMargin:    1000,
			},
		},
	))
}

func TestPutToDb(t *testing.T) {
	dir := datadir.New(t.TempDir())
	log := log.New()
	headerId := registerEntity(dir, "headers")
	require.Equal(t, ae.EntityId(0), headerId)

	// create marked appendable
	freezer := snaptype.NewHeaderFreezer(kv.HeaderCanonical, kv.Headers, log)

	builder := state.NewSimpleAccessorBuilder(state.NewAccessorArgs(true, true), headerId,
		state.WithIndexKeyFactory(&snaptype.HeaderAccessorIndexKeyFactory{}))

	ma, err := state.NewMarkedEntity(headerId, kv.HeaderCanonical, kv.Headers, log,
		state.MA_WithFreezer(freezer),
		state.MA_WithPruneFrom(state.Num(1)),
		state.MA_WithIndexBuilders(builder))
	require.NoError(t, err)
	require.NotNil(t, ma)
}

func TestGetFromDb(t *testing.T) {
	// get
	// getnc
}

func TestPrune(t *testing.T) {
	// prune
}
func TestUnwind(t *testing.T) {
	// unwind
}

func TestBuildFiles(t *testing.T) {
	// put stuff until build files is called
	// and snapshot is created (with indexes)
	// check beginfilesro works with new visible file

	// then check get
	// then prune and check get
	// then unwind and check get
}

func TestMerging(t *testing.T) {
	// keep stuffing data until things merge
	// begin files ro should give "right" files.
}
