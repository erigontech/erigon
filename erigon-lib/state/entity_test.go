package state

import (
	"testing"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
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

func registerEntity(dirs datadir.Dirs, name string) EntityId {
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
	dirs := datadir.New(t.TempDir())
	headerId := registerEntity(dirs, "headers")
	require.Equal(t, ae.EntityId(0), headerId)

	// create marked appendable
	ma, err := NewMarkedAppendable(headerId, kv.HeaderCanonical, kv.Headers, log.New())

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
