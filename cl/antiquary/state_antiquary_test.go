//go:build integration

package antiquary

import (
	"context"
	_ "embed"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := memdb.NewTestDB(t)
	reader := tests.LoadChain(blocks, postState, db, t)

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, reader, log.New(), true, true, true)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
}

func TestStateAntiquaryCapella(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}
