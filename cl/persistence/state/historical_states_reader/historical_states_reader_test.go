package historical_states_reader_test

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := memdb.NewTestDB(t)
	reader := tests.LoadChain(blocks, postState, db, t)

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, reader, log.New(), true, true, true)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	// Now lets test it against the reader
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	vt = state_accessors.NewStaticValidatorTable()
	require.NoError(t, state_accessors.ReadValidatorsTable(tx, vt))
	hr := historical_states_reader.NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, reader, vt, preState)
	s, err := hr.ReadHistoricalState(ctx, tx, blocks[len(blocks)-1].Block.Slot)
	require.NoError(t, err)

	postHash, err := s.HashSSZ()
	require.NoError(t, err)
	_, err = postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(postHash), blocks[len(blocks)-1].Block.StateRoot)
}

func TestStateAntiquaryCapella(t *testing.T) {
	//t.Skip()
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	//t.Skip()
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	//t.Skip()
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}
