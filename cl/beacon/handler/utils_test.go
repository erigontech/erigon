package handler

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setupTestingHandler(t *testing.T, v clparams.StateVersion) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, f afero.Fs, preState, postState *state.CachingBeaconState, handler *ApiHandler, opPool pool.OperationsPool, syncedData *synced_data.SyncedDataManager, fcu *forkchoice.ForkChoiceStorageMock) {
	bcfg := clparams.MainnetBeaconConfig
	if v == clparams.Phase0Version {
		blocks, preState, postState = tests.GetPhase0Random()
	} else if v == clparams.BellatrixVersion {
		bcfg.AltairForkEpoch = 1
		bcfg.BellatrixForkEpoch = 1
		blocks, preState, postState = tests.GetBellatrixRandom()
	} else {
		require.FailNow(t, "unknown state version")
	}
	fcu = forkchoice.NewForkChoiceStorageMock()
	db = memdb.NewTestDB(t)
	var reader *tests.MockBlockReader
	reader, f = tests.LoadChain(blocks, postState, db, t)

	rawDB := persistence.NewAferoRawBlockSaver(f, &clparams.MainnetBeaconConfig)
	bcfg.InitializeForkSchedule()

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, preState, vt, &bcfg, datadir.New("/tmp"), nil, db, nil, reader, nil, log.New(), true, true, f)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	// historical states reader below
	statesReader := historical_states_reader.NewHistoricalStatesReader(&bcfg, reader, vt, f, preState)
	opPool = pool.NewOperationsPool(&bcfg)
	fcu.Pool = opPool
	syncedData = synced_data.NewSyncedDataManager(true, &bcfg)
	gC := clparams.GenesisConfigs[clparams.MainnetNetwork]
	handler = NewApiHandler(
		&gC,
		&bcfg,
		rawDB,
		db,
		fcu,
		opPool,
		reader,
		syncedData,
		statesReader,
		nil)
	handler.init()
	return
}
