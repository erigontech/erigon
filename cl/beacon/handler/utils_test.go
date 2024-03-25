package handler

import (
	"context"
	"math"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/validator/validator_params"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setupTestingHandler(t *testing.T, v clparams.StateVersion, logger log.Logger) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, f afero.Fs, preState, postState *state.CachingBeaconState, h *ApiHandler, opPool pool.OperationsPool, syncedData *synced_data.SyncedDataManager, fcu *forkchoice.ForkChoiceStorageMock, vp *validator_params.ValidatorParams) {
	bcfg := clparams.MainnetBeaconConfig
	if v == clparams.Phase0Version {
		blocks, preState, postState = tests.GetPhase0Random()
	} else if v == clparams.BellatrixVersion {
		bcfg.AltairForkEpoch = 1
		bcfg.BellatrixForkEpoch = 1
		blocks, preState, postState = tests.GetBellatrixRandom()
	} else if v == clparams.CapellaVersion {
		bcfg.AltairForkEpoch = 1
		bcfg.BellatrixForkEpoch = 1
		bcfg.CapellaForkEpoch = 1
		blocks, preState, postState = tests.GetCapellaRandom()
	}
	fcu = forkchoice.NewForkChoiceStorageMock()
	db = memdb.NewTestDB(t)
	blobDb := memdb.NewTestDB(t)
	var reader *tests.MockBlockReader
	reader = tests.LoadChain(blocks, postState, db, t)
	firstBlockRoot, _ := blocks[0].Block.HashSSZ()
	firstBlockHeader := blocks[0].SignedBeaconBlockHeader()

	bcfg.InitializeForkSchedule()

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &bcfg, datadir.New("/tmp"), nil, db, nil, reader, logger, true, true, false)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	// historical states reader below
	statesReader := historical_states_reader.NewHistoricalStatesReader(&bcfg, reader, vt, preState)
	opPool = pool.NewOperationsPool(&bcfg)
	fcu.Pool = opPool
	syncedData = synced_data.NewSyncedDataManager(true, &bcfg)
	gC := clparams.GenesisConfigs[clparams.MainnetNetwork]
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, &bcfg, &gC)
	blobStorage.WriteBlobSidecars(ctx, firstBlockRoot, []*cltypes.BlobSidecar{
		{
			Index:                    0,
			Blob:                     cltypes.Blob{byte(1)},
			SignedBlockHeader:        firstBlockHeader,
			KzgCommitment:            [48]byte{69},
			CommitmentInclusionProof: solid.NewHashVector(17),
		},
		{
			Index:                    1,
			Blob:                     cltypes.Blob{byte(2)},
			SignedBlockHeader:        firstBlockHeader,
			KzgCommitment:            [48]byte{1},
			CommitmentInclusionProof: solid.NewHashVector(17),
		},
	})
	vp = validator_params.NewValidatorParams()
	h = NewApiHandler(
		logger,
		&gC,
		&bcfg,
		db,
		fcu,
		opPool,
		reader,
		syncedData,
		statesReader,
		nil,
		"test-version", &beacon_router_configuration.RouterConfiguration{
			Beacon:     true,
			Node:       true,
			Builder:    true,
			Config:     true,
			Debug:      true,
			Events:     true,
			Validator:  true,
			Lighthouse: true,
		}, nil, blobStorage, nil, vp, nil, nil) // TODO: add tests
	h.Init()
	return
}
