// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"context"
	"math"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	mockMonitor "github.com/erigontech/erigon/cl/monitor/mock_services"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	mock_services2 "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
)

func setupTestingHandler(t *testing.T, v clparams.StateVersion, logger log.Logger) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, f afero.Fs, preState, postState *state.CachingBeaconState, h *ApiHandler, opPool pool.OperationsPool, syncedData *synced_data.SyncedDataManager, fcu *mock_services2.ForkChoiceStorageMock, vp *validator_params.ValidatorParams) {
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
	fcu = mock_services2.NewForkChoiceStorageMock(t)
	db = memdb.NewTestDB(t)
	blobDb := memdb.NewTestDB(t)
	var reader *tests.MockBlockReader
	reader = tests.LoadChain(blocks, postState, db, t)
	firstBlockRoot, _ := blocks[0].Block.HashSSZ()
	firstBlockHeader := blocks[0].SignedBeaconBlockHeader()

	bcfg.InitializeForkSchedule()

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &bcfg, datadir.New("/tmp"), nil, db, nil, reader, logger, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	// historical states reader below
	statesReader := historical_states_reader.NewHistoricalStatesReader(&bcfg, reader, vt, preState)
	opPool = pool.NewOperationsPool(&bcfg)
	fcu.Pool = opPool
	syncedData = synced_data.NewSyncedDataManager(true, &bcfg)
	genesis, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesis.GenesisTime(), genesis.GenesisValidatorsRoot(), &bcfg)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, &bcfg, ethClock)
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
	ctrl := gomock.NewController(t)
	syncCommitteeMessagesService := mock_services.NewMockSyncCommitteeMessagesService(ctrl)
	syncContributionService := mock_services.NewMockSyncContributionService(ctrl)
	aggregateAndProofsService := mock_services.NewMockAggregateAndProofService(ctrl)
	voluntaryExitService := mock_services.NewMockVoluntaryExitService(ctrl)
	blsToExecutionChangeService := mock_services.NewMockBLSToExecutionChangeService(ctrl)
	proposerSlashingService := mock_services.NewMockProposerSlashingService(ctrl)
	mockValidatorMonitor := mockMonitor.NewMockValidatorMonitor(ctrl)

	// ctx context.Context, subnetID *uint64, msg *cltypes.SyncCommitteeMessage) error
	syncCommitteeMessagesService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.SyncCommitteeMessage) error {
		return h.syncMessagePool.AddSyncCommitteeMessage(postState, *subnetID, msg)
	}).AnyTimes()

	syncContributionService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.SignedContributionAndProof) error {
		return h.syncMessagePool.AddSyncContribution(postState, msg.Message.Contribution)
	}).AnyTimes()
	aggregateAndProofsService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.SignedAggregateAndProof) error {
		opPool.AttestationsPool.Insert(msg.Message.Aggregate.Signature(), msg.Message.Aggregate)
		return nil
	}).AnyTimes()
	voluntaryExitService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.SignedVoluntaryExit) error {
		opPool.VoluntaryExitsPool.Insert(msg.VoluntaryExit.ValidatorIndex, msg)
		return nil
	}).AnyTimes()
	blsToExecutionChangeService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.SignedBLSToExecutionChange) error {
		opPool.BLSToExecutionChangesPool.Insert(msg.Signature, msg)
		return nil
	}).AnyTimes()
	proposerSlashingService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subnetID *uint64, msg *cltypes.ProposerSlashing) error {
		opPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(msg), msg)
		return nil
	}).AnyTimes()
	mockValidatorMonitor.EXPECT().ObserveValidator(gomock.Any()).AnyTimes()

	vp = validator_params.NewValidatorParams()
	h = NewApiHandler(
		logger,
		&clparams.NetworkConfig{},
		ethClock,
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
		}, nil, blobStorage, nil, vp, nil, nil, fcu.SyncContributionPool, nil, nil,
		syncCommitteeMessagesService,
		syncContributionService,
		aggregateAndProofsService,
		nil,
		voluntaryExitService,
		blsToExecutionChangeService,
		proposerSlashingService,
		nil,
		mockValidatorMonitor,
	) // TODO: add tests
	h.Init()
	return
}
