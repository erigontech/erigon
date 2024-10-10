package services

import (
	"context"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
)

//go:embed test_data/blob_sidecar_service_blob.ssz_snappy
var blobSidecarServiceBlob []byte

//go:embed test_data/blob_sidecar_service_block.ssz_snappy
var blobSidecarServiceBlock []byte

//go:embed test_data/blob_sidecar_service_state.ssz_snappy
var blobSidecarServiceState []byte

func getObjectsForBlobSidecarServiceTests(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.BlobSidecar) {
	stateObj := state.New(&clparams.MainnetBeaconConfig)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	blob := cltypes.Blob{}
	require.NoError(t, utils.DecodeSSZSnappy(stateObj, blobSidecarServiceState, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(block, blobSidecarServiceBlock, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(&blob, blobSidecarServiceBlob, int(clparams.DenebVersion)))
	var proof libcommon.Bytes48
	proofStr := "0xb5a64254a75a8dd4e5fde529e2f657de268fcb9eedff43363c946f40bbf36ef16ee13a890504a7a8c4f689085146ad51"
	proofBytes := common.Hex2Bytes(proofStr[2:])
	copy(proof[:], proofBytes)
	sidecar := &cltypes.BlobSidecar{
		Index:             uint64(0),
		SignedBlockHeader: block.SignedBeaconBlockHeader(),
		Blob:              blob,
		KzgCommitment:     common.Bytes48(*block.Block.Body.BlobKzgCommitments.Get(0)),
		KzgProof:          proof,
	}
	return stateObj, block, sidecar
}

func setupBlobSidecarService(t *testing.T, ctrl *gomock.Controller, test bool) (BlobSidecarsService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	ctx := context.Background()
	ctx2, cn := context.WithTimeout(ctx, 1)
	cn()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(true, cfg)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	blockService := NewBlobSidecarService(ctx2, cfg, forkchoiceMock, syncedDataManager, ethClock, test)
	return blockService, syncedDataManager, ethClock, forkchoiceMock
}

func TestBlobServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, _, _, _ := setupBlobSidecarService(t, ctrl, false)

	require.Error(t, blobService.ProcessMessage(context.Background(), nil, &cltypes.BlobSidecar{}))
}

func TestBlobServiceInvalidIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, _, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, _ := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)

	require.Error(t, blobService.ProcessMessage(context.Background(), nil, &cltypes.BlobSidecar{
		Index: 99999,
	}))
}

func TestBlobServiceInvalidSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, _, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, _ := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(99999)

	require.Error(t, blobService.ProcessMessage(context.Background(), &sn, &cltypes.BlobSidecar{
		Index: 0,
	}))
}

func TestBlobServiceBadTimings(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(0)

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	require.Error(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}

func TestBlobServiceAlreadyHave(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(0)
	sidecarRoot, err := blobSidecar.SignedBlockHeader.Header.HashSSZ()
	require.NoError(t, err)

	fcu.Headers[sidecarRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.Error(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}

func TestBlobServiceDontHaveParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(0)

	// fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.Error(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}

func TestBlobServiceInvalidSidecarSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(0)

	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.Error(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}

func TestBlobServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, true)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	syncedData.OnHeadState(stateObj)
	sn := uint64(0)

	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()
	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot].Slot--
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.NoError(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}
