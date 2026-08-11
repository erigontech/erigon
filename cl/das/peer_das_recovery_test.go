package das

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
)

func newRecoveryTestPeerDas(t *testing.T) (*peerdas, *mock_services.MockDataColumnStorage, *mock_services.MockBlobStorage) {
	t.Helper()
	ctrl := gomock.NewController(t)
	columns := mock_services.NewMockDataColumnStorage(ctrl)
	blobs := mock_services.NewMockBlobStorage(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.NumberOfColumns = 4
	return &peerdas{
		beaconConfig:      &cfg,
		columnStorage:     columns,
		blobStorage:       blobs,
		recoverBlobsQueue: make(chan recoverBlobsRequest, 1),
		isRecovering:      make(map[common.Hash]*blobRecovery),
	}, columns, blobs
}

func TestCanceledForcedRecoveryIsDroppedBeforeWork(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x01")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(10), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(10), root).Return([]uint64{0, 1}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- d.ForceScheduleRecover(ctx, 10, root, 2) }()
	request := <-d.recoverBlobsQueue
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	called := false
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) {
		called = true
	})
	require.False(t, called)
}

func TestForcedRecoveryCoalescesOntoActiveResult(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x02")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(20), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(20), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(20), root).Return([]*cltypes.BlobSidecar{{}}, true, nil)
	d.isRecovering[root] = &blobRecovery{}

	result := make(chan error, 1)
	go func() { result <- d.ForceScheduleRecover(t.Context(), 20, root, 2) }()
	request := <-d.recoverBlobsQueue
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) {
		t.Fatal("coalesced request started duplicate recovery")
	})

	select {
	case err := <-result:
		t.Fatalf("forced recovery returned before active recovery completed: %v", err)
	default:
	}
	d.finishBlobRecovery(root, nil)
	require.ErrorContains(t, <-result, "blob recovery did not complete")
}

func TestForcedRecoveryRechecksCompletedDataAfterQueueDelay(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x03")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return(nil, false, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(30), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return([]*cltypes.BlobSidecar{{}, {}}, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(30), root).Return([]*cltypes.BlobSidecar{{}, {}}, true, nil)

	result := make(chan error, 1)
	go func() { result <- d.ForceScheduleRecover(t.Context(), 30, root, 2) }()
	request := <-d.recoverBlobsQueue
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) {
		t.Fatal("completed queued request started stale forced recovery")
	})
	require.NoError(t, <-result)
}

func TestForcedRecoveryRejectsUnderreportedBlobMetadata(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x04")
	oneBlob := []*cltypes.BlobSidecar{{}}
	twoBlobs := []*cltypes.BlobSidecar{{}, {}}
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(oneBlob, true, nil)
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(40), root).Return([]uint64{0, 1}, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(oneBlob, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(twoBlobs, true, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(40), root).Return(twoBlobs, true, nil)

	result := make(chan error, 1)
	go func() { result <- d.ForceScheduleRecover(t.Context(), 40, root, 2) }()
	request := <-d.recoverBlobsQueue
	called := false
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) {
		called = true
	})
	require.True(t, called)
	require.NoError(t, <-result)
}
