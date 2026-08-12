// Copyright 2026 The Erigon Authors
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

package das

import (
	"context"
	"errors"
	"testing"
	"time"

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
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		called = true
		return nil
	})
	require.False(t, called)
}

func TestAdmittedRecoverySurvivesCallerCancellation(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x05")
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(50), root).Return(nil, false, nil)
	blobs.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(50), root).Return([]*cltypes.BlobSidecar{{}}, true, nil)
	callerCtx, cancelCaller := context.WithCancel(t.Context())
	result := make(chan error, 1)
	request := recoverBlobsRequest{slot: 50, blockRoot: root, expectedBlobs: 1, force: true, ctx: callerCtx, result: result}
	d.handleRecoverBlobsRequest(t.Context(), request, func(ownerCtx context.Context, _ recoverBlobsRequest) error {
		cancelCaller()
		require.NoError(t, ownerCtx.Err())
		return nil
	})
	require.NoError(t, <-result)
}

func TestLiveRecoveryCoalescesRetryAfterOwnerFailure(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, maxBlobRecoveryWaiters+2)
	root := common.HexToHash("0x06")
	otherRoot := common.HexToHash("0x16")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 60, blockRoot: root}, func(context.Context, recoverBlobsRequest) error {
			close(started)
			<-release
			return errors.New("owner failed")
		})
	}()
	<-started
	for range maxBlobRecoveryWaiters {
		require.NoError(t, d.TryScheduleRecover(60, root))
	}
	d.recoveringMutex.Lock()
	waiterCount := len(d.isRecovering[root].waiters)
	d.recoveringMutex.Unlock()
	close(release)
	<-done
	require.Zero(t, waiterCount)

	require.NoError(t, d.TryScheduleRecover(61, otherRoot))
	requests := []recoverBlobsRequest{<-d.recoverBlobsQueue, <-d.recoverBlobsQueue}
	counts := map[common.Hash]int{}
	for _, request := range requests {
		counts[request.blockRoot]++
	}
	require.Equal(t, 1, counts[root])
	require.Equal(t, 1, counts[otherRoot])
	require.Empty(t, d.recoverBlobsQueue)
}

func TestDequeuedDuplicateRecoveryRetriesOnceAfterOwnerFailure(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	d.recoverBlobsQueue = make(chan recoverBlobsRequest, 3)
	root := common.HexToHash("0x17")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(71), root).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()

	require.NoError(t, d.TryScheduleRecover(71, root))
	require.NoError(t, d.TryScheduleRecover(71, root))
	first := <-d.recoverBlobsQueue
	duplicate := <-d.recoverBlobsQueue

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	callbacks := 0
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), first, func(context.Context, recoverBlobsRequest) error {
			callbacks++
			close(started)
			<-release
			return errors.New("owner failed")
		})
	}()
	<-started
	d.handleRecoverBlobsRequest(t.Context(), duplicate, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("dequeued duplicate started concurrent recovery")
		return nil
	})
	close(release)
	<-done

	select {
	case retry := <-d.recoverBlobsQueue:
		d.handleRecoverBlobsRequest(t.Context(), retry, func(context.Context, recoverBlobsRequest) error {
			callbacks++
			return errors.New("retry remained incomplete")
		})
	case <-time.After(time.Second):
		t.Fatal("dequeued duplicate did not preserve a retry after owner failure")
	}
	require.Equal(t, 2, callbacks)
	require.Empty(t, d.recoverBlobsQueue)
}

func TestRecoveryOwnerStopsWhenWorkerIsCanceled(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x26")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()
	workerCtx, cancelWorker := context.WithCancel(t.Context())
	entered := make(chan struct{})
	release := make(chan struct{})
	callbackResult := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(workerCtx, recoverBlobsRequest{slot: 62, blockRoot: root}, func(ownerCtx context.Context, _ recoverBlobsRequest) error {
			close(entered)
			select {
			case <-ownerCtx.Done():
				callbackResult <- ownerCtx.Err()
				return ownerCtx.Err()
			case <-release:
				return errors.New("test released callback")
			}
		})
	}()
	<-entered
	cancelWorker()

	select {
	case err := <-callbackResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("worker cancellation did not reach recovery callback")
	}
	<-done
}

func TestLiveRecoveryRequeuesWhenCoalescedOwnerRemainsIncomplete(t *testing.T) {
	d, columns, blobs := newRecoveryTestPeerDas(t)
	d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}
	root := common.HexToHash("0x07")
	columns.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(70), root).Return([]uint64{0, 1}, nil).AnyTimes()
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).AnyTimes()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 70, blockRoot: root}, func(context.Context, recoverBlobsRequest) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	require.NoError(t, d.TryScheduleRecover(70, root))
	close(release)
	<-done

	select {
	case request := <-d.recoverBlobsQueue:
		require.Equal(t, uint64(70), request.slot)
		require.Equal(t, root, request.blockRoot)
	case <-time.After(time.Second):
		t.Fatal("incomplete coalesced recovery was not requeued")
	}
}

func TestNonForcedRecoveryBroadcastsCallbackFailure(t *testing.T) {
	d, _, blobs := newRecoveryTestPeerDas(t)
	root := common.HexToHash("0x08")
	wantErr := errors.New("column recovery failed")
	blobs.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(0), nil).Times(2)
	result := make(chan error, 1)

	d.handleRecoverBlobsRequest(t.Context(), recoverBlobsRequest{slot: 80, blockRoot: root, result: result}, func(context.Context, recoverBlobsRequest) error {
		return wantErr
	})

	require.ErrorIs(t, <-result, wantErr)
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
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("coalesced request started duplicate recovery")
		return nil
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
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		t.Fatal("completed queued request started stale forced recovery")
		return nil
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
	d.handleRecoverBlobsRequest(t.Context(), request, func(context.Context, recoverBlobsRequest) error {
		called = true
		return nil
	})
	require.True(t, called)
	require.NoError(t, <-result)
}
