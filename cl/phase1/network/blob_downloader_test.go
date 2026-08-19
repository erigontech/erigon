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

package network

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/das/mock_services"
	blobstoragemock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
)

type staticPeerDasGetter struct{ pd das.PeerDas }

func (s staticPeerDasGetter) GetPeerDas() das.PeerDas { return s.pd }

// A historical fulu block whose PeerDAS data columns are served by no peer (older
// than the network custody window) makes DownloadColumnsAndRecoverBlobs block until
// its context is cancelled. Column recovery must be bounded per block so the archive
// blob backfill cannot hang forever holding the index read tx.
func TestBlobHistoryDownloaderFuluColumnRecoveryIsBounded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().
		DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ []cltypes.ColumnSyncableSignedBlock) error {
			<-ctx.Done() // never recovers — unblocks only when the per-attempt ctx expires
			return ctx.Err()
		}).
		AnyTimes()
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)

	b := &BlobHistoryDownloader{
		ctx:                   context.Background(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: 50 * time.Millisecond,
		logger:                log.New(),
	}

	fulu := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	fulu.Block.Slot = 100
	fulu.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})

	done := make(chan struct{})
	go func() {
		b.recoverFuluColumns([]*cltypes.SignedBeaconBlock{fulu})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recoverFuluColumns hung — unbounded PeerDAS column recovery")
	}
}

func TestBlobHistoryDownloaderFuluInitialStorageCheckUsesBlockTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(context.Canceled).AnyTimes()
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ common.Hash) (uint32, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   ctx,
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: 20 * time.Millisecond,
		logger:                log.New(),
	}
	done := make(chan bool, 1)
	go func() { done <- downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}) }()

	bounded := false
	select {
	case result := <-done:
		bounded = !result
	case <-time.After(100 * time.Millisecond):
		cancel()
		<-done
	}
	require.True(t, bounded, "initial durable check exceeded the per-block timeout")
}

func TestBlobHistoryDownloaderMixedBatchAttemptsFuluAfterDenebFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)

	deneb := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	deneb.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	fulu := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		beaconCfg:             &clparams.MainnetBeaconConfig,
		rpc:                   boundaryPeerCounter(1),
		blobStorage:           blobStorage,
		columnBackfillTimeout: time.Second,
		logger:                log.New(),
	}

	require.False(t, downloader.processBatch([]*cltypes.SignedBeaconBlock{deneb, fulu}))
}

func TestBlobHistoryDownloaderMixedBatchCancellationDoesNotStartFulu(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	deneb := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	deneb.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	fulu := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	downloader := &BlobHistoryDownloader{
		ctx:                   ctx,
		beaconCfg:             &clparams.MainnetBeaconConfig,
		rpc:                   cancelingBlobPeerClient{cancel: cancel},
		blobStorage:           blobstoragemock.NewMockBlobStorage(ctrl),
		columnBackfillTimeout: time.Second,
		logger:                log.New(),
	}

	require.False(t, downloader.processBatch([]*cltypes.SignedBeaconBlock{deneb, fulu}))
}

func TestBlobHistoryDownloaderIncompleteFuluRecoveryDoesNotBlockScanCompletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = 20 * time.Millisecond
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, notified)
	require.True(t, downloader.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderCompletedFuluRecoveryMeetsDurablePostcondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*cltypes.BlobSidecar{storedFuluSidecar(block, 0)}, true, nil)
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Second,
		verifyBlobSidecars: func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
			return nil
		},
		logger: log.New(),
	}

	require.True(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluRecoveryWaitsForAsyncPersistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	var reads atomic.Int32
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, common.Hash) (uint32, error) {
		if reads.Add(1) < 3 {
			return 0, nil
		}
		return 1, nil
	}).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*cltypes.BlobSidecar{storedFuluSidecar(block, 0)}, true, nil)
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Second,
		verifyBlobSidecars: func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
			return nil
		},
		logger: log.New(),
	}

	require.True(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
	require.GreaterOrEqual(t, reads.Load(), int32(3))
}

func TestBlobHistoryDownloaderFuluRecoveryRejectsStaleCommitmentCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).Times(2)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false, nil).Times(2)
	blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Nanosecond,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluTransientReadErrorDoesNotRemoveStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false, errors.New("temporary read failure"))
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		columnBackfillTimeout: time.Second,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluRecoveryClearsPartialCommitmentCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Nanosecond,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluRecoveryClearsExcessCommitmentCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(3), nil)
	blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Nanosecond,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderCountEqualFuluSkipsDeepScanValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func TestBlobHistoryDownloaderDoesNotDeepVerifyCountEqualStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	verifyCalls := 0
	downloader.verifyBlobSidecars = func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
		verifyCalls++
		return nil
	}

	require.NoError(t, downloader.downloadOnce(false))
	require.NoError(t, downloader.downloadOnce(false))
	require.Zero(t, verifyCalls)
}

func TestBlobHistoryDownloaderFuluRecoveryRejectsInvalidStoredSidecars(t *testing.T) {
	tests := map[string]func(*cltypes.BlobSidecar){
		"missing header": func(sidecar *cltypes.BlobSidecar) { sidecar.SignedBlockHeader = nil },
		"wrong index":    func(sidecar *cltypes.BlobSidecar) { sidecar.Index = 1 },
		"wrong commitment": func(sidecar *cltypes.BlobSidecar) {
			sidecar.KzgCommitment = common.Bytes48{1}
		},
		"wrong root": func(sidecar *cltypes.BlobSidecar) { sidecar.SignedBlockHeader.Header.Slot++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			peerDas := mock_services.NewMockPeerDas(ctrl)
			peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
			blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
			block.Block.Slot = 100
			block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
			sidecar := storedFuluSidecar(block, 0)
			mutate(sidecar)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
			blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*cltypes.BlobSidecar{sidecar}, true, nil)
			downloader := &BlobHistoryDownloader{
				ctx:                   t.Context(),
				blobStorage:           blobStorage,
				peerDasGetter:         staticPeerDasGetter{pd: peerDas},
				columnBackfillTimeout: time.Nanosecond,
				verifyBlobSidecars: func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
					return nil
				},
				logger: log.New(),
			}

			require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
		})
	}
}

func TestBlobHistoryDownloaderFuluRecoveryRejectsFailedStoredSidecarVerification(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return([]*cltypes.BlobSidecar{storedFuluSidecar(block, 0)}, true, nil)
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Nanosecond,
		verifyBlobSidecars: func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
			return errors.New("invalid blob proof")
		},
		logger: log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluBlockWithoutBlobsCompletes(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func TestBlobHistoryDownloaderFuluBlockWithoutBlobsClearsStaleMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).Times(2)
	blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func storedFuluSidecar(block *cltypes.SignedBeaconBlock, index int) *cltypes.BlobSidecar {
	return &cltypes.BlobSidecar{
		Index:             uint64(index),
		KzgCommitment:     common.Bytes48(*block.GetBlobKzgCommitments().Get(index)),
		SignedBlockHeader: block.SignedBeaconBlockHeader(),
	}
}

func TestDenebRecoveryBatchRejectsMalformedCandidates(t *testing.T) {
	id := &cltypes.BlobIdentifier{}
	batch := &denebRecoveryBatch{
		groups: map[common.Hash]*requestedBlobBlock{
			{}: {ids: []*cltypes.BlobIdentifier{id}, sidecars: make(map[uint64]*cltypes.BlobSidecar)},
		},
		order: []common.Hash{{}},
	}
	tests := map[string]*PeerAndSidecars{
		"empty candidate":   {},
		"nil sidecar":       {Responses: []*cltypes.BlobSidecar{nil}},
		"nil signed header": {Responses: []*cltypes.BlobSidecar{{}}},
		"nil header": {Responses: []*cltypes.BlobSidecar{{
			SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{},
		}}},
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := batch.validate(batch.remaining(), candidate.Responses)
			require.Error(t, err)
		})
	}
}

func TestDenebRecoveryBatchAccumulatesComplementaryPartialsAndStoresCompleteGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage := blobstoragemock.NewMockBlobStorage(ctrl)
	block, req, sidecars := denebRecoveryFixture(t, 2)
	batch, err := newDenebRecoveryBatch([]*cltypes.SignedBeaconBlock{block}, req)
	require.NoError(t, err)
	batch.verifier = func([]*cltypes.BlobSidecar, func(*cltypes.SignedBeaconBlockHeader) error) error { return nil }

	progress, err := batch.validate(req, []*cltypes.BlobSidecar{sidecars[1]})
	require.NoError(t, err)
	require.Equal(t, 1, progress)
	_, err = batch.store(t.Context(), storage)
	require.NoError(t, err)
	require.Equal(t, uint64(0), batch.remaining().Get(0).Index)

	storage.EXPECT().WriteBlobSidecars(gomock.Any(), req.Get(0).BlockRoot, []*cltypes.BlobSidecar{sidecars[0], sidecars[1]}).Return(nil)
	progress, err = batch.validate(batch.remaining(), []*cltypes.BlobSidecar{sidecars[0]})
	require.NoError(t, err)
	require.Equal(t, 1, progress)
	_, err = batch.store(t.Context(), storage)
	require.NoError(t, err)
	require.Zero(t, batch.remaining().Len())
}

func TestDenebRecoveryBatchAcceptsUsefulOverlapFromInflightRequest(t *testing.T) {
	block, req, sidecars := denebRecoveryFixture(t, 2)
	batch, err := newDenebRecoveryBatch([]*cltypes.SignedBeaconBlock{block}, req)
	require.NoError(t, err)
	batch.verifier = func([]*cltypes.BlobSidecar, func(*cltypes.SignedBeaconBlockHeader) error) error { return nil }

	progress, err := batch.validate(req, []*cltypes.BlobSidecar{sidecars[0]})
	require.NoError(t, err)
	require.Equal(t, 1, progress)

	progress, err = batch.validate(req, []*cltypes.BlobSidecar{sidecars[0], sidecars[1]})
	require.NoError(t, err)
	require.Equal(t, 1, progress)
	require.Zero(t, batch.remaining().Len())
}

func TestDenebRecoveryBatchRetriesStorageWithoutRefetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage := blobstoragemock.NewMockBlobStorage(ctrl)
	block, req, sidecars := denebRecoveryFixture(t, 1)
	batch, err := newDenebRecoveryBatch([]*cltypes.SignedBeaconBlock{block}, req)
	require.NoError(t, err)
	batch.verifier = func([]*cltypes.BlobSidecar, func(*cltypes.SignedBeaconBlockHeader) error) error { return nil }
	storage.EXPECT().WriteBlobSidecars(gomock.Any(), req.Get(0).BlockRoot, sidecars).Return(errors.New("temporary write failure"))
	storage.EXPECT().WriteBlobSidecars(gomock.Any(), req.Get(0).BlockRoot, sidecars).Return(nil)

	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	validationReady := make(chan struct{}, 2)
	var requests atomic.Int32
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		requests.Add(1)
		return sidecars, "peer", nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, batch.remaining, func(ctx context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
			progress, err := batch.validate(candidate.requested, candidate.Responses)
			if err != nil {
				return false, false, err
			}
			stored, err := batch.store(ctx, storage)
			if err != nil {
				return progress > 0 || batch.hasCompleteUnstoredGroup(), false, err
			}
			return progress > 0 || stored > 0, batch.complete(), nil
		}, blobBackfillRequestSchedule{
			ticks: ticks, expires: expires, now: func() time.Time { return time.Unix(100, 0) },
			validationReady: func() { validationReady <- struct{}{} },
		})
		done <- err
	}()

	receiveBlobTestSignal(t, validationReady)
	ticks <- time.Unix(101, 0)
	require.NoError(t, receiveBlobTestValue(t, done))
	require.Equal(t, int32(1), requests.Load())
	require.True(t, batch.complete())
}

func TestDenebRecoveryBatchInvalidResponsesDoNotPoisonAccumulator(t *testing.T) {
	block, req, sidecars := denebRecoveryFixture(t, 2)
	batch, err := newDenebRecoveryBatch([]*cltypes.SignedBeaconBlock{block}, req)
	require.NoError(t, err)
	batch.verifier = func([]*cltypes.BlobSidecar, func(*cltypes.SignedBeaconBlockHeader) error) error { return nil }

	_, err = batch.validate(req, []*cltypes.BlobSidecar{sidecars[0], sidecars[0]})
	require.Error(t, err)
	unrequested := *sidecars[0]
	unrequested.Index = 3
	_, err = batch.validate(req, []*cltypes.BlobSidecar{&unrequested})
	require.Error(t, err)
	require.Equal(t, 2, batch.remaining().Len())

	progress, err := batch.validate(req, []*cltypes.BlobSidecar{sidecars[0]})
	require.NoError(t, err)
	require.Equal(t, 1, progress)
	require.Equal(t, uint64(1), batch.remaining().Get(0).Index)
}

func denebRecoveryFixture(t *testing.T, count int) (*cltypes.SignedBeaconBlock, *solid.ListSSZ[*cltypes.BlobIdentifier], []*cltypes.BlobSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	for range count {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	req, err := BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	header := block.SignedBeaconBlockHeader()
	sidecars := make([]*cltypes.BlobSidecar, count)
	for i := range count {
		sidecars[i] = &cltypes.BlobSidecar{Index: uint64(i), SignedBlockHeader: header}
	}
	return block, req, sidecars
}

func TestBlobHistoryDownloaderFailedDenebRequestWithholdsCompletionNotification(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil)
	ctx, cancel := context.WithCancel(t.Context())
	client := cancelingBlobPeerClient{cancel: cancel}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.ctx = ctx
	downloader.rpc = client
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.False(t, notified)
	require.False(t, downloader.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderDenebWithoutBlobsRepairsStaleStorage(t *testing.T) {
	tests := map[string]struct {
		stored      uint32
		removeErr   error
		wantRemoved bool
		wantDone    bool
	}{
		"clean":           {wantDone: true},
		"stale":           {stored: 1, wantRemoved: true, wantDone: true},
		"removal failure": {stored: 1, removeErr: errors.New("remove failed"), wantRemoved: true, wantDone: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
			countCalls := 2
			if test.stored == 0 {
				countCalls = 1
			}
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(test.stored, nil).Times(countCalls)
			if test.wantRemoved {
				blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(test.removeErr)
			}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
			block.Block.Slot = 100
			downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
			downloader.blobStorage = blobStorage
			notified := false
			downloader.SetNotifyBlobBackfilled(func() { notified = true })

			require.NoError(t, downloader.downloadOnce(false))
			require.Equal(t, test.wantDone, downloader.backfillCompleted.Load())
			require.Equal(t, test.wantDone, notified)
		})
	}
}

func TestBlobHistoryDownloaderMixedDenebBatchRepairsZeroCommitmentStorage(t *testing.T) {
	for _, zeroFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "zero first", false: "zero last"}[zeroFirst], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			zero := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
			zero.Block.Slot = 100
			nonzero, sidecar := validDenebRecoverySidecar(t, 101)
			zeroRoot, err := zero.Block.HashSSZ()
			require.NoError(t, err)
			nonzeroRoot, err := nonzero.Block.HashSSZ()
			require.NoError(t, err)

			blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), zeroRoot).Return(uint32(1), nil)
			blobStorage.EXPECT().RemoveBlobSidecars(gomock.Any(), zero.Block.Slot, zeroRoot).Return(nil)
			blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), nonzeroRoot, []*cltypes.BlobSidecar{sidecar}).Return(nil)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), zeroRoot).Return(uint32(0), nil)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), nonzeroRoot).Return(uint32(1), nil)
			blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), nonzero.Block.Slot, nonzeroRoot).Return([]*cltypes.BlobSidecar{sidecar}, true, nil)
			blocks := []*cltypes.SignedBeaconBlock{nonzero, zero}
			if zeroFirst {
				blocks[0], blocks[1] = blocks[1], blocks[0]
			}
			downloader := &BlobHistoryDownloader{
				ctx:         t.Context(),
				beaconCfg:   &clparams.MainnetBeaconConfig,
				rpc:         staticBlobPeerClient{responses: []*cltypes.BlobSidecar{sidecar}},
				blobStorage: blobStorage,
				logger:      log.New(),
			}

			require.True(t, downloader.recoverDenebBlobs(blocks))
		})
	}
}

func validDenebRecoverySidecar(t *testing.T, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar) {
	t.Helper()
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
	_, err = block.Block.HashSSZ()
	require.NoError(t, err)
	branch, err := block.Block.Body.KzgCommitmentMerkleProof(0)
	require.NoError(t, err)
	inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
	for i := range branch {
		inclusionProof.Set(i, common.Hash(branch[i]))
	}
	return block, cltypes.NewBlobSidecar(0, (*cltypes.Blob)(&blob), common.Bytes48(commitment), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusionProof)
}

type staticBlobPeerClient struct{ responses []*cltypes.BlobSidecar }

func (staticBlobPeerClient) Peers() (uint64, error) { return 1, nil }

func (c staticBlobPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return c.responses, "peer", nil
}

type cancelingBlobPeerClient struct {
	cancel context.CancelFunc
}

func (cancelingBlobPeerClient) Peers() (uint64, error) { return 1, nil }

func (c cancelingBlobPeerClient) SendBlobsSidecarByIdentifierReq(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	c.cancel()
	<-ctx.Done()
	return nil, "", ctx.Err()
}
