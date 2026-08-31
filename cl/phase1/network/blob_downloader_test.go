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
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blobstoragemock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type staticPeerDasGetter struct{ pd das.PeerDas }

func (s staticPeerDasGetter) GetPeerDas() das.PeerDas { return s.pd }

type completingBlobStorage struct {
	blob_storage.BlobStorage
	root     common.Hash
	sidecars []*cltypes.BlobSidecar
	once     sync.Once
	writeErr error
}

func (s *completingBlobStorage) KzgCommitmentsCount(ctx context.Context, root common.Hash) (uint32, error) {
	count, err := s.BlobStorage.KzgCommitmentsCount(ctx, root)
	if root == s.root {
		s.once.Do(func() { s.writeErr = s.BlobStorage.WriteBlobSidecars(ctx, root, s.sidecars) })
	}
	return count, errors.Join(err, s.writeErr)
}

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
		rpc:                   boundaryPeerCounter(1),
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
		rpc:                   boundaryPeerCounter(1),
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

func TestBlobHistoryDownloaderIncompleteFuluRecoveryWithholdsCompletionUntilRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	var countReads atomic.Int32
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash) (uint32, error) {
			if countReads.Add(1) >= 4 {
				return 1, nil
			}
			return 0, nil
		},
	).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, gomock.Any()).Return(
		[]*cltypes.BlobSidecar{storedFuluSidecar(block, 0)}, true, nil,
	).AnyTimes()
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = 20 * time.Millisecond
	downloader.verifyBlobSidecars = func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
		return nil
	}
	var notified atomic.Int32
	downloader.SetNotifyBlobBackfilled(func(bool) { notified.Add(1) })

	require.NoError(t, downloader.downloadOnce(false))
	require.Zero(t, notified.Load())
	require.False(t, downloader.backfillCompleted.Load())
	require.NotEmpty(t, downloader.retryRanges)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, int32(1), notified.Load())
	require.True(t, downloader.backfillCompleted.Load())
	require.Empty(t, downloader.retryRanges)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, int32(1), notified.Load())
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
		rpc:                   boundaryPeerCounter(1),
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

func TestBlobHistoryDownloaderFuluRecoveryDoesNotDeleteConcurrentCompletion(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	storage := blob_storage.NewBlobStore(db, fs)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	blobs := []goethkzg.Blob{{1}, {2}}
	commitments := make([]goethkzg.KZGCommitment, len(blobs))
	for i := range blobs {
		commitment, err := kzg.Ctx().BlobToKZGCommitment(&blobs[i], 0)
		require.NoError(t, err)
		commitments[i] = commitment
		block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	full := make([]*cltypes.BlobSidecar, len(blobs))
	for i := range blobs {
		proof, err := kzg.Ctx().ComputeBlobKZGProof(&blobs[i], commitments[i], 0)
		require.NoError(t, err)
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusionProof.Set(j, common.Hash(branch[j]))
		}
		full[i] = cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&blobs[i]), common.Bytes48(commitments[i]), common.Bytes48(proof), block.SignedBeaconBlockHeader(), inclusionProof)
	}
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, full[:1]))
	completing := &completingBlobStorage{BlobStorage: storage, root: root, sidecars: full}

	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		rpc:                   boundaryPeerCounter(1),
		blobStorage:           completing,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Second,
		verifyBlobSidecars: func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
			return nil
		},
		logger: log.New(),
	}

	require.True(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
	fresh := blob_storage.NewBlobStore(db, fs)
	sidecars, found, err := fresh.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, sidecars, 2)
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
		rpc:                   boundaryPeerCounter(1),
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

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		rpc:                   boundaryPeerCounter(1),
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
		rpc:                   boundaryPeerCounter(1),
		blobStorage:           blobStorage,
		columnBackfillTimeout: time.Second,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluRecoveryRetainsPartialCommitmentCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		rpc:                   boundaryPeerCounter(1),
		blobStorage:           blobStorage,
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: time.Nanosecond,
		logger:                log.New(),
	}

	require.False(t, downloader.recoverFuluColumns([]*cltypes.SignedBeaconBlock{block}))
}

func TestBlobHistoryDownloaderFuluRecoveryRetainsExcessCommitmentCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(3), nil).AnyTimes()

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := &BlobHistoryDownloader{
		ctx:                   t.Context(),
		rpc:                   boundaryPeerCounter(1),
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
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

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

func TestBlobHistoryDownloaderRetriesRecoveryAfterDurablePostcheckFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	countCalls := 0
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, common.Hash) (uint32, error) {
		countCalls++
		if countCalls <= 2 {
			return 0, nil
		}
		return 1, nil
	}).AnyTimes()

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	invalidSidecar := storedFuluSidecar(block, 0)
	invalidSidecar.KzgCommitment[0] = 1
	validSidecar := storedFuluSidecar(block, 0)
	readCalls := 0
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, gomock.Any()).DoAndReturn(func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
		readCalls++
		if readCalls <= 2 {
			return []*cltypes.BlobSidecar{invalidSidecar}, true, nil
		}
		return []*cltypes.BlobSidecar{validSidecar}, true, nil
	}).AnyTimes()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = 5 * time.Millisecond
	downloader.verifyBlobSidecars = func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
		return nil
	}

	require.NoError(t, downloader.downloadOnce(false))
	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, 3, readCalls)
}

func TestBlobHistoryDownloaderConservesEveryIncompleteFuluBlockAcrossPasses(t *testing.T) {
	tests := []struct {
		name              string
		completionAttempt map[uint64]int
	}{
		{
			name:              "first block fails",
			completionAttempt: map[uint64]int{8: 2, 7: 1},
		},
		{
			name:              "later block fails",
			completionAttempt: map[uint64]int{8: 1, 7: 2},
		},
		{
			name:              "first and later blocks fail",
			completionAttempt: map[uint64]int{8: 2, 7: 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			blocks := make(map[uint64]*cltypes.SignedBeaconBlock, 2)
			roots := make(map[common.Hash]uint64, 2)
			sidecars := make(map[uint64]*cltypes.BlobSidecar, 2)
			for _, slot := range []uint64{8, 7} {
				block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
				block.Block.Slot = slot
				block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
				root, err := block.Block.HashSSZ()
				require.NoError(t, err)
				blocks[slot] = block
				roots[root] = slot
				sidecars[slot] = storedFuluSidecar(block, 0)
			}

			attempts := make(map[uint64]int, 2)
			peerDas := mock_services.NewMockPeerDas(ctrl)
			peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, requested []cltypes.ColumnSyncableSignedBlock) error {
					require.Len(t, requested, 1)
					attempts[requested[0].GetSlot()]++
					return nil
				},
			).AnyTimes()
			blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
			blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, root common.Hash) (uint32, error) {
					slot := roots[root]
					if attempts[slot] >= tc.completionAttempt[slot] {
						return 1, nil
					}
					return 0, nil
				},
			).AnyTimes()
			blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, slot uint64, _ common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
					return []*cltypes.BlobSidecar{sidecars[slot]}, true, nil
				},
			).AnyTimes()

			downloader := newBoundaryDownloader(t, 1_000, 0, 1, &boundaryBlockReader{blocks: blocks})
			downloader.blobStorage = blobStorage
			downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
			downloader.columnBackfillTimeout = time.Millisecond
			downloader.verifyBlobSidecars = func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error {
				return nil
			}

			require.NoError(t, downloader.downloadOnce(false))
			require.NoError(t, downloader.downloadOnce(false))
			require.Equal(t, tc.completionAttempt, attempts)
			require.Empty(t, downloader.retryRanges)
		})
	}
}

func TestBlobHistoryDownloaderRetryDropsAlreadyCompleteDenebBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	block, sidecar := validDenebRecoverySidecar(t, 100)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), blockRoot).Return(uint32(1), nil).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, blockRoot).Return([]*cltypes.BlobSidecar{sidecar}, true, nil).AnyTimes()
	peer := &countingBlobPeerClient{responses: []*cltypes.BlobSidecar{sidecar}}
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	downloader.rpc = peer
	downloader.addRetrySlot(block.Block.Slot)

	require.NoError(t, downloader.downloadOnce(false))
	require.Zero(t, peer.requests)
	require.Empty(t, downloader.retryRanges)
}

func TestBlobHistoryDownloaderRetryRetainsDenebBlockOnStorageReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	block, _ := validDenebRecoverySidecar(t, 100)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	wantErr := errors.New("temporary storage failure")
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	countCalls := 0
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), blockRoot).DoAndReturn(func(context.Context, common.Hash) (uint32, error) {
		countCalls++
		if countCalls == 1 {
			return 0, wantErr
		}
		return 1, nil
	}).AnyTimes()
	peer := &countingBlobPeerClient{}
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	var logs bytes.Buffer
	downloader.logger.SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	downloader.blobStorage = blobStorage
	downloader.rpc = peer
	downloader.addRetrySlot(block.Block.Slot)

	require.NoError(t, downloader.downloadOnce(false))
	require.Zero(t, peer.requests)
	require.Equal(t, []blobRetryRange{{start: block.Block.Slot, end: block.Block.Slot, cursor: block.Block.Slot}}, downloader.retryRanges)
	require.Contains(t, logs.String(), "Failed to read stored blob sidecars during retry")
	require.Contains(t, logs.String(), "slot=100")
	require.Contains(t, logs.String(), wantErr.Error())
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
				rpc:                   boundaryPeerCounter(1),
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
		rpc:                   boundaryPeerCounter(1),
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
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func TestBlobHistoryDownloaderFuluBlockWithoutBlobsIgnoresStaleMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 100
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

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
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.False(t, notified)
	require.False(t, downloader.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderDenebWithoutBlobsIgnoresStaleStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	downloader := newBoundaryDownloader(t, block.Block.Slot, 0, block.Block.Slot, &boundaryBlockReader{block: block})
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func TestBlobHistoryDownloaderPostchecksPersistedDenebGroupAfterRequestCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	blockA, sidecarA := validDenebRecoverySidecar(t, 100)
	blockB, _ := validDenebRecoverySidecar(t, 101)
	rootA, err := blockA.Block.HashSSZ()
	require.NoError(t, err)

	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), rootA, []*cltypes.BlobSidecar{sidecarA}).DoAndReturn(
		func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			cancel()
			return nil
		},
	)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, root common.Hash) (uint32, error) {
			if root == rootA {
				return 1, nil
			}
			return 0, nil
		},
	).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), blockA.Block.Slot, rootA).Return(nil, false, nil)
	downloader := &BlobHistoryDownloader{
		ctx:         ctx,
		beaconCfg:   &clparams.MainnetBeaconConfig,
		rpc:         staticBlobPeerClient{responses: []*cltypes.BlobSidecar{sidecarA}},
		blobStorage: blobStorage,
		logger:      log.New(),
	}

	require.False(t, downloader.recoverDenebBlobs([]*cltypes.SignedBeaconBlock{blockA, blockB}))
	require.Len(t, downloader.retryRanges, 1)
	require.Equal(t, blockA.Block.Slot, downloader.retryRanges[0].start)
	require.Equal(t, blockB.Block.Slot, downloader.retryRanges[0].end)
}

func TestBlobHistoryDownloaderMixedDenebBatchIgnoresZeroCommitmentStorage(t *testing.T) {
	for _, zeroFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "zero first", false: "zero last"}[zeroFirst], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			zero := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
			zero.Block.Slot = 100
			nonzero, sidecar := validDenebRecoverySidecar(t, 101)
			nonzeroRoot, err := nonzero.Block.HashSSZ()
			require.NoError(t, err)

			blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
			blobStorage.EXPECT().WriteBlobSidecars(gomock.Any(), nonzeroRoot, []*cltypes.BlobSidecar{sidecar}).Return(nil)
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

type countingBlobPeerClient struct {
	responses []*cltypes.BlobSidecar
	requests  int
}

func (*countingBlobPeerClient) Peers() (uint64, error) { return 1, nil }

func (c *countingBlobPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	c.requests++
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
