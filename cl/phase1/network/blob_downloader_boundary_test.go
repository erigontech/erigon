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

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das/mock_services"
	blobmock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func TestBlobHistoryDownloaderProcessesFirstUnfrozenSlot(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	wantErr := errors.New("first unfrozen slot visited")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot, firstUnfrozenSlot, firstUnfrozenSlot, 1, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
	require.Equal(t, []uint64{firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderBatchStopsAtFrozenBoundary(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot+1, firstUnfrozenSlot, 0, 1, reader)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{firstUnfrozenSlot + 1, firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderRefreshesFrozenBoundaryBetweenBatches(t *testing.T) {
	snapshot := &boundaryMutableSnapshot{}
	reader := &boundaryBlockReader{onRead: func(slot uint64) {
		if slot == 13 {
			snapshot.frozen.Store(13)
		}
	}}
	downloader := newBoundaryDownloader(t, 20, 0, 0, 1, reader)
	downloader.sn = snapshot

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
}

func TestBlobHistoryDownloaderRemovesWritesCoveredDuringRecovery(t *testing.T) {
	const slot = uint64(100)
	snapshot := &boundaryMutableSnapshot{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, reader)
	downloader.sn = snapshot
	ctrl := gomock.NewController(t)
	storage := blobmock.NewMockBlobStorage(ctrl)
	gomock.InOrder(
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil),
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil),
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil),
		storage.EXPECT().RemoveBlobSidecars(gomock.Any(), slot, gomock.Any()).Return(nil),
	)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, []cltypes.ColumnSyncableSignedBlock) error {
		snapshot.frozen.Store(slot + 1)
		return nil
	})
	downloader.blobStorage = storage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = time.Second

	require.NoError(t, downloader.downloadOnce(false))
}

func TestBlobHistoryDownloaderCleansDelayedRecoveryBelowFrozenBoundary(t *testing.T) {
	const slot = uint64(100)
	snapshot := &boundaryMutableSnapshot{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, reader)
	downloader.sn = snapshot
	ctrl := gomock.NewController(t)
	storage := blobmock.NewMockBlobStorage(ctrl)
	gomock.InOrder(
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil),
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil),
		storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil),
		storage.EXPECT().RemoveBlobSidecars(gomock.Any(), slot, gomock.Any()).Return(nil),
	)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	downloader.blobStorage = storage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = time.Second

	require.NoError(t, downloader.downloadOnce(false))
	require.Len(t, downloader.frozenBlobCleanup, 1)
	snapshot.frozen.Store(slot + 1)
	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, downloader.frozenBlobCleanup)
}

func TestBlobHistoryDownloaderRunsWithAvailablePeer(t *testing.T) {
	const slot = uint64(100)
	wantErr := errors.New("available peer used")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
}

func TestBlobHistoryDownloaderWaitsWithoutPeers(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 100, 0, 100, 0, reader)

	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, reader.slots)
}

func TestBlobHistoryDownloaderDoesNotCompleteWhenFuluRecoveryDoesNotPersist(t *testing.T) {
	const slot = uint64(100)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})

	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}
	downloader := newBoundaryDownloader(t, slot, 0, slot, 1, reader)
	ctrl := gomock.NewController(t)
	storage := blobmock.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).Times(4)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	downloader.blobStorage = storage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = time.Second
	downloader.backfillCompleted.Store(true)
	notified := false
	downloader.SetNotifyBlobBackfilled(func() { notified = true })

	require.NoError(t, downloader.downloadOnce(false))
	require.False(t, downloader.backfillCompleted.Load())
	require.False(t, notified)
	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{slot, slot}, reader.slots)
}

func TestBlobHistoryDownloaderFloorsLowSlotRetryTarget(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 1, 0, 0, 1, reader)

	require.NoError(t, downloader.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{1, 0}, reader.slots)
}

func TestBlobHistoryDownloaderUnsyncedWaitObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 1, 0, 0, 1, reader)
	downloader.ctx = ctx
	downloader.syncedChecker = &boundaryEventuallySynced{}

	started := time.Now()
	err := downloader.downloadOnce(false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func newBoundaryDownloader(t *testing.T, headSlot, frozenBlobs, targetSlot, peers uint64, reader freezeblocks.BeaconSnapshotReader) *BlobHistoryDownloader {
	t.Helper()
	downloader := &BlobHistoryDownloader{
		ctx:           t.Context(),
		beaconCfg:     &clparams.MainnetBeaconConfig,
		peerClient:    boundaryPeerClient{peers: peers},
		indiciesDB:    memdb.NewTestDB(t, dbcfg.ChainDB),
		blockReader:   reader,
		sn:            boundarySnapshot(frozenBlobs),
		syncedChecker: boundarySyncedChecker(true),
		targetSlot:    targetSlot,
		archiveBlobs:  true,
		logger:        log.New(),
	}
	downloader.headSlot.Store(headSlot)
	return downloader
}

type boundaryBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	slots  []uint64
	err    error
	onRead func(uint64)
	blocks map[uint64]*cltypes.SignedBeaconBlock
}

func (r *boundaryBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	if r.onRead != nil {
		r.onRead(slot)
	}
	return r.blocks[slot], r.err
}

type boundaryPeerClient struct {
	peers uint64
}

func (p boundaryPeerClient) Peers() (uint64, error) { return p.peers, nil }

func (boundaryPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", errors.New("unexpected blob request")
}

type boundarySnapshot uint64

func (s boundarySnapshot) FrozenBlobs() uint64 { return uint64(s) }

type boundaryMutableSnapshot struct {
	frozen atomic.Uint64
}

func (s *boundaryMutableSnapshot) FrozenBlobs() uint64 { return s.frozen.Load() }

type boundarySyncedChecker bool

func (s boundarySyncedChecker) Synced() bool { return bool(s) }

type boundaryEventuallySynced struct {
	calls atomic.Uint64
}

func (s *boundaryEventuallySynced) Synced() bool { return s.calls.Add(1) > 1 }
