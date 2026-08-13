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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das/mock_services"
	blobmock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func TestBlobHistoryDownloaderDoesNotCompleteWhenFuluRecoveryDoesNotPersist(t *testing.T) {
	const slot = uint64(100)
	reader := &lifecycleBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: lifecycleFuluBlock(slot)}}
	downloader := newLifecycleDownloader(t, lifecycleDownloaderOptions{headSlot: slot, targetSlot: slot, peers: 1, reader: reader})
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
	downloader := newLifecycleDownloader(t, lifecycleDownloaderOptions{headSlot: 1, peers: 1, reader: reader})

	require.NoError(t, downloader.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{1, 0}, reader.slots)
}

func TestBlobHistoryDownloaderUnsyncedWaitObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	reader := &boundaryBlockReader{}
	downloader := newLifecycleDownloader(t, lifecycleDownloaderOptions{headSlot: 1, peers: 1, reader: reader})
	downloader.ctx = ctx
	downloader.syncedChecker = &lifecycleEventuallySynced{}

	started := time.Now()
	err := downloader.downloadOnce(false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func lifecycleFuluBlock(slot uint64) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	return block
}

type lifecycleDownloaderOptions struct {
	headSlot   uint64
	targetSlot uint64
	peers      uint64
	reader     freezeblocks.BeaconSnapshotReader
}

func newLifecycleDownloader(t *testing.T, options lifecycleDownloaderOptions) *BlobHistoryDownloader {
	t.Helper()
	return newBoundaryDownloader(t, options.headSlot, 0, options.targetSlot, options.peers, options.reader)
}

type lifecycleBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
	slots  []uint64
}

func (r *lifecycleBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	return r.blocks[slot], nil
}

type lifecycleEventuallySynced struct {
	calls atomic.Uint64
}

func (s *lifecycleEventuallySynced) Synced() bool { return s.calls.Add(1) > 1 }
