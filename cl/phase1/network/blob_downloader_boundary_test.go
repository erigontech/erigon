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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das/mock_services"
	blobstoragemock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func TestBlobHistoryDownloaderProcessesFirstUnfrozenSlot(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	wantErr := errors.New("first unfrozen slot visited")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot, firstUnfrozenSlot, firstUnfrozenSlot, reader)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
	require.Equal(t, []uint64{firstUnfrozenSlot}, reader.slots)
}

func TestBlobHistoryDownloaderBatchStopsAtFrozenBoundary(t *testing.T) {
	const firstUnfrozenSlot = uint64(100)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, firstUnfrozenSlot+1, firstUnfrozenSlot, 0, reader)

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
	downloader := newBoundaryDownloader(t, 20, 0, 0, reader)
	downloader.sn = snapshot

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
}

func TestBlobHistoryDownloaderRefreshesFrozenBoundaryAfterRetry(t *testing.T) {
	snapshot := &boundaryMutableSnapshot{}
	reader := &boundaryBlockReader{onRead: func(slot uint64) {
		if slot == 1 {
			snapshot.frozen.Store(15)
		}
	}}
	downloader := newBoundaryDownloader(t, 20, 0, 0, reader)
	downloader.sn = snapshot
	downloader.addRetrySlot(1)
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{1, 20, 19, 18, 17, 16, 15}, reader.slots)
	require.Empty(t, downloader.retryRanges)
	require.True(t, downloader.backfillCompleted.Load())
	require.True(t, notified)
}

func TestBlobHistoryDownloaderRunsWithSinglePeer(t *testing.T) {
	const slot = uint64(100)
	wantErr := errors.New("single peer admitted")
	reader := &boundaryBlockReader{err: wantErr}
	downloader := newBoundaryDownloader(t, slot, 0, slot, reader)
	downloader.rpc = boundaryPeerCounter(1)

	require.ErrorIs(t, downloader.downloadOnce(false), wantErr)
}

func TestBlobHistoryDownloaderWaitsWithoutPeers(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 100, 0, 100, reader)
	downloader.rpc = boundaryPeerCounter(0)

	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, reader.slots)
}

func TestBlobHistoryDownloaderStopsWhenPeersDisappear(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 20
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	reader := &boundaryBlockReader{block: block}
	downloader := newBoundaryDownloader(t, 20, 0, 0, reader)
	peers := &boundarySequencePeerCounter{counts: []uint64{1, 0}}
	downloader.rpc = peers
	downloader.blobStorage = blobStorage
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{20, 19, 18, 17, 16, 15, 14, 13}, reader.slots)
	require.Zero(t, peers.requests)
	require.False(t, notified)
	require.Zero(t, downloader.nextBackfillTargetSlot)
}

func TestBlobHistoryDownloaderNewRetryRevokesPriorCompletionBeforeCancellationReturn(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, []cltypes.ColumnSyncableSignedBlock) error {
			cancel()
			return context.Canceled
		},
	)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 20
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.GetSlot(), 0, block.GetSlot(), &boundaryBlockReader{block: block})
	downloader.ctx = ctx
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.backfillCompleted.Store(true)
	var downstreamComplete atomic.Bool
	var transitions atomic.Int32
	downstreamComplete.Store(true)
	downloader.SetNotifyBlobBackfilled(func(completed bool) {
		downstreamComplete.Store(completed)
		transitions.Add(1)
	})

	require.NoError(t, downloader.downloadOnce(false))
	require.NotEmpty(t, downloader.retryRanges)
	require.False(t, downloader.backfillCompleted.Load())
	require.False(t, downstreamComplete.Load())
	require.Equal(t, int32(1), transitions.Load())
	downloader.addRetrySlot(block.GetSlot())
	require.Equal(t, int32(1), transitions.Load(), "duplicate retry emitted another false transition")
}

func TestBlobHistoryDownloaderCompletionCallbackCanReplaceItself(t *testing.T) {
	downloader := &BlobHistoryDownloader{}
	done := make(chan struct{})
	downloader.SetNotifyBlobBackfilled(func(bool) {
		downloader.SetNotifyBlobBackfilled(nil)
		close(done)
	})

	go downloader.setBackfillCompleted(true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion callback ran while the callback lock was held")
	}
}

func TestBlobHistoryDownloaderRetryRechecksPeersBeforeDenebRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 20
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	ctx, cancel := context.WithCancel(t.Context())
	downloader := newBoundaryDownloader(t, block.GetSlot(), 0, block.GetSlot(), &boundaryBlockReader{block: block})
	downloader.ctx = ctx
	peers := &boundarySequencePeerCounter{counts: []uint64{1, 0}, onRequest: cancel}
	downloader.rpc = peers
	downloader.blobStorage = blobStorage
	downloader.addRetrySlot(block.GetSlot())

	require.NoError(t, downloader.downloadOnce(false))
	require.GreaterOrEqual(t, peers.calls, 2)
	require.Zero(t, peers.requests)
	require.Equal(t, []blobRetryRange{{start: block.GetSlot(), end: block.GetSlot(), cursor: block.GetSlot()}}, downloader.retryRanges)
}

func TestBlobHistoryDownloaderRechecksPeersBeforeFuluRecovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = 20
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	downloader := newBoundaryDownloader(t, block.GetSlot(), 0, block.GetSlot(), &boundaryBlockReader{block: block})
	peers := &boundarySequencePeerCounter{counts: []uint64{1, 1, 0}}
	downloader.rpc = peers
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	downloader.columnBackfillTimeout = time.Second

	require.NoError(t, downloader.downloadOnce(false))
	require.GreaterOrEqual(t, peers.calls, 3)
	require.Equal(t, []blobRetryRange{{start: block.GetSlot(), end: block.GetSlot(), cursor: block.GetSlot()}}, downloader.retryRanges)
}

func TestBlobHistoryDownloaderLocalOnlyPassChecksPeersOnce(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, blocksBatchSize*2+1, 0, 1, reader)
	peers := &boundarySequencePeerCounter{counts: []uint64{1}}
	downloader.rpc = peers

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, 1, peers.calls)
}

func TestBlobHistoryDownloaderUnsyncedWaitObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	downloader := newBoundaryDownloader(t, 20, 0, 0, &boundaryBlockReader{err: errors.New("retry ran while unsynced")})
	downloader.ctx = ctx
	downloader.addRetrySlot(20)
	downloader.syncedChecker = boundarySyncedCheckerFunc(func() bool {
		select {
		case <-entered:
		default:
			close(entered)
		}
		return false
	})
	done := make(chan error, 1)
	go func() { done <- downloader.downloadOnce(false) }()

	select {
	case <-entered:
		cancel()
		require.NoError(t, <-done)
	case err := <-done:
		require.Failf(t, "retry bypassed sync gate", "download exited before sync gate: %v", err)
	}
}

func TestBlobHistoryDownloaderKeepsRetryTargetAtDenebStart(t *testing.T) {
	const denebStart = uint64(100)
	downloader := newBoundaryDownloader(t, denebStart+10, 0, denebStart, &boundaryBlockReader{})

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, denebStart, downloader.nextBackfillTargetSlot)
}

func TestBlobHistoryDownloaderSecondCompletedPassScansRecentUnfrozenRange(t *testing.T) {
	const head = uint64(1_000)
	reader := &boundaryBlockReader{block: cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)}
	downloader := newBoundaryDownloader(t, head, 0, 0, reader)

	require.NoError(t, downloader.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, downloader.downloadOnce(false))

	recentFloor := head - clparams.MainnetBeaconConfig.SlotsPerEpoch*2
	require.Equal(t, recentFloor, downloader.nextBackfillTargetSlot)
	require.Equal(t, head-recentFloor+1, uint64(len(reader.slots)))
	require.Equal(t, recentFloor, reader.slots[len(reader.slots)-1])
}

func TestBlobHistoryDownloaderRetryReadFailureDoesNotBlockRecentScan(t *testing.T) {
	const (
		head      = uint64(1_000)
		retrySlot = uint64(1)
	)
	recentFloor := head - clparams.MainnetBeaconConfig.SlotsPerEpoch*2
	reader := &boundaryBlockReader{errors: map[uint64]error{retrySlot: errors.New("retry read failed")}}
	downloader := newBoundaryDownloader(t, head, 0, recentFloor, reader)
	downloader.addRetrySlot(retrySlot)

	for range 2 {
		reader.slots = nil
		require.NoError(t, downloader.downloadOnce(false))
		require.Equal(t, retrySlot, reader.slots[0])
		require.Equal(t, head, reader.slots[1])
		require.Equal(t, recentFloor, reader.slots[len(reader.slots)-1])
		require.Equal(t, []blobRetryRange{{start: retrySlot, end: retrySlot, cursor: retrySlot}}, downloader.retryRanges)
	}
}

func TestBlobHistoryDownloaderCancellationDuringRetryStopsBeforeRecentScan(t *testing.T) {
	const (
		head      = uint64(1_000)
		retrySlot = uint64(1)
	)
	ctx, cancel := context.WithCancel(t.Context())
	reader := &boundaryBlockReader{
		errors: map[uint64]error{retrySlot: context.Canceled},
		onRead: func(slot uint64) {
			if slot == retrySlot {
				cancel()
			}
		},
	}
	recentFloor := head - clparams.MainnetBeaconConfig.SlotsPerEpoch*2
	downloader := newBoundaryDownloader(t, head, 0, recentFloor, reader)
	downloader.ctx = ctx
	downloader.addRetrySlot(retrySlot)
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{retrySlot}, reader.slots)
	require.Equal(t, []blobRetryRange{{start: retrySlot, end: retrySlot, cursor: retrySlot}}, downloader.retryRanges)
	require.False(t, downloader.backfillCompleted.Load())
	require.False(t, notified)
}

func TestBlobHistoryDownloaderFailedRecoveryContinuesScanWithoutNotifying(t *testing.T) {
	const (
		head   = uint64(100)
		target = uint64(80)
	)
	ctrl := gomock.NewController(t)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	failedRecovery := errors.New("recovery failed")
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(failedRecovery)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = head
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{head: block}}
	downloader := newBoundaryDownloader(t, head, 0, target, reader)
	downloader.blobStorage = blobStorage
	downloader.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	notified := false
	downloader.SetNotifyBlobBackfilled(func(completed bool) { notified = completed })

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, target, reader.slots[len(reader.slots)-1])
	require.False(t, notified)
	require.False(t, downloader.backfillCompleted.Load())
	require.NotEmpty(t, downloader.retryRanges)
}

func TestBlobHistoryDownloaderNonArchiveSecondPassScansRecentRange(t *testing.T) {
	const head = uint64(1_000)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, head, 0, 0, reader)
	downloader.archiveBlobs = false
	downloader.immediateBlobsBackfilling = true

	require.NoError(t, downloader.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, downloader.downloadOnce(false))

	recentFloor := head - clparams.MainnetBeaconConfig.SlotsPerEpoch*2
	require.Equal(t, recentFloor, downloader.nextBackfillTargetSlot)
	require.Equal(t, head-recentFloor+1, uint64(len(reader.slots)))
	require.Equal(t, recentFloor, reader.slots[len(reader.slots)-1])
}

func TestBlobHistoryDownloaderRetriesSparseSlotsWithoutScanningGap(t *testing.T) {
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, 1_000, 0, 1_000, reader)
	downloader.addRetrySlot(1)
	downloader.addRetrySlot(20)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, []uint64{1, 20}, reader.slots[:2])
	require.Empty(t, downloader.retryRanges)
}

func TestBlobHistoryDownloaderRetryRangesRemainFairAcrossSparseFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{
		1:         cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		1_000_000: cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
	}}
	for slot, block := range reader.blocks {
		block.Block.Slot = slot
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	downloader := newBoundaryDownloader(t, 1_000_000, 0, 1_000_000, reader)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), errors.New("temporary read failure")).AnyTimes()
	downloader.blobStorage = blobStorage
	downloader.addRetrySlot(1)
	downloader.addRetrySlot(1_000_000)

	require.NoError(t, downloader.retryFailedRecoveries(0))
	require.Equal(t, []uint64{1, 1_000_000}, reader.slots)
}

func TestBlobHistoryDownloaderRetryRangeExtensionPreservesProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	blocks := make(map[uint64]*cltypes.SignedBeaconBlock, blocksBatchSize*2+1)
	for slot := uint64(1); slot <= blocksBatchSize*2+1; slot++ {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
		blocks[slot] = block
	}
	reader := &boundaryBlockReader{blocks: blocks}
	downloader := newBoundaryDownloader(t, blocksBatchSize*2+1, 0, blocksBatchSize*2+1, reader)
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), errors.New("temporary read failure")).AnyTimes()
	downloader.blobStorage = blobStorage
	downloader.retryRanges = []blobRetryRange{{start: 1, end: blocksBatchSize * 2, cursor: blocksBatchSize * 2}}

	require.NoError(t, downloader.retryFailedRecoveries(0))
	reader.slots = nil
	downloader.addRetrySlot(blocksBatchSize*2 + 1)
	require.NoError(t, downloader.retryFailedRecoveries(0))

	require.Contains(t, reader.slots, uint64(1))
}

func TestBlobHistoryDownloaderRetriesThirtyThreeSparseFailuresWithoutDenseFallback(t *testing.T) {
	const failureCount = 33
	ctrl := gomock.NewController(t)
	blocks := make(map[uint64]*cltypes.SignedBeaconBlock, failureCount)
	downloader := newBoundaryDownloader(t, 1, 0, 1, &boundaryBlockReader{blocks: blocks})
	reader := downloader.blockReader.(*boundaryBlockReader)
	for i := range failureCount {
		slot := uint64(i)*1_000_000 + 1
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
		blocks[slot] = block
		downloader.addRetrySlot(slot)
	}
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), errors.New("temporary read failure")).AnyTimes()
	downloader.blobStorage = blobStorage

	require.Len(t, downloader.retryRanges, 1)
	require.Equal(t, failureCount, downloader.retryRanges[0].intervalCount())
	for range (failureCount + int(blocksBatchSize) - 1) / int(blocksBatchSize) {
		require.NoError(t, downloader.retryFailedRecoveries(0))
	}
	for slot := range blocks {
		require.Contains(t, reader.slots, slot)
	}
}

func TestBlobHistoryDownloaderRetryRangeOverflowConservesEveryFailure(t *testing.T) {
	downloader := newBoundaryDownloader(t, 1, 0, 1, &boundaryBlockReader{})
	for shard := range maxBlobRetryRanges {
		downloader.addRetrySlot(uint64(shard) << blobRetryShardShift)
	}
	downloader.addRetrySlot(1)

	require.Len(t, downloader.retryRanges, maxBlobRetryRanges)
	require.Equal(t, uint64(0), downloader.retryRanges[0].start)
	require.Equal(t, uint64(1), downloader.retryRanges[0].end)
	require.Equal(t, uint64(2), downloader.retryRanges[0].workCount())
	for shard := range maxBlobRetryRanges {
		slot := uint64(shard) << blobRetryShardShift
		contained := false
		for _, retryRange := range downloader.retryRanges {
			contained = contained || retryRange.contains(slot)
		}
		require.Truef(t, contained, "retry slot %d was discarded", slot)
	}
	require.True(t, downloader.retryRanges[0].contains(1))
}

func TestBlobHistoryDownloaderRetryRangeOverflowVisitsOnlySparseFailures(t *testing.T) {
	const failureCount = maxBlobRetryRanges + 1
	ctrl := gomock.NewController(t)
	blocks := make(map[uint64]*cltypes.SignedBeaconBlock, failureCount)
	failures := make(map[uint64]struct{}, failureCount)
	reader := &boundaryBlockReader{blocks: blocks}
	downloader := newBoundaryDownloader(t, 1, 0, 1, reader)
	for i := range failureCount {
		slot := uint64(i)*1_000_000 + 1
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
		blocks[slot] = block
		failures[slot] = struct{}{}
		downloader.addRetrySlot(slot)
	}
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), errors.New("temporary read failure")).AnyTimes()
	downloader.blobStorage = blobStorage

	passes := (failureCount + int(blocksBatchSize) - 1) / int(blocksBatchSize)
	for range passes {
		require.NoError(t, downloader.retryFailedRecoveries(0))
	}
	seen := make(map[uint64]struct{}, len(reader.slots))
	for _, slot := range reader.slots {
		_, failed := failures[slot]
		require.Truef(t, failed, "retried synthetic gap slot %d", slot)
		seen[slot] = struct{}{}
	}
	require.Len(t, seen, failureCount)
}

func TestBlobHistoryDownloaderRetryVisitsMixedDenseAndSparseFailuresWithinOneCycle(t *testing.T) {
	const denseFailures = 32
	const sparseFailures = 32
	ctrl := gomock.NewController(t)
	blocks := make(map[uint64]*cltypes.SignedBeaconBlock, denseFailures+sparseFailures)
	reader := &boundaryBlockReader{blocks: blocks}
	downloader := newBoundaryDownloader(t, 1, 0, 1, reader)
	addFailure := func(slot uint64) {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
		blocks[slot] = block
		downloader.addRetrySlot(slot)
	}
	for slot := range uint64(denseFailures) {
		addFailure(slot)
	}
	for i := range sparseFailures {
		addFailure(uint64(i+1) * 1_000_000)
	}
	blobStorage := blobstoragemock.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), errors.New("temporary read failure")).AnyTimes()
	downloader.blobStorage = blobStorage

	for range (len(blocks) + int(blocksBatchSize) - 1) / int(blocksBatchSize) {
		require.NoError(t, downloader.retryFailedRecoveries(0))
	}
	seen := make(map[uint64]struct{}, len(reader.slots))
	for _, slot := range reader.slots {
		_, failed := blocks[slot]
		require.Truef(t, failed, "retried synthetic gap slot %d", slot)
		seen[slot] = struct{}{}
	}
	require.Len(t, seen, len(blocks))
}

func BenchmarkBlobHistoryDownloaderAddSparseRetrySlots(b *testing.B) {
	for _, failures := range []int{1024, 2048, 4096} {
		b.Run(fmt.Sprintf("failures_%d", failures), func(b *testing.B) {
			b.ReportMetric(float64(failures), "failures/op")
			for b.Loop() {
				downloader := &BlobHistoryDownloader{}
				for i := range failures {
					downloader.addRetrySlot(uint64(i)*1_000_000 + 1)
				}
			}
		})
	}
}

func TestBlobHistoryDownloaderRetryRangeCompressesContiguousSlots(t *testing.T) {
	downloader := &BlobHistoryDownloader{}
	for slot := range uint64(21) {
		downloader.addRetrySlot(slot)
	}

	require.Equal(t, []blobRetryRange{{start: 0, end: 20, cursor: 0}}, downloader.retryRanges)
}

func TestBlobHistoryDownloaderResolveRetrySlotClearsVacatedRange(t *testing.T) {
	newRange := func(slot uint64) blobRetryRange {
		intervals := newBlobRetryIntervalTree()
		intervals.ReplaceOrInsert(blobRetryInterval{start: slot, end: slot, cursor: slot})
		return blobRetryRange{start: slot, end: slot, cursor: slot, intervals: intervals, work: 1}
	}

	downloader := &BlobHistoryDownloader{retryRanges: make([]blobRetryRange, 3, 4)}
	downloader.retryRanges[0] = newRange(1)
	downloader.retryRanges[1] = newRange(2)
	downloader.retryRanges[2] = newRange(3)

	downloader.resolveRetrySlot(2)

	require.Equal(t, []uint64{1, 3}, []uint64{downloader.retryRanges[0].start, downloader.retryRanges[1].start})
	for _, released := range downloader.retryRanges[len(downloader.retryRanges):cap(downloader.retryRanges)] {
		require.Equal(t, blobRetryRange{}, released)
	}

	empty := &BlobHistoryDownloader{}
	empty.resolveRetrySlot(1)
	empty.trimRetryRanges(1)
	require.Nil(t, empty.retryRanges)
}

func TestBlobHistoryDownloaderTrimRetryRangesClearsVacatedRanges(t *testing.T) {
	newRange := func(slots ...uint64) blobRetryRange {
		intervals := newBlobRetryIntervalTree()
		for _, slot := range slots {
			intervals.ReplaceOrInsert(blobRetryInterval{start: slot, end: slot, cursor: slot})
		}
		return blobRetryRange{start: slots[0], end: slots[len(slots)-1], cursor: slots[0], intervals: intervals, work: uint64(len(slots))}
	}

	downloader := &BlobHistoryDownloader{retryRanges: make([]blobRetryRange, 3, 4)}
	downloader.retryRanges[0] = newRange(1)
	downloader.retryRanges[1] = newRange(2, 5)
	downloader.retryRanges[2] = newRange(8)

	downloader.trimRetryRanges(4)

	require.Equal(t, []uint64{5, 8}, []uint64{downloader.retryRanges[0].start, downloader.retryRanges[1].start})
	require.True(t, downloader.retryRanges[0].contains(5))
	require.False(t, downloader.retryRanges[0].contains(2))
	for _, released := range downloader.retryRanges[len(downloader.retryRanges):cap(downloader.retryRanges)] {
		require.Equal(t, blobRetryRange{}, released)
	}
}

func TestBlobHistoryDownloaderResolvedRetrySlotsAreRemovedAroundReadFailure(t *testing.T) {
	reader := &boundaryBlockReader{errors: map[uint64]error{2: errors.New("retry read failed")}}
	downloader := newBoundaryDownloader(t, 2, 0, 2, reader)
	downloader.retryRanges = []blobRetryRange{{start: 0, end: 2, cursor: 1}}

	require.NoError(t, downloader.retryFailedRecoveries(0))
	require.ElementsMatch(t, []uint64{0, 1, 2}, reader.slots)
	require.Equal(t, []blobRetryRange{{start: 2, end: 2, cursor: 2}}, downloader.retryRanges)
}

func TestBlobHistoryDownloaderInteriorResolveKeepsRetryRangeBound(t *testing.T) {
	downloader := newBoundaryDownloader(t, 10, 0, 10, &boundaryBlockReader{})
	for slot := range uint64(11) {
		downloader.addRetrySlot(slot)
	}
	for i := 1; i < maxBlobRetryRanges; i++ {
		slot := uint64(i+1) * 10
		downloader.addRetrySlot(slot)
	}

	downloader.resolveRetrySlot(5)

	require.LessOrEqual(t, len(downloader.retryRanges), maxBlobRetryRanges)
	for _, retryRange := range downloader.retryRanges {
		require.False(t, retryRange.contains(5))
	}
	for i := range maxBlobRetryRanges + 1 {
		slot := uint64(i) * 10
		contained := false
		for _, retryRange := range downloader.retryRanges {
			contained = contained || retryRange.contains(slot)
		}
		require.Truef(t, contained, "retry slot %d was discarded", slot)
	}
}

func TestBlobHistoryDownloaderDropsRetriesBeforeNonArchiveRetentionFloor(t *testing.T) {
	retention := clparams.MainnetBeaconConfig.MinSlotsForBlobsSidecarsRequest()
	headSlot := retention + 10
	expiredSlot := uint64(9)
	reader := &boundaryBlockReader{}
	downloader := newBoundaryDownloader(t, headSlot, 0, 0, reader)
	downloader.archiveBlobs = false
	downloader.immediateBlobsBackfilling = true
	downloader.addRetrySlot(expiredSlot)

	require.NoError(t, downloader.downloadOnce(false))
	require.NotContains(t, reader.slots, expiredSlot)
	require.Empty(t, downloader.retryRanges)
}

func newBoundaryDownloader(t *testing.T, headSlot, frozenBlobs, targetSlot uint64, reader freezeblocks.BeaconSnapshotReader) *BlobHistoryDownloader {
	t.Helper()
	downloader := &BlobHistoryDownloader{
		ctx:                    t.Context(),
		beaconCfg:              &clparams.MainnetBeaconConfig,
		rpc:                    boundaryPeerCounter(1),
		indiciesDB:             mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		blockReader:            reader,
		sn:                     boundarySnapshot(frozenBlobs),
		syncedChecker:          boundarySyncedChecker(true),
		nextBackfillTargetSlot: targetSlot,
		denebStartSlot:         targetSlot,
		archiveBlobs:           true,
		logger:                 log.New(),
	}
	downloader.headSlot.Store(headSlot)
	return downloader
}

type boundaryBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	slots  []uint64
	err    error
	errors map[uint64]error
	block  *cltypes.SignedBeaconBlock
	blocks map[uint64]*cltypes.SignedBeaconBlock
	onRead func(uint64)
}

func (r *boundaryBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	if r.onRead != nil {
		r.onRead(slot)
	}
	if err := r.errors[slot]; err != nil {
		return nil, err
	}
	if r.blocks != nil {
		return r.blocks[slot], r.err
	}
	return r.block, r.err
}

type boundaryPeerCounter uint64

func (p boundaryPeerCounter) Peers() (uint64, error) { return uint64(p), nil }

func (p boundaryPeerCounter) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", nil
}

type boundarySequencePeerCounter struct {
	counts    []uint64
	calls     int
	requests  int
	onRequest func()
}

func (p *boundarySequencePeerCounter) Peers() (uint64, error) {
	index := min(p.calls, len(p.counts)-1)
	p.calls++
	return p.counts[index], nil
}

func (p *boundarySequencePeerCounter) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	p.requests++
	if p.onRequest != nil {
		p.onRequest()
	}
	return nil, "", nil
}

type boundarySnapshot uint64

func (s boundarySnapshot) FrozenBlobs() uint64 { return uint64(s) }

type boundaryMutableSnapshot struct {
	frozen atomic.Uint64
}

func (s *boundaryMutableSnapshot) FrozenBlobs() uint64 { return s.frozen.Load() }

type boundarySyncedChecker bool

func (s boundarySyncedChecker) Synced() bool { return bool(s) }

type boundarySyncedCheckerFunc func() bool

func (f boundarySyncedCheckerFunc) Synced() bool { return f() }
