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
	"math"
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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type staticPeerDasGetter struct{ pd das.PeerDas }

func (s staticPeerDasGetter) GetPeerDas() das.PeerDas { return s.pd }

type forcedRecoveryPeerDas struct {
	das.PeerDas
	force func(context.Context, uint64, common.Hash) error
}

func (p forcedRecoveryPeerDas) ForceScheduleRecover(ctx context.Context, slot uint64, root common.Hash, expectedBlobs uint64) error {
	return p.force(ctx, slot, root)
}

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

	b := &BlobHistoryDownloader{
		ctx:                   context.Background(),
		peerDasGetter:         staticPeerDasGetter{pd: peerDas},
		columnBackfillTimeout: 50 * time.Millisecond,
		logger:                log.New(),
	}

	fulu := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	fulu.Block.Slot = 100

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

func TestBlobHistoryDownloaderReportsPendingSlots(t *testing.T) {
	t.Run("archive", func(t *testing.T) {
		denebSlot := clparams.MainnetBeaconConfig.DenebForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
		b := &BlobHistoryDownloader{archiveBlobs: true, beaconCfg: &clparams.MainnetBeaconConfig}
		b.headSlot.Store(denebSlot + 100)
		require.True(t, b.BlobBackfillPending(denebSlot))
		require.False(t, b.BlobBackfillPending(denebSlot-1))
		require.True(t, b.BlobBackfillPending(b.headSlot.Load()))
		require.True(t, b.BlobBackfillPending(b.headSlot.Load()+1))
		b.completedRanges = []backfillRange{{denebSlot, b.headSlot.Load()}}
		b.backfillCompleted.Store(true)
		require.False(t, b.BlobBackfillPending(denebSlot))
	})

	t.Run("immediate", func(t *testing.T) {
		beaconCfg := clparams.MainnetBeaconConfig
		beaconCfg.DenebForkEpoch = 0
		beaconCfg.FuluForkEpoch = math.MaxUint64
		window := beaconCfg.MinSlotsForBlobsSidecarsRequest()
		b := &BlobHistoryDownloader{immediateBlobsBackfilling: true, beaconCfg: &beaconCfg}
		b.headSlot.Store(window + 100)
		oldestEpoch := b.headSlot.Load()/b.beaconCfg.SlotsPerEpoch - b.beaconCfg.MinEpochsForBlobSidecarsRequests
		oldestSlot := oldestEpoch * b.beaconCfg.SlotsPerEpoch
		require.True(t, b.BlobBackfillPending(b.headSlot.Load()-1))
		require.True(t, b.BlobBackfillPending(oldestSlot))
		require.False(t, b.BlobBackfillPending(oldestSlot-1))
		require.True(t, b.BlobBackfillPending(b.headSlot.Load()+1))
	})

	t.Run("head not initialized", func(t *testing.T) {
		denebSlot := clparams.MainnetBeaconConfig.DenebForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
		for _, b := range []*BlobHistoryDownloader{
			{archiveBlobs: true, beaconCfg: &clparams.MainnetBeaconConfig},
			{immediateBlobsBackfilling: true, beaconCfg: &clparams.MainnetBeaconConfig},
		} {
			require.True(t, b.BlobBackfillPending(denebSlot))
			require.False(t, b.BlobBackfillPending(denebSlot-1))
		}
	})

	t.Run("disabled", func(t *testing.T) {
		b := &BlobHistoryDownloader{}
		require.False(t, b.BlobBackfillPending(1))
	})
}

func TestBlobHistoryDownloaderHandlesUnscheduledDeneb(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = math.MaxUint64
	d := NewBlobHistoryDownloader(t.Context(), &beaconCfg, nil, nil, nil, nil, nil, nil, nil, true, false, log.Root())

	require.Equal(t, uint64(math.MaxUint64), d.targetSlot)
	require.False(t, d.BlobBackfillPending(math.MaxUint64))
}

func TestBlobHistoryDownloaderPreservesArchiveTargetAfterFailure(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(1)
	)
	wantErr := errors.New("read failed")
	d := newBlobDownloaderForBoundaryTest(t, headSlot, 0, targetSlot, 16, &recordingBlobBlockReader{err: wantErr})

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	if d.targetSlot != targetSlot {
		t.Fatalf("target slot %d, want %d", d.targetSlot, targetSlot)
	}
}

func TestBlobHistoryDownloaderPreservesArchiveTargetAfterDenebFailure(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(1)
	)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = headSlot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, targetSlot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: block},
	})
	d.blobStorage = emptyBlobStorage{}
	wantErr := errors.New("Deneb request failed")
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return nil, wantErr
	}

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	if d.targetSlot != targetSlot {
		t.Fatalf("target slot %d, want %d", d.targetSlot, targetSlot)
	}
	if d.backfillCompleted.Load() {
		t.Fatal("backfill marked complete after Deneb failure")
	}
}

func TestBlobHistoryDownloaderContinuesPastFailedDenebBatch(t *testing.T) {
	const (
		headSlot   = uint64(100)
		olderSlot  = headSlot - blocksBatchSize
		targetSlot = uint64(1)
	)
	failedBlock, _ := makeBlobBoundaryObjects(t, headSlot, 1)
	olderBlock, olderSidecars := makeBlobBoundaryObjects(t, olderSlot, 1)
	failedRoot, err := failedBlock.Block.HashSSZ()
	require.NoError(t, err)
	olderRoot, err := olderBlock.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, olderSlot, targetSlot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{
			headSlot:  failedBlock,
			olderSlot: olderBlock,
		},
	})
	d.blobStorage = storage
	wantErr := errors.New("newest batch unavailable")
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		if req.Get(0).BlockRoot == failedRoot {
			return nil, wantErr
		}
		return &PeerAndSidecars{Peer: "peer", Responses: olderSidecars}, nil
	}

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	stored, complete, err := storage.ReadBlobSidecars(t.Context(), olderSlot, olderRoot)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, stored, 1)
	require.False(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderFuluFailureDoesNotCompleteBackfill(t *testing.T) {
	const slot = uint64(100)
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	wantErr := errors.New("columns unavailable")
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(wantErr)
	fulu, _ := makeBlobBoundaryObjectsVersion(t, slot, 1, clparams.FuluVersion)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, 1, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: fulu},
	})
	d.blobStorage = newBlobBoundaryStorage(t)
	d.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	d.columnBackfillTimeout = time.Second

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	require.False(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderFuluSuccessRequiresStoredBlobs(t *testing.T) {
	const slot = uint64(100)
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	fulu, _ := makeBlobBoundaryObjectsVersion(t, slot, 1, clparams.FuluVersion)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, 1, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: fulu},
	})
	d.blobStorage = newBlobBoundaryStorage(t)
	d.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	d.columnBackfillTimeout = time.Second

	require.ErrorContains(t, d.downloadOnce(false), "incomplete Fulu blob recovery")
	require.False(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderForcesFuluRecoveryWhenIndexExistsButBlobIsMissing(t *testing.T) {
	const slot = uint64(100)
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).Return(nil)
	fulu, sidecars := makeBlobBoundaryObjectsVersion(t, slot, 1, clparams.FuluVersion)
	root, err := fulu.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	forced := false
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: fulu},
	})
	d.blobStorage = storage
	d.peerDasGetter = staticPeerDasGetter{pd: forcedRecoveryPeerDas{
		PeerDas: peerDas,
		force: func(ctx context.Context, gotSlot uint64, gotRoot common.Hash) error {
			forced = true
			require.Equal(t, slot, gotSlot)
			require.Equal(t, common.Hash(root), gotRoot)
			return storage.WriteBlobSidecars(ctx, root, sidecars)
		},
	}}
	d.columnBackfillTimeout = time.Second

	require.NoError(t, d.downloadOnce(false))
	require.True(t, forced)
	require.True(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderRejectsInvalidStoredBlobSet(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*cltypes.BlobSidecar)
	}{
		{
			name: "wrong block root",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.SignedBlockHeader.Header.ParentRoot[0]++
			},
		},
		{
			name: "wrong commitment",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.KzgCommitment[0]++
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, sidecars := makeBlobBoundaryObjects(t, 100, 1)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			tc.mutate(sidecars[0])
			d := &BlobHistoryDownloader{ctx: t.Context(), blobStorage: completeBlobStorage{sidecars: sidecars}}

			complete, err := d.actualBlobSetComplete(block, root)
			require.NoError(t, err)
			require.False(t, complete)
		})
	}
}

func TestBlobHistoryDownloaderRunsFuluRecoveryAfterDenebFailure(t *testing.T) {
	const headSlot = uint64(100)
	ctrl := gomock.NewController(t)
	peerDas := mock_services.NewMockPeerDas(ctrl)
	deneb, _ := makeBlobBoundaryObjects(t, headSlot, 1)
	fulu, fuluSidecars := makeBlobBoundaryObjectsVersion(t, headSlot-1, 1, clparams.FuluVersion)
	fuluRoot, err := fulu.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot-1, 1, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: deneb, headSlot - 1: fulu},
	})
	d.blobStorage = storage
	d.peerDasGetter = staticPeerDasGetter{pd: peerDas}
	d.columnBackfillTimeout = time.Second
	wantErr := errors.New("deneb unavailable")
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return nil, wantErr
	}
	peerDas.EXPECT().DownloadColumnsAndRecoverBlobs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, []cltypes.ColumnSyncableSignedBlock) error {
			return storage.WriteBlobSidecars(t.Context(), fuluRoot, fuluSidecars)
		},
	)

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	_, complete, err := storage.ReadBlobSidecars(t.Context(), headSlot-1, fuluRoot)
	require.NoError(t, err)
	require.True(t, complete)
}

func TestBlobHistoryDownloaderHeadAdvanceOnlyMarksNewRangePending(t *testing.T) {
	const headSlot = uint64(100)
	reader := &mappedBlobBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{}}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, 1, 16, reader)
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	d.beaconCfg = &beaconCfg
	d.blobStorage = newBlobBoundaryStorage(t)
	require.NoError(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(headSlot))

	newBlock, _ := makeBlobBoundaryObjects(t, headSlot+1, 1)
	reader.blocks[headSlot+1] = newBlock
	d.SetHead(headSlot+1, common.Hash{}, headSlot)
	wantErr := errors.New("new head unavailable")
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return nil, wantErr
	}

	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	require.False(t, d.BlobBackfillPending(headSlot))
	require.True(t, d.BlobBackfillPending(headSlot+1))
}

func TestBlobHistoryDownloaderDoesNotMarkSkippedFuluGapComplete(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 1
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = 100
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 10
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, 200, 0, 0, 16, reader)
	d.beaconCfg = &beaconCfg
	d.blobStorage = newBlobBoundaryStorage(t)

	require.NoError(t, d.downloadOnce(false))
	reader.slots = nil
	d.SetHead(105, common.Hash{}, 105)
	require.True(t, d.BlobBackfillPending(100))
	require.NoError(t, d.downloadOnce(false))
	require.Contains(t, reader.slots, uint64(100))
	require.Contains(t, reader.slots, uint64(105))
}

func TestBlobHistoryDownloaderDoesNotRescanCompletedHead(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(90)
	)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, targetSlot, targetSlot, 16, reader)

	require.NoError(t, d.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, d.downloadOnce(false))
	require.Empty(t, reader.slots)
}

func TestBlobHistoryDownloaderHeadAdvanceScansOnlyUncoveredSuffix(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(90)
	)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, targetSlot, targetSlot, 16, reader)
	require.NoError(t, d.downloadOnce(false))

	reader.slots = nil
	d.SetHead(headSlot+2, common.Hash{}, headSlot)
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, []uint64{headSlot + 2, headSlot + 1}, reader.slots)
}

func TestBlobHistoryDownloaderHeadRegressionExpandsEpochAlignedLowerEdge(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = math.MaxUint64
	beaconCfg.SlotsPerEpoch = 8
	beaconCfg.MinEpochsForBlobSidecarsRequests = 2
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, 100, 0, 0, 16, reader)
	d.archiveBlobs = false
	d.immediateBlobsBackfilling = true
	d.beaconCfg = &beaconCfg
	require.NoError(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(80))

	reader.slots = nil
	d.SetHead(79, common.Hash{}, 79)
	require.True(t, d.BlobBackfillPending(56))
	require.False(t, d.BlobBackfillPending(55))
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, uint64(79), reader.slots[0])
	require.Equal(t, uint64(56), reader.slots[len(reader.slots)-1])

	reader.slots = nil
	d.SetHead(100, common.Hash{}, 79)
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, uint64(100), reader.slots[0])
	require.Equal(t, uint64(80), reader.slots[len(reader.slots)-1])
}

func TestBlobHistoryDownloaderUsesForkSpecificRetentionWindows(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = 10
	beaconCfg.SlotsPerEpoch = 8
	beaconCfg.MinEpochsForBlobSidecarsRequests = 4
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 2
	b := &BlobHistoryDownloader{immediateBlobsBackfilling: true, beaconCfg: &beaconCfg}
	b.headSlot.Store(104)

	require.True(t, b.BlobBackfillPending(72))
	require.True(t, b.BlobBackfillPending(79))
	require.False(t, b.BlobBackfillPending(80))
	require.False(t, b.BlobBackfillPending(87))
	require.True(t, b.BlobBackfillPending(88))
	require.True(t, b.BlobBackfillPending(104))
}

func TestBlobHistoryDownloaderSameSlotReorgInvalidatesCompletion(t *testing.T) {
	const headSlot = uint64(100)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, headSlot, 16, reader)
	d.SetHead(headSlot, common.HexToHash("0x01"), headSlot)
	require.NoError(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(headSlot))

	reader.slots = nil
	d.SetHead(headSlot, common.HexToHash("0x02"), headSlot-1)
	require.True(t, d.BlobBackfillPending(headSlot))
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, []uint64{headSlot}, reader.slots)
}

func TestBlobHistoryDownloaderInvalidateCompletionAboveMakesReorgRangePending(t *testing.T) {
	d := newBlobDownloaderForBoundaryTest(t, 100, 100, 64, 16, &recordingBlobBlockReader{})
	d.SetHead(100, common.HexToHash("0x01"), 100)
	d.mu.Lock()
	d.completedRanges = []backfillRange{{start: 64, end: 100}}
	d.backfillCompleted.Store(true)
	d.mu.Unlock()
	require.False(t, d.BlobBackfillPending(100))

	d.InvalidateCompletionAbove(99)

	require.True(t, d.BlobBackfillPending(100))
	require.Equal(t, uint64(100), d.HeadSlot())
}

func TestBlobHistoryDownloaderHigherHeadReorgRescansChangedSuffix(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(90)
		forkSlot   = uint64(98)
	)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, targetSlot, targetSlot, 16, reader)
	d.SetHead(headSlot, common.HexToHash("0x01"), headSlot)
	require.NoError(t, d.downloadOnce(false))

	reader.slots = nil
	d.SetHead(headSlot+2, common.HexToHash("0x02"), forkSlot)
	require.True(t, d.BlobBackfillPending(forkSlot+1))
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, []uint64{102, 101, 100, 99}, reader.slots)
}

func TestBlobHistoryDownloaderHeadJumpStartsAtCurrentRetentionFloor(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = math.MaxUint64
	beaconCfg.SlotsPerEpoch = 8
	beaconCfg.MinEpochsForBlobSidecarsRequests = 2
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, 100, 0, 0, 16, reader)
	d.archiveBlobs = false
	d.immediateBlobsBackfilling = true
	d.beaconCfg = &beaconCfg
	require.NoError(t, d.downloadOnce(false))

	reader.slots = nil
	d.SetHead(1000, common.Hash{}, 100)
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, uint64(1000), reader.slots[0])
	require.Equal(t, uint64(984), reader.slots[len(reader.slots)-1])
	require.Len(t, reader.slots, 17)
}

func TestBlobHistoryDownloaderAcceptsReorderedDenebResponse(t *testing.T) {
	const headSlot = uint64(100)
	first, firstSidecars := makeBlobBoundaryObjects(t, headSlot, 1)
	second, secondSidecars := makeBlobBoundaryObjects(t, headSlot-1, 1)
	storage := newBlobBoundaryStorage(t)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot-1, 1, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: first, headSlot - 1: second},
	})
	d.blobStorage = storage
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return &PeerAndSidecars{Peer: "peer", Responses: []*cltypes.BlobSidecar{secondSidecars[0], firstSidecars[0]}}, nil
	}

	require.NoError(t, d.downloadOnce(false))
	for slot, block := range map[uint64]*cltypes.SignedBeaconBlock{headSlot: first, headSlot - 1: second} {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		_, complete, err := storage.ReadBlobSidecars(t.Context(), slot, root)
		require.NoError(t, err)
		require.True(t, complete)
	}
}

func TestBlobHistoryDownloaderPersistsCompleteBlocksFromShortResponse(t *testing.T) {
	const headSlot = uint64(100)
	first, firstSidecars := makeBlobBoundaryObjects(t, headSlot, 1)
	second, _ := makeBlobBoundaryObjects(t, headSlot-1, 1)
	firstRoot, err := first.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot-1, 1, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: first, headSlot - 1: second},
	})
	d.blobStorage = storage
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return &PeerAndSidecars{Peer: "peer", Responses: firstSidecars}, nil
	}

	require.Error(t, d.downloadOnce(false))
	_, complete, err := storage.ReadBlobSidecars(t.Context(), headSlot, firstRoot)
	require.NoError(t, err)
	require.True(t, complete)
	require.False(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderDoesNotTrustIndexedCountWhenSidecarIsMissing(t *testing.T) {
	const slot = uint64(100)
	block, _ := makeBlobBoundaryObjects(t, slot, 1)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 1, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block},
	})
	d.blobStorage = indexedButMissingBlobStorage{}
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return nil, errors.New("recovery required")
	}

	require.ErrorContains(t, d.downloadOnce(false), "recovery required")
	require.False(t, d.backfillCompleted.Load())
	require.True(t, d.BlobBackfillPending(slot))
}

func TestBlobHistoryDownloaderAccumulatesPartialBlockAcrossRequests(t *testing.T) {
	const headSlot = uint64(100)
	block, sidecars := makeBlobBoundaryObjects(t, headSlot, 2)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, headSlot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: block},
	})
	d.blobStorage = storage
	requests := 0
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		requests++
		require.Equal(t, 3-requests, req.Len())
		return &PeerAndSidecars{Peer: "peer", Responses: []*cltypes.BlobSidecar{sidecars[requests-1]}}, nil
	}

	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, 2, requests)
	stored, complete, err := storage.ReadBlobSidecars(t.Context(), headSlot, blockRoot)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, stored, 2)
}

func TestBlobHistoryDownloaderRejectsRepeatedPartialResponse(t *testing.T) {
	const headSlot = uint64(100)
	block, sidecars := makeBlobBoundaryObjects(t, headSlot, 2)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, headSlot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: block},
	})
	d.blobStorage = newBlobBoundaryStorage(t)
	requests := 0
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		requests++
		return &PeerAndSidecars{Peer: "peer", Responses: sidecars[:1]}, nil
	}

	require.ErrorContains(t, d.downloadOnce(false), "unrequested blob sidecar")
	require.Equal(t, 2, requests)
}

func TestBlobHistoryDownloaderRejectsInvalidOrIncompleteDenebResponse(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(1)
	)
	testCases := []struct {
		name          string
		commitments   int
		alterResponse func([]*cltypes.BlobSidecar) *PeerAndSidecars
	}{
		{
			name:        "nil result",
			commitments: 1,
			alterResponse: func([]*cltypes.BlobSidecar) *PeerAndSidecars {
				return nil
			},
		},
		{
			name:        "short response",
			commitments: 2,
			alterResponse: func(sidecars []*cltypes.BlobSidecar) *PeerAndSidecars {
				return &PeerAndSidecars{Peer: "peer", Responses: sidecars[:1]}
			},
		},
		{
			name:        "extra response",
			commitments: 1,
			alterResponse: func(sidecars []*cltypes.BlobSidecar) *PeerAndSidecars {
				return &PeerAndSidecars{Peer: "peer", Responses: append(sidecars, sidecars[0])}
			},
		},
		{
			name:        "duplicate index",
			commitments: 2,
			alterResponse: func(sidecars []*cltypes.BlobSidecar) *PeerAndSidecars {
				sidecars[1] = sidecars[0]
				return &PeerAndSidecars{Peer: "peer", Responses: sidecars}
			},
		},
		{
			name:        "wrong root",
			commitments: 1,
			alterResponse: func(sidecars []*cltypes.BlobSidecar) *PeerAndSidecars {
				sidecars[0].SignedBlockHeader.Header.ParentRoot[0]++
				return &PeerAndSidecars{Peer: "peer", Responses: sidecars}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, sidecars := makeBlobBoundaryObjects(t, headSlot, tc.commitments)
			d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, targetSlot, 16, &mappedBlobBlockReader{
				blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: block},
			})
			d.blobStorage = blob_storage.NewBlobStore(
				memdb.NewTestDB(t, dbcfg.ChainDB),
				afero.NewMemMapFs(),
				math.MaxUint64,
				&clparams.MainnetBeaconConfig,
				nil,
			)
			d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
				return tc.alterResponse(sidecars), nil
			}

			require.Error(t, d.downloadOnce(false))
			require.Equal(t, targetSlot, d.targetSlot)
			require.False(t, d.backfillCompleted.Load())
		})
	}
}

func TestBlobHistoryDownloaderPersistsBoundaryBlobAcrossRestart(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(1)
	)
	block, sidecars := makeBlobBoundaryObjects(t, headSlot, 1)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	storage := blob_storage.NewBlobStore(
		memdb.NewTestDB(t, dbcfg.ChainDB),
		afero.NewMemMapFs(),
		math.MaxUint64,
		&clparams.MainnetBeaconConfig,
		nil,
	)
	newDownloader := func() *BlobHistoryDownloader {
		d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot, targetSlot, 16, &mappedBlobBlockReader{
			blocks: map[uint64]*cltypes.SignedBeaconBlock{headSlot: block},
		})
		d.blobStorage = storage
		return d
	}
	d := newDownloader()
	requests := 0
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		requests++
		return &PeerAndSidecars{Peer: "peer", Responses: sidecars}, nil
	}

	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, 1, requests)
	stored, complete, err := storage.ReadBlobSidecars(t.Context(), headSlot, blockRoot)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, stored, 1)

	restarted := newDownloader()
	restarted.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
		return nil, errors.New("persisted boundary blob requested after restart")
	}
	require.NoError(t, restarted.downloadOnce(false))
}

func TestBlobHistoryDownloaderPreservesArchiveTargetAfterSuccess(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(1)
	)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, 90, targetSlot, 16, reader)

	if err := d.downloadOnce(false); err != nil {
		t.Fatal(err)
	}
	if len(reader.slots) <= int(blocksBatchSize) {
		t.Fatalf("read only %d slots, want more than one batch", len(reader.slots))
	}
	if d.targetSlot != targetSlot {
		t.Fatalf("target slot %d, want %d", d.targetSlot, targetSlot)
	}

	d.headSlot.Store(headSlot + 10)
	if err := d.downloadOnce(false); err != nil {
		t.Fatal(err)
	}
	if d.targetSlot != targetSlot {
		t.Fatalf("target slot after head advance %d, want %d", d.targetSlot, targetSlot)
	}
}

func TestBlobHistoryDownloaderDoesNotPersistNonArchiveTarget(t *testing.T) {
	const targetSlot = uint64(1)
	d := newBlobDownloaderForBoundaryTest(t, 100, 100, targetSlot, 16, &recordingBlobBlockReader{})
	d.archiveBlobs = false

	if err := d.downloadOnce(false); err != nil {
		t.Fatal(err)
	}
	if d.targetSlot != targetSlot {
		t.Fatalf("target slot %d, want %d", d.targetSlot, targetSlot)
	}
}

func makeBlobBoundaryObjects(t *testing.T, slot uint64, count int) (*cltypes.SignedBeaconBlock, []*cltypes.BlobSidecar) {
	return makeBlobBoundaryObjectsVersion(t, slot, count, clparams.DenebVersion)
}

func makeBlobBoundaryObjectsVersion(t *testing.T, slot uint64, count int, version clparams.StateVersion) (*cltypes.SignedBeaconBlock, []*cltypes.BlobSidecar) {
	t.Helper()
	blob := cltypes.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blob), commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	blockCommitment := cltypes.KZGCommitment(commitment)
	for range count {
		block.Block.Body.BlobKzgCommitments.Append(&blockCommitment)
	}
	sidecars := make([]*cltypes.BlobSidecar, count)
	for index := range count {
		proofBranch, err := block.Block.Body.KzgCommitmentMerkleProof(index)
		require.NoError(t, err)
		inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for i, hash := range proofBranch {
			inclusionProof.Set(i, hash)
		}
		sidecars[index] = cltypes.NewBlobSidecar(
			uint64(index),
			&blob,
			common.Bytes48(commitment),
			common.Bytes48(proof),
			block.SignedBeaconBlockHeader(),
			inclusionProof,
		)
	}
	return block, sidecars
}

func newBlobBoundaryStorage(t *testing.T) blob_storage.BlobStorage {
	t.Helper()
	return blob_storage.NewBlobStore(
		memdb.NewTestDB(t, dbcfg.ChainDB),
		afero.NewMemMapFs(),
		math.MaxUint64,
		&clparams.MainnetBeaconConfig,
		nil,
	)
}

func newBlobDownloaderForBoundaryTest(t *testing.T, headSlot, frozenBlobs, targetSlot, peers uint64, reader freezeblocks.BeaconSnapshotReader) *BlobHistoryDownloader {
	t.Helper()
	ctx := t.Context()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.FuluForkEpoch = math.MaxUint64
	d := &BlobHistoryDownloader{
		ctx:           ctx,
		beaconCfg:     &beaconCfg,
		rpc:           peerCountClient{active: peers},
		indiciesDB:    memdb.NewTestDB(t, dbcfg.ChainDB),
		blockReader:   reader,
		sn:            frozenBlobSnapshot{exclusiveEnd: frozenBlobs},
		syncedChecker: syncedChecker(true),
		targetSlot:    targetSlot,
		archiveBlobs:  true,
		logger:        log.Root(),
	}
	d.headSlot.Store(headSlot)
	return d
}

type recordingBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	slots []uint64
	err   error
}

type mappedBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
}

func (r *mappedBlobBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return r.blocks[slot], nil
}

type emptyBlobStorage struct{ blob_storage.BlobStorage }

func (emptyBlobStorage) KzgCommitmentsCount(context.Context, common.Hash) (uint32, error) {
	return 0, nil
}

func (emptyBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	return nil, false, nil
}

type indexedButMissingBlobStorage struct{ emptyBlobStorage }

func (indexedButMissingBlobStorage) KzgCommitmentsCount(context.Context, common.Hash) (uint32, error) {
	return 1, nil
}

type completeBlobStorage struct {
	emptyBlobStorage
	sidecars []*cltypes.BlobSidecar
}

func (s completeBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	return s.sidecars, true, nil
}

func (r *recordingBlobBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	return nil, r.err
}

type frozenBlobSnapshot struct{ exclusiveEnd uint64 }

func (s frozenBlobSnapshot) FrozenBlobs() uint64 { return s.exclusiveEnd }

type syncedChecker bool

func (s syncedChecker) Synced() bool { return bool(s) }

type peerCountClient struct{ active uint64 }

func (p peerCountClient) Peers() (uint64, error) { return p.active, nil }

func (peerCountClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", errors.New("unexpected blob request")
}
