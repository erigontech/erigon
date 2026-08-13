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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func waitBlobDownloaderSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func receiveBlobDownloaderError(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func receiveBlobDownloaderRoot(t *testing.T, roots <-chan common.Hash) common.Hash {
	t.Helper()
	select {
	case root := <-roots:
		return root
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requested root")
		return common.Hash{}
	}
}

type staticPeerDasGetter struct{ pd das.PeerDas }

func (s staticPeerDasGetter) GetPeerDas() das.PeerDas { return s.pd }

type signatureCandidateBlobPeerClient struct {
	responses [][]*cltypes.BlobSidecar
	peers     []string
	calls     atomic.Int64
	mu        sync.Mutex
	banned    []string
}

func (*signatureCandidateBlobPeerClient) Peers() (uint64, error) { return 2, nil }

func (c *signatureCandidateBlobPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	index := int(c.calls.Add(1)) - 1
	return c.responses[index], c.peers[index], nil
}

func (c *signatureCandidateBlobPeerClient) BanPeer(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.banned = append(c.banned, peer)
}

func (c *signatureCandidateBlobPeerClient) bannedPeers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.banned...)
}

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

func TestBlobHistoryDownloaderRejectsSignatureMismatchedCandidateAndPersistsHonestResponse(t *testing.T) {
	const slot = uint64(100)
	block, honest := makeBlobBoundaryObjects(t, slot, 1)
	maliciousSidecar := *honest[0]
	maliciousHeader := *maliciousSidecar.SignedBlockHeader
	maliciousHeader.Signature[0]++
	maliciousSidecar.SignedBlockHeader = &maliciousHeader
	client := &signatureCandidateBlobPeerClient{
		responses: [][]*cltypes.BlobSidecar{{&maliciousSidecar}, honest},
		peers:     []string{"malicious-peer", "honest-peer"},
	}
	storage := newBlobBoundaryStorage(t)
	d := &BlobHistoryDownloader{
		ctx:         t.Context(),
		beaconCfg:   &clparams.MainnetBeaconConfig,
		rpc:         client,
		blobStorage: storage,
		requestBlobs: func(ctx context.Context, client BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], validate func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
			return requestBlobsFranticallyValidated(ctx, req, client.SendBlobsSidecarByIdentifierReq, blobPeerRejecterFor(client), validate)
		},
	}

	require.NoError(t, d.recoverDenebBlobs([]*cltypes.SignedBeaconBlock{block}))
	require.Equal(t, []string{"malicious-peer"}, client.bannedPeers())
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	stored, complete, err := storage.ReadBlobSidecars(t.Context(), slot, root)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, stored, 1)
	require.Equal(t, block.Signature, stored[0].SignedBlockHeader.Signature)
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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

func TestBlobHistoryDownloaderRejectsNilCanonicalCommitments(t *testing.T) {
	const slot = uint64(100)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	block.Block.Body.BlobKzgCommitments = nil
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block},
	})

	require.ErrorContains(t, d.downloadOnce(false), "nil kzg commitments")
	require.True(t, d.BlobBackfillPending(slot))
	require.False(t, d.backfillCompleted.Load())
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
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], _ func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	require.False(t, d.BlobBackfillPending(olderSlot))
	require.True(t, d.BlobBackfillPending(headSlot))
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
		{
			name: "wrong blob proof",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.KzgProof[0]++
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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

func TestBlobHistoryDownloaderAuditsBoundedCompletedHead(t *testing.T) {
	const (
		headSlot   = uint64(100)
		targetSlot = uint64(90)
	)
	reader := &recordingBlobBlockReader{}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, targetSlot, targetSlot, 16, reader)

	require.NoError(t, d.downloadOnce(false))
	reader.slots = nil
	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, []uint64{100, 99, 98, 97, 96, 95, 94, 93}, reader.slots)
}

func TestBlobHistoryDownloaderHeadAdvanceAuditsAndScansUncoveredSuffix(t *testing.T) {
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
	require.Equal(t, []uint64{100, 99, 98, 97, 96, 95, 94, 93, headSlot + 2, headSlot + 1}, reader.slots)
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

func TestBlobHistoryDownloaderTransitionCeilingBlocksOldCanonicalPass(t *testing.T) {
	const slot = uint64(100)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &recordingBlobBlockReader{})
	d.SetHead(slot, common.HexToHash("0x01"), slot)
	d.mu.Lock()
	d.completedRanges = []backfillRange{{start: slot, end: slot}}
	d.backfillCompleted.Store(true)
	d.mu.Unlock()

	d.InvalidateCompletionAbove(slot - 1)
	require.NoError(t, d.downloadOnce(false))
	require.True(t, d.BlobBackfillPending(slot))

	d.SetHead(slot, common.HexToHash("0x02"), slot-1)
	require.True(t, d.BlobBackfillPending(slot))
}

func TestBlobHistoryDownloaderAbortHeadUpdateAllowsOldCanonicalRetry(t *testing.T) {
	const slot = uint64(100)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &recordingBlobBlockReader{})
	d.SetHead(slot, common.HexToHash("0x01"), slot)
	d.mu.Lock()
	d.completedRanges = []backfillRange{{start: slot, end: slot}}
	d.backfillCompleted.Store(true)
	d.mu.Unlock()

	d.InvalidateCompletionAbove(slot - 1)
	d.AbortHeadUpdate()
	require.True(t, d.BlobBackfillPending(slot))
	require.NoError(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(slot))
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
	require.Equal(t, []uint64{98, 97, 96, 95, 94, 93, 92, 91, 102, 101, 100, 99}, reader.slots)
}

func TestBlobHistoryDownloaderStalePassCannotRestoreReorgedSuffix(t *testing.T) {
	const slot = uint64(100)
	oldBlock, oldSidecars := makeBlobBoundaryObjects(t, slot, 1)
	newBlock, newSidecars := makeBlobBoundaryObjects(t, slot, 2)
	oldRoot, err := oldBlock.Block.HashSSZ()
	require.NoError(t, err)
	newRoot, err := newBlock.Block.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, oldRoot, newRoot)

	reader := &mappedBlobBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: oldBlock}}
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 1, reader)
	d.blobStorage = newBlobBoundaryStorage(t)
	d.SetHead(slot, oldRoot, slot-1)
	collected := make(chan struct{})
	resume := make(chan struct{})
	requestedRoots := make(chan common.Hash, 2)
	var requests atomic.Int64
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], _ func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		requestedRoots <- req.Get(0).BlockRoot
		if requests.Add(1) == 1 {
			close(collected)
			<-resume
			return &PeerAndSidecars{Peer: "peer", Responses: oldSidecars}, nil
		}
		return &PeerAndSidecars{Peer: "peer", Responses: newSidecars}, nil
	}

	firstPass := make(chan error, 1)
	go func() { firstPass <- d.downloadOnce(false) }()
	waitBlobDownloaderSignal(t, collected, "first stale pass collection")
	d.InvalidateCompletionAbove(slot - 1)
	reader.blocks[slot] = newBlock
	d.SetHead(slot, newRoot, slot-1)
	close(resume)
	require.NoError(t, receiveBlobDownloaderError(t, firstPass, "first stale pass"))
	require.True(t, d.BlobBackfillPending(slot))

	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, common.Hash(oldRoot), receiveBlobDownloaderRoot(t, requestedRoots))
	require.Equal(t, common.Hash(newRoot), receiveBlobDownloaderRoot(t, requestedRoots))
	require.False(t, d.BlobBackfillPending(slot))
}

func TestBlobHistoryDownloaderStalePassPreservesSafeHeadExtensionPrefix(t *testing.T) {
	const slot = uint64(100)
	block, sidecars := makeBlobBoundaryObjects(t, slot, 1)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	reader := &mappedBlobBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 1, reader)
	d.blobStorage = newBlobBoundaryStorage(t)
	d.SetHead(slot, root, slot-1)
	collected := make(chan struct{})
	resume := make(chan struct{})
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		close(collected)
		<-resume
		return &PeerAndSidecars{Peer: "peer", Responses: sidecars}, nil
	}

	firstPass := make(chan error, 1)
	go func() { firstPass <- d.downloadOnce(false) }()
	waitBlobDownloaderSignal(t, collected, "head extension pass collection")
	d.SetHead(slot+1, common.HexToHash("0x101"), slot)
	close(resume)
	require.NoError(t, receiveBlobDownloaderError(t, firstPass, "head extension pass"))

	require.False(t, d.BlobBackfillPending(slot))
	require.True(t, d.BlobBackfillPending(slot+1))
}

func TestBlobHistoryDownloaderAuditCannotRegisterStaleReorgSuffix(t *testing.T) {
	const (
		oldHead        = uint64(100)
		commonAncestor = uint64(95)
		targetSlot     = uint64(90)
	)
	reader := &barrierBlobBlockReader{entered: make(chan struct{}), resume: make(chan struct{})}
	d := newBlobDownloaderForBoundaryTest(t, oldHead, targetSlot, targetSlot, 16, reader)
	d.mu.Lock()
	d.completedRanges = []backfillRange{{start: targetSlot, end: oldHead}}
	d.backfillCompleted.Store(true)
	d.mu.Unlock()

	passDone := make(chan error, 1)
	go func() { passDone <- d.downloadOnce(false) }()
	waitBlobDownloaderSignal(t, reader.entered, "completion audit")
	d.InvalidateCompletionAbove(commonAncestor)
	d.SetHead(commonAncestor, common.HexToHash("0x02"), commonAncestor)
	close(reader.resume)
	require.NoError(t, receiveBlobDownloaderError(t, passDone, "completion audit pass"))

	d.mu.RLock()
	for _, completed := range d.completedRanges {
		require.LessOrEqual(t, completed.end, commonAncestor)
	}
	d.mu.RUnlock()

	block, _ := makeBlobBoundaryObjects(t, oldHead, 1)
	d.blockReader = &mappedBlobBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{oldHead: block}}
	d.blobStorage = newBlobBoundaryStorage(t)
	requested := make(chan struct{}, 1)
	wantErr := errors.New("new canonical suffix unavailable")
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		requested <- struct{}{}
		return nil, wantErr
	}
	d.SetHead(oldHead, common.HexToHash("0x03"), oldHead)
	require.True(t, d.BlobBackfillPending(oldHead))
	require.ErrorIs(t, d.downloadOnce(false), wantErr)
	select {
	case <-requested:
	default:
		t.Fatal("new canonical suffix was not requested")
	}
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		return &PeerAndSidecars{Peer: "peer", Responses: firstSidecars}, nil
	}

	require.Error(t, d.downloadOnce(false))
	_, complete, err := storage.ReadBlobSidecars(t.Context(), headSlot, firstRoot)
	require.NoError(t, err)
	require.True(t, complete)
	require.False(t, d.backfillCompleted.Load())
}

func TestBlobHistoryDownloaderBoundsAuditWhileAnotherSlotRemainsPending(t *testing.T) {
	const headSlot = uint64(100)
	completeBlock, completeSidecars := makeBlobBoundaryObjects(t, headSlot, 1)
	missingBlock, _ := makeBlobBoundaryObjects(t, headSlot-1, 1)
	completeRoot, err := completeBlock.Block.HashSSZ()
	require.NoError(t, err)
	storage := newBlobBoundaryStorage(t)
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), completeRoot, completeSidecars))
	reader := &mappedBlobBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{
		headSlot:     completeBlock,
		headSlot - 1: missingBlock,
	}}
	d := newBlobDownloaderForBoundaryTest(t, headSlot, headSlot-1, headSlot-1, 16, reader)
	d.blobStorage = storage
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		return nil, errors.New("blob unavailable")
	}

	require.Error(t, d.downloadOnce(false))
	reader.slots = nil
	require.Error(t, d.downloadOnce(false))
	require.Contains(t, reader.slots, headSlot)
	require.Contains(t, reader.slots, headSlot-1)
	require.Len(t, reader.slots, 2)
}

func TestBlobHistoryDownloaderAuditsCompletedSlotWhileAnotherSlotRemainsPending(t *testing.T) {
	const completeSlot = uint64(100)
	const pendingSlot = completeSlot - 1
	completeBlock, completeSidecars := makeBlobBoundaryObjects(t, completeSlot, 1)
	pendingBlock, _ := makeBlobBoundaryObjects(t, pendingSlot, 1)
	completeRoot, err := completeBlock.Block.HashSSZ()
	require.NoError(t, err)
	underlying := newBlobBoundaryStorage(t)
	require.NoError(t, underlying.WriteBlobSidecars(t.Context(), completeRoot, completeSidecars))
	storage := &lossyBlobStorage{BlobStorage: underlying}
	d := newBlobDownloaderForBoundaryTest(t, completeSlot, pendingSlot, pendingSlot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{completeSlot: completeBlock, pendingSlot: pendingBlock},
	})
	d.blobStorage = storage
	requestedRoots := make([][]common.Hash, 0, 2)
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], _ func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		roots := make([]common.Hash, 0, req.Len())
		for index := range req.Len() {
			roots = append(roots, req.Get(index).BlockRoot)
		}
		requestedRoots = append(requestedRoots, roots)
		return nil, errors.New("blob unavailable")
	}

	require.Error(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(completeSlot))
	storage.missing.Store(true)

	require.Error(t, d.downloadOnce(false))
	require.True(t, d.BlobBackfillPending(completeSlot))
	require.Len(t, requestedRoots, 2)
	require.Contains(t, requestedRoots[1], common.Hash(completeRoot))
}

func TestBlobHistoryDownloaderDropsFrozenCompletedRangeFragments(t *testing.T) {
	const (
		headSlot          = uint64(200_000)
		firstUnfrozenSlot = uint64(199_000)
		fragmentCount     = 100_000
	)
	d := newBlobDownloaderForBoundaryTest(t, headSlot, firstUnfrozenSlot, 1, 0, &recordingBlobBlockReader{})
	d.completedRanges = make([]backfillRange, 0, fragmentCount)
	for slot := uint64(1); slot < headSlot; slot += 2 {
		d.completedRanges = append(d.completedRanges, backfillRange{start: slot, end: slot})
	}

	require.NoError(t, d.downloadOnce(false))
	require.False(t, d.BlobBackfillPending(firstUnfrozenSlot-2))
	require.LessOrEqual(t, len(d.completedRanges), int((headSlot-firstUnfrozenSlot)/2))
	require.LessOrEqual(t, cap(d.completedRanges), int(headSlot-firstUnfrozenSlot))
	for _, completed := range d.completedRanges {
		require.GreaterOrEqual(t, completed.start, firstUnfrozenSlot)
	}
}

func TestBlobHistoryDownloaderDoesNotRestoreCompletionAcrossFrozenReorgBoundary(t *testing.T) {
	d := newBlobDownloaderForBoundaryTest(t, 100, 80, 1, 0, &recordingBlobBlockReader{})
	d.addPassCompletedRanges(&backfillPass{safeThrough: 50}, []backfillRange{{start: 1, end: 100}})

	require.Empty(t, d.completedRanges)
}

func TestBlobHistoryDownloaderDoesNotTrustIndexedCountWhenSidecarIsMissing(t *testing.T) {
	const slot = uint64(100)
	block, _ := makeBlobBoundaryObjects(t, slot, 1)
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 1, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block},
	})
	d.blobStorage = indexedButMissingBlobStorage{}
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		return nil, errors.New("recovery required")
	}

	require.ErrorContains(t, d.downloadOnce(false), "recovery required")
	require.False(t, d.backfillCompleted.Load())
	require.True(t, d.BlobBackfillPending(slot))
}

func TestBlobHistoryDownloaderRepairsCorruptDurableSidecar(t *testing.T) {
	const slot = uint64(100)
	block, sidecars := makeBlobBoundaryObjects(t, slot, 1)
	storage := &corruptBlobStorage{}
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block},
	})
	d.blobStorage = storage
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		return &PeerAndSidecars{Peer: "peer", Responses: sidecars}, nil
	}

	require.Error(t, d.downloadOnce(false))
	require.True(t, storage.removed)
	require.True(t, storage.written)
}

func TestBlobHistoryDownloaderRevalidatesCompletedRangeAfterDurableLoss(t *testing.T) {
	const slot = uint64(100)
	block, sidecars := makeBlobBoundaryObjects(t, slot, 1)
	underlying := newBlobBoundaryStorage(t)
	storage := &lossyBlobStorage{BlobStorage: underlying}
	d := newBlobDownloaderForBoundaryTest(t, slot, slot, slot, 16, &mappedBlobBlockReader{
		blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block},
	})
	d.blobStorage = storage
	requests := 0
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
		requests++
		return &PeerAndSidecars{Peer: "peer", Responses: sidecars}, nil
	}

	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, 1, requests)
	storage.missing.Store(true)

	require.NoError(t, d.downloadOnce(false))
	require.Equal(t, 2, requests)
	require.False(t, storage.missing.Load())
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
	d.requestBlobs = func(_ context.Context, _ BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], _ func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
			d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	d.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	restarted.requestBlobs = func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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

type barrierBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *barrierBlobBlockReader) ReadBeaconBlockBodyBySlot(context.Context, kv.Tx, uint64) (*cltypes.SignedBeaconBlock, error) {
	r.once.Do(func() {
		close(r.entered)
		<-r.resume
	})
	return nil, nil
}

type mappedBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
	slots  []uint64
}

func (r *mappedBlobBlockReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
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

type lossyBlobStorage struct {
	blob_storage.BlobStorage
	missing atomic.Bool
}

func (s *lossyBlobStorage) ReadBlobSidecars(ctx context.Context, slot uint64, root common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	sidecars, complete, err := s.BlobStorage.ReadBlobSidecars(ctx, slot, root)
	if err != nil || !s.missing.Load() || len(sidecars) == 0 {
		return sidecars, complete, err
	}
	return sidecars[:len(sidecars)-1], false, nil
}

func (s *lossyBlobStorage) WriteBlobSidecars(ctx context.Context, root common.Hash, sidecars []*cltypes.BlobSidecar) error {
	if err := s.BlobStorage.WriteBlobSidecars(ctx, root, sidecars); err != nil {
		return err
	}
	s.missing.Store(false)
	return nil
}

type corruptBlobStorage struct {
	emptyBlobStorage
	removed bool
	written bool
}

func (*corruptBlobStorage) KzgCommitmentsCount(context.Context, common.Hash) (uint32, error) {
	return 1, nil
}

func (*corruptBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	return nil, false, blob_storage.ErrBlobSidecarCorrupt
}

func (s *corruptBlobStorage) RemoveBlobSidecars(context.Context, uint64, common.Hash) error {
	s.removed = true
	return nil
}

func (s *corruptBlobStorage) WriteBlobSidecars(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
	s.written = true
	return errors.New("test stops after repair write")
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

type notifyingUnsyncedChecker struct{ checked chan struct{} }

func (s notifyingUnsyncedChecker) Synced() bool {
	select {
	case s.checked <- struct{}{}:
	default:
	}
	return false
}

func TestBlobHistoryDownloaderUnsyncedWaitObservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d := newBlobDownloaderForBoundaryTest(t, 100, 100, 1, 1, &recordingBlobBlockReader{})
	d.ctx = ctx
	checked := make(chan struct{}, 1)
	d.syncedChecker = notifyingUnsyncedChecker{checked: checked}
	done := make(chan error, 1)
	go func() { done <- d.downloadOnce(false) }()
	waitBlobDownloaderSignal(t, checked, "sync status check")
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("unsynced wait ignored downloader cancellation")
	}
}

type peerCountClient struct{ active uint64 }

func (p peerCountClient) Peers() (uint64, error) { return p.active, nil }

func (peerCountClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return nil, "", errors.New("unexpected blob request")
}
