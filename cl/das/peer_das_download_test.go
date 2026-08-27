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
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

// maliciousColumnSentinel stands in for a selected custody peer. It answers
// metadata honestly so it becomes eligible for column requests, then serves a
// fixed response to every column request.
type maliciousColumnSentinel struct {
	sentinelproto.SentinelClient
	metadataResponse []byte
	columnResponse   []byte

	metadataOnce  sync.Once
	metadataReady chan struct{}
	banOnce       sync.Once
	banned        chan struct{}
}

func (s *maliciousColumnSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{Peers: []*sentinelproto.Peer{{Pid: "malicious-peer"}}}, nil
}

func (s *maliciousColumnSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.banOnce.Do(func() { close(s.banned) })
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *maliciousColumnSentinel) SendPeerRequest(_ context.Context, req *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	peer := &sentinelproto.Peer{Pid: "malicious-peer"}
	if strings.Contains(req.Topic, "metadata") {
		s.metadataOnce.Do(func() { close(s.metadataReady) })
		return &sentinelproto.ResponseData{Data: s.metadataResponse, Peer: peer}, nil
	}
	return &sentinelproto.ResponseData{Data: s.columnResponse, Peer: peer}, nil
}

func TestRunDownloadDoesNotRevalidateBlobStorageWhileWaitingForColumns(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	var halfChecks atomic.Int32
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			if halfChecks.Add(1) == 2 {
				cancel()
			}
			return nil, nil
		},
	).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	var durableReads atomic.Int32
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil).AnyTimes()
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			durableReads.Add(1)
			return nil, false, nil
		},
	).AnyTimes()

	d := &peerdas{
		beaconConfig:  &cfg,
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
	}
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.Block.Slot}: {},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 1},
	}

	done := make(chan error, 1)
	go func() { done <- d.runDownload(ctx, req, true) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("download did not stop after its context was canceled")
	}
	require.Zero(t, durableReads.Load(), "storage validation must be event-driven, not ticker-driven")
}

func TestRunDownloadValidatesBlobStorageAfterCountChanges(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, _ := recoverableFuluData(t, &cfg)

	ctx, cancel := context.WithCancel(t.Context())
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	var halfChecks atomic.Int32
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			if halfChecks.Add(1) == 2 {
				cancel()
			}
			return nil, nil
		},
	).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(len(sidecars)), nil).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			durableReads.Add(1)
			return sidecars, true, nil
		},
	).AnyTimes()
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.Block.Slot}: {},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 1},
	}

	require.NoError(t, d.runDownload(ctx, req, true))
	require.Equal(t, int32(1), durableReads.Load(), "a changed durable count must trigger one validation event")
}

func TestRunDownloadRetriesTransientBlobReadAtUnchangedCount(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, sidecars, _ := recoverableFuluData(t, &cfg)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	var halfChecks atomic.Int32
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			if halfChecks.Add(1) == 3 {
				cancel()
			}
			return nil, nil
		},
	).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(len(sidecars)), nil).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			if durableReads.Add(1) == 1 {
				return nil, false, errors.New("transient read failure")
			}
			return sidecars, true, nil
		},
	).AnyTimes()
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.Block.Slot}: {},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 1},
	}

	require.NoError(t, d.runDownload(ctx, req, true))
	require.Equal(t, int32(2), durableReads.Load(), "transient validation failure must retry without a count mutation")
	require.NoError(t, ctx.Err(), "valid storage must finish the request before lifecycle cancellation")
}

func TestRunDownloadPacesPermanentBlobReadErrorsUntilCancellation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).Return(nil, nil).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			if durableReads.Add(1) == 2 {
				cancel()
			}
			return nil, false, errors.New("permanent read failure")
		},
	).AnyTimes()
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.Block.Slot}: {},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 0},
	}

	done := make(chan error, 1)
	go func() { done <- d.runDownload(ctx, req, true) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("permanent read errors were not paced by the download lifecycle")
	}
	require.Equal(t, int32(2), durableReads.Load())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestRunDownloadDoesNotRepeatConclusiveInvalidBlobValidation(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 100
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	var halfChecks atomic.Int32
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]uint64, error) {
			if halfChecks.Add(1) == 2 {
				cancel()
			}
			return nil, nil
		},
	).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), root).Return(uint32(1), nil).AnyTimes()
	var durableReads atomic.Int32
	blobStorage.EXPECT().ReadBlobSidecars(gomock.Any(), block.Block.Slot, root).DoAndReturn(
		func(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
			durableReads.Add(1)
			return []*cltypes.BlobSidecar{nil}, true, nil
		},
	).AnyTimes()
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	d := &peerdas{beaconConfig: &cfg, columnStorage: columnStorage, blobStorage: blobStorage}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.Block.Slot}: {},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 0},
	}

	require.NoError(t, d.runDownload(ctx, req, true))
	require.Equal(t, int32(1), durableReads.Load(), "conclusively invalid storage must stay cached until its count changes")
}

// A peer selects the response fork digest, so it can serve a Gloas-schema
// sidecar (no SignedBlockHeader) that claims a pre-Gloas slot. runDownload must
// ban the peer rather than dereference the header the schema left unset; the
// per-sidecar goroutine has no recover, so a nil deref there kills the process.
//
// This drives the real BeaconRpcP2P decode path, so it also pins that
// DataColumnSidecar.Version() reflects the peer's digest choice.
func TestRunDownloadRejectsGloasSidecarWithPreGloasSlot(t *testing.T) {
	// Kept at mainnet's schedule: Gloas sits at FAR_FUTURE_EPOCH, so its digest is
	// registered and resolvable even though no slot maps to Gloas.
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)

	currentSlot := (cfg.FuluForkEpoch + 1) * cfg.SlotsPerEpoch
	genesisTime := uint64(time.Now().Unix()) - currentSlot*cfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &cfg)

	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	decodeVersion, err := clock.StateVersionByForkDigest(gloasDigest)
	require.NoError(t, err)
	require.Equal(t, clparams.GloasVersion, decodeVersion, "far-future Gloas digest must resolve")

	// Advertising every custody group makes this peer eligible for any column.
	cgc := cfg.NumberOfColumns
	syncnets := [1]byte{}
	metadata := &cltypes.Metadata{Syncnets: &syncnets, CustodyGroupCount: &cgc}
	var metadataWire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&metadataWire, metadata))

	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot = 0
	sidecar.BeaconBlockRoot = common.HexToHash("0x1234")
	require.Nil(t, sidecar.SignedBlockHeader, "Gloas schema must leave SignedBlockHeader unset")
	var columnWire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&columnWire, sidecar, gloasDigest[:]...))

	sentinel := &maliciousColumnSentinel{
		metadataResponse: metadataWire.Bytes(),
		columnResponse:   columnWire.Bytes(),
		metadataReady:    make(chan struct{}),
		banned:           make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, &cfg, clock, nil)

	select {
	case <-sentinel.metadataReady:
	case <-time.After(30 * time.Second):
		t.Fatal("malicious peer was not added to the custody-peer queue")
	}

	// gloasDataCache is populated so that a sidecar wrongly accepted as Gloas
	// fails this test's ban assertion rather than nil-panicking downstream.
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("gloasDataCacheTest", 8)
	require.NoError(t, err)
	d := &peerdas{
		rpc:            rpcClient,
		beaconConfig:   &cfg,
		state:          peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:  blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
		gloasDataCache: gloasDataCache,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: common.HexToHash("0xbeef"), slot: currentSlot}: {0: true},
		},
	}

	done := make(chan error, 1)
	go func() { done <- d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-time.After(30 * time.Second):
		t.Fatal("malicious sidecar was never rejected")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runDownload did not return after cancellation")
	}

	// Nothing may be stored: the sidecar is rejected before any write.
	saved, err := d.columnStorage.GetSavedColumnIndex(context.Background(), currentSlot, common.HexToHash("0xbeef"))
	require.NoError(t, err)
	require.Empty(t, saved)
}
