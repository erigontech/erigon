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
	"encoding/binary"
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
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/p2p/enode"
)

// maliciousColumnSentinel stands in for a selected custody peer. It answers
// metadata honestly so it becomes eligible for column requests, then serves a
// fixed response to every column request.
type maliciousColumnSentinel struct {
	sentinelproto.SentinelClient
	metadataResponse []byte
	columnResponse   []byte
	columnResponses  [][]byte
	peerEnodeID      string

	metadataOnce    sync.Once
	metadataReady   chan struct{}
	columnOnce      sync.Once
	columnRequested chan struct{}
	releaseColumn   chan struct{}
	columnCalls     atomic.Int32
	banOnce         sync.Once
	banned          chan struct{}
}

func (s *maliciousColumnSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{Peers: []*sentinelproto.Peer{{Pid: "malicious-peer", EnodeId: s.peerEnodeID}}}, nil
}

func (s *maliciousColumnSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.banOnce.Do(func() { close(s.banned) })
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *maliciousColumnSentinel) SendPeerRequest(ctx context.Context, req *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	peer := &sentinelproto.Peer{Pid: "malicious-peer"}
	if strings.Contains(req.Topic, "metadata") {
		s.metadataOnce.Do(func() { close(s.metadataReady) })
		return &sentinelproto.ResponseData{Data: s.metadataResponse, Peer: peer}, nil
	}
	if s.columnRequested != nil {
		s.columnOnce.Do(func() { close(s.columnRequested) })
	}
	if s.releaseColumn != nil {
		select {
		case <-s.releaseColumn:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	response := s.columnResponse
	if len(s.columnResponses) > 0 {
		index := min(int(s.columnCalls.Add(1))-1, len(s.columnResponses)-1)
		response = s.columnResponses[index]
	}
	return &sentinelproto.ResponseData{Data: response, Peer: peer}, nil
}

type writeObservingColumnStorage struct {
	blob_storage.DataColumnStorage
	lookups chan struct{}
	writes  chan *cltypes.DataColumnSidecar
}

func (s *writeObservingColumnStorage) ColumnSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (bool, error) {
	select {
	case s.lookups <- struct{}{}:
	default:
	}
	return s.DataColumnStorage.ColumnSidecarExists(ctx, slot, blockRoot, columnIndex)
}

func (s *writeObservingColumnStorage) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, sidecar *cltypes.DataColumnSidecar) error {
	if err := s.DataColumnStorage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar); err != nil {
		return err
	}
	select {
	case s.writes <- sidecar:
	default:
	}
	return nil
}

func newColumnResponseRPC(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, responses ...[]*cltypes.DataColumnSidecar) (*rpc.BeaconRpcP2P, *maliciousColumnSentinel) {
	return newColumnResponseRPCWithPeer(t, cfg, currentSlot, "", cfg.NumberOfColumns, responses...)
}

func newColumnResponseRPCWithPeer(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, peerEnodeID string, custodyGroupCount uint64, responses ...[]*cltypes.DataColumnSidecar) (*rpc.BeaconRpcP2P, *maliciousColumnSentinel) {
	return newColumnResponseRPCWithPeerAndForkEpoch(t, cfg, currentSlot, peerEnodeID, custodyGroupCount, cfg.FuluForkEpoch, responses...)
}

func newColumnResponseRPCWithPeerAndForkEpoch(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, peerEnodeID string, custodyGroupCount, responseForkEpoch uint64, responses ...[]*cltypes.DataColumnSidecar) (*rpc.BeaconRpcP2P, *maliciousColumnSentinel) {
	t.Helper()
	genesisTime := uint64(time.Now().Unix()) - currentSlot*cfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, cfg)
	require.GreaterOrEqual(t, clock.StateVersionByEpoch(clock.GetCurrentEpoch()), clparams.FuluVersion)
	digest, err := clock.ComputeForkDigest(responseForkEpoch)
	require.NoError(t, err)
	encoded := make([][]byte, len(responses))
	for i, sidecars := range responses {
		var wire bytes.Buffer
		for j, sidecar := range sidecars {
			if j > 0 {
				require.NoError(t, wire.WriteByte(0))
			}
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, sidecar, digest[:]...))
		}
		encoded[i] = wire.Bytes()
	}

	syncnets := [1]byte{}
	metadata := &cltypes.Metadata{Syncnets: &syncnets, CustodyGroupCount: &custodyGroupCount}
	var metadataWire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&metadataWire, metadata))
	sentinel := &maliciousColumnSentinel{
		metadataResponse: metadataWire.Bytes(),
		columnResponses:  encoded,
		peerEnodeID:      peerEnodeID,
		metadataReady:    make(chan struct{}),
		banned:           make(chan struct{}),
	}
	rpcClient := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock, nil)
	select {
	case <-sentinel.metadataReady:
	case <-time.After(30 * time.Second):
		t.Fatal("column response peer was not added to the custody-peer queue")
	}
	return rpcClient, sentinel
}

func TestRunDownloadRejectsResponseForkDigestMismatchBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	currentSlot := (cfg.FuluForkEpoch + 1) * cfg.SlotsPerEpoch
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, currentSlot)
	rpcClient, sentinel := newColumnResponseRPCWithPeerAndForkEpoch(
		t,
		&cfg,
		currentSlot,
		"",
		cfg.NumberOfColumns,
		cfg.DenebForkEpoch,
		[]*cltypes.DataColumnSidecar{columns[0]},
	)

	storage := &writeObservingColumnStorage{
		DataColumnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatal("fork-digest mismatch reached storage lookup")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("fork-digest mismatch was neither rejected nor processed")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestRunDownloadRejectsStaleBPOForkDigestBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	require.GreaterOrEqual(t, len(cfg.BlobSchedule), 2)
	earlierEpoch := cfg.BlobSchedule[0].Epoch
	laterEpoch := cfg.BlobSchedule[1].Epoch
	currentSlot := laterEpoch * cfg.SlotsPerEpoch

	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-currentSlot*cfg.SecondsPerSlot, common.Hash{}, &cfg)
	earlierDigest, err := clock.ComputeForkDigest(earlierEpoch)
	require.NoError(t, err)
	laterDigest, err := clock.ComputeForkDigest(laterEpoch)
	require.NoError(t, err)
	require.NotEqual(t, earlierDigest, laterDigest)
	earlierVersion, err := clock.StateVersionByForkDigest(earlierDigest)
	require.NoError(t, err)
	laterVersion, err := clock.StateVersionByForkDigest(laterDigest)
	require.NoError(t, err)
	require.Equal(t, clparams.FuluVersion, earlierVersion)
	require.Equal(t, clparams.FuluVersion, laterVersion)

	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, currentSlot)
	rpcClient, sentinel := newColumnResponseRPCWithPeerAndForkEpoch(
		t,
		&cfg,
		currentSlot,
		"",
		cfg.NumberOfColumns,
		earlierEpoch,
		[]*cltypes.DataColumnSidecar{columns[0]},
	)

	storage := &writeObservingColumnStorage{
		DataColumnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatal("stale BPO fork digest reached storage lookup")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("stale BPO fork digest was neither rejected nor processed")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestRunDownloadAcceptsExactBPOForkDigest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	require.GreaterOrEqual(t, len(cfg.BlobSchedule), 2)

	for _, test := range []struct {
		name    string
		epoch   uint64
		blinded bool
	}{
		{name: "first BPO epoch with full block metadata", epoch: cfg.BlobSchedule[0].Epoch},
		{name: "second BPO epoch with blinded block metadata", epoch: cfg.BlobSchedule[1].Epoch, blinded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			currentSlot := test.epoch * cfg.SlotsPerEpoch
			executionBlockHash := common.Hash{}
			if test.blinded {
				executionBlockHash = common.HexToHash("0x01")
			}
			block, root, _, columns := recoverableFuluDataAtSlotWithExecutionBlockHash(t, &cfg, currentSlot, executionBlockHash)
			var recoveryBlock cltypes.ColumnSyncableSignedBlock = block
			if test.blinded {
				blinded, err := block.Blinded()
				require.NoError(t, err)
				recoveryBlock = blinded
			}
			recoveryRoot, err := recoveryBlock.BlockHashSSZ()
			require.NoError(t, err)
			require.Equal(t, root, common.Hash(recoveryRoot))
			metadata, err := newBlobRecoveryMetadata(recoveryBlock, root)
			require.NoError(t, err)
			rpcClient, sentinel := newColumnResponseRPCWithPeerAndForkEpoch(
				t,
				&cfg,
				currentSlot,
				"",
				cfg.NumberOfColumns,
				test.epoch,
				[]*cltypes.DataColumnSidecar{columns[0]},
			)

			storage := &writeObservingColumnStorage{
				DataColumnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
				lookups:           make(chan struct{}, 1),
				writes:            make(chan *cltypes.DataColumnSidecar, 1),
			}
			d := &peerdas{
				rpc:           rpcClient,
				beaconConfig:  &cfg,
				state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
				columnStorage: storage,
			}
			req := &downloadRequest{
				beaconConfig: &cfg,
				downloadTable: map[downloadTableEntry]map[uint64]bool{
					{blockRoot: root, slot: block.GetSlot()}: {0: true},
				},
				recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
				validatedBlobCount: map[common.Hash]uint32{root: 0},
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); d.runDownload(ctx, req, true) }()

			select {
			case sidecar := <-storage.writes:
				require.Equal(t, uint64(0), sidecar.Index)
			case <-sentinel.banned:
				cancel()
				<-done
				t.Fatal("exact BPO fork digest was banned")
			case <-time.After(30 * time.Second):
				cancel()
				<-done
				t.Fatal("exact BPO fork digest did not reach storage")
			}
			cancel()
			<-done
			require.Equal(t, 0, req.remainingEntriesCount())
		})
	}
}

func TestRunDownloadRejectsFuluSignatureMismatchBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	mismatchedHeader := *columns[1].SignedBlockHeader
	mismatchedHeader.Signature[0] ^= 1
	columns[1].SignedBlockHeader = &mismatchedHeader
	require.True(t, VerifyDataColumnSidecar(columns[1]))
	require.True(t, VerifyDataColumnSidecarInclusionProof(columns[1]))
	require.True(t, VerifyDataColumnSidecarKZGProofs(columns[1]))

	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, block.GetSlot(), []*cltypes.DataColumnSidecar{columns[0], columns[1]})
	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true, 1: true},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 0},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, true) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		select {
		case <-storage.writes:
			cancel()
			<-done
			t.Fatal("signature-mismatched Fulu column was persisted")
		case <-sentinel.banned:
			cancel()
			<-done
			t.Fatal("signature mismatch reached storage lookup before rejection")
		case <-time.After(30 * time.Second):
			cancel()
			<-done
			t.Fatal("signature mismatch reached storage lookup without a terminal result")
		}
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("signature mismatch was neither rejected nor processed")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
	_, remaining := req.requestData()
	require.Len(t, remaining, 2)
	saved, err := baseStorage.GetSavedColumnIndex(t.Context(), block.GetSlot(), root)
	require.NoError(t, err)
	require.Empty(t, saved)
}

func TestRunDownloadAcceptsGloasSidecarWithRecoveryMetadata(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	currentSlot := cfg.GloasForkEpoch * cfg.SlotsPerEpoch
	block, root, columns := recoverableGloasColumns(t, &cfg, currentSlot)
	metadata, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	require.True(t, metadata.hasSignature)
	rpcClient, sentinel := newColumnResponseRPCWithPeerAndForkEpoch(
		t,
		&cfg,
		currentSlot,
		"",
		cfg.NumberOfColumns,
		cfg.GloasForkEpoch,
		[]*cltypes.DataColumnSidecar{columns[0]},
	)

	storage := &writeObservingColumnStorage{
		DataColumnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("gloasSignaturePreflight", 1)
	require.NoError(t, err)
	gloasDataCache.Add(root, &gloasBlockData{
		BlobKzgCommitments:      block.GetBlobKzgCommitments(),
		SignedBeaconBlockHeader: block.SignedBeaconBlockHeader(),
	})
	d := &peerdas{
		rpc:            rpcClient,
		beaconConfig:   &cfg,
		state:          peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage:  storage,
		gloasDataCache: gloasDataCache,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true},
		},
		recoveryDetails:    map[common.Hash]*blobRecoveryMetadata{root: metadata},
		validatedBlobCount: map[common.Hash]uint32{root: 0},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, true) }()

	select {
	case sidecar := <-storage.writes:
		require.Equal(t, uint64(0), sidecar.Index)
	case <-sentinel.banned:
		cancel()
		<-done
		t.Fatal("valid Gloas sidecar was rejected by Fulu signature preflight")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("valid Gloas sidecar did not reach storage")
	}
	cancel()
	<-done
	require.Equal(t, 0, req.remainingEntriesCount())
}

func TestRunDownloadRejectsColumnFilteredOutOfWireRequest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)

	var nodeID enode.ID
	foundPeer := false
	for candidate := range uint64(10_000) {
		binary.LittleEndian.PutUint64(nodeID[:8], candidate)
		mask, err := peerdasutils.GetCustodyColumns(nodeID, 1)
		require.NoError(t, err)
		if mask[0] && !mask[1] {
			foundPeer = true
			break
		}
	}
	require.True(t, foundPeer)
	rpcClient, sentinel := newColumnResponseRPCWithPeer(t, &cfg, block.GetSlot(), nodeID.String(), 1, []*cltypes.DataColumnSidecar{columns[1]})

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true, 1: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatal("column excluded from the wire request reached storage lookup")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("column excluded from the wire request was neither rejected nor stored")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestRunDownloadRejectsValidUnrequestedFuluIdentityBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	blockA, rootA, _, _ := recoverableFuluDataAtSlot(t, &cfg, 100)
	_, rootB, _, columnsB := recoverableFuluDataAtSlot(t, &cfg, 101)
	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, blockA.GetSlot(), []*cltypes.DataColumnSidecar{columnsB[1]})

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: rootA, slot: blockA.GetSlot()}: {0: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatalf("unrequested root %x reached storage lookup", rootB)
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("unrequested sidecar was neither rejected nor stored")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount(), "intended root was removed by an unrelated response")
}

func TestRunDownloadRejectsOverCardinalityBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)
	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, block.GetSlot(), []*cltypes.DataColumnSidecar{columns[0], columns[0]})

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatal("over-cardinality response reached storage lookup")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("over-cardinality response was neither rejected nor stored")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestRunDownloadRejectsDuplicateRequestedTupleBeforeStorage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)
	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, block.GetSlot(), []*cltypes.DataColumnSidecar{columns[0], columns[0]})

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true, 1: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.banned:
	case <-storage.lookups:
		cancel()
		<-done
		t.Fatal("duplicate requested tuple reached storage lookup")
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("duplicate response tuple was neither rejected nor processed")
	}
	cancel()
	<-done
	require.Equal(t, 1, req.remainingEntriesCount())
}

func TestRunDownloadAcceptsPartialReorderedRequestedSubset(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)
	metadata, err := newBlobRecoveryMetadata(&blockHashCountingBlock{ColumnSyncableSignedBlock: block}, root)
	require.NoError(t, err)
	require.False(t, metadata.hasSignature)
	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, block.GetSlot(), []*cltypes.DataColumnSidecar{columns[2], columns[0]}, nil)

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 8),
		writes:            make(chan *cltypes.DataColumnSidecar, 2),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true, 1: true, 2: true},
		},
		recoveryDetails: map[common.Hash]*blobRecoveryMetadata{root: metadata},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	written := map[uint64]bool{}
	for len(written) < 2 {
		select {
		case sidecar := <-storage.writes:
			written[sidecar.Index] = true
		case <-sentinel.banned:
			cancel()
			<-done
			t.Fatal("legal partial reordered response was banned")
		case <-time.After(30 * time.Second):
			cancel()
			<-done
			t.Fatal("legal partial reordered response did not reach storage")
		}
	}
	cancel()
	<-done
	require.Equal(t, map[uint64]bool{0: true, 2: true}, written)
	_, remaining := req.requestData()
	require.Equal(t, map[requestedDataColumn]struct{}{
		{slot: block.GetSlot(), blockRoot: root, index: 1}: {},
	}, remaining)
}

func TestRunDownloadAcceptsLateSnapshotMemberAfterConcurrentCompletion(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, root, _, columns := recoverableFuluDataAtSlot(t, &cfg, 100)
	rpcClient, sentinel := newColumnResponseRPC(t, &cfg, block.GetSlot(), []*cltypes.DataColumnSidecar{columns[0]})
	sentinel.columnRequested = make(chan struct{})
	sentinel.releaseColumn = make(chan struct{})

	baseStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
	storage := &writeObservingColumnStorage{
		DataColumnStorage: baseStorage,
		lookups:           make(chan struct{}, 1),
		writes:            make(chan *cltypes.DataColumnSidecar, 1),
	}
	d := &peerdas{
		rpc:           rpcClient,
		beaconConfig:  &cfg,
		state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
		columnStorage: storage,
	}
	req := &downloadRequest{
		beaconConfig: &cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: root, slot: block.GetSlot()}: {0: true},
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

	select {
	case <-sentinel.columnRequested:
	case <-time.After(30 * time.Second):
		t.Fatal("column request did not reach the selected peer")
	}
	require.NoError(t, baseStorage.WriteColumnSidecars(t.Context(), root, 0, columns[0]))
	req.removeColumn(block.GetSlot(), root, 0)
	close(sentinel.releaseColumn)

	select {
	case <-storage.lookups:
	case <-sentinel.banned:
		t.Fatal("late response member was checked against mutable current membership")
	case <-time.After(30 * time.Second):
		t.Fatal("late response was not processed")
	}
	select {
	case <-done:
	case <-sentinel.banned:
		t.Fatal("late response member was falsely banned")
	case <-time.After(30 * time.Second):
		t.Fatal("runDownload did not finish after concurrent completion")
	}
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

	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, true) }()
	select {
	case <-done:
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

	d.runDownload(ctx, req, true)
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

	d.runDownload(ctx, req, true)
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

	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, true) }()
	select {
	case <-done:
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

	d.runDownload(ctx, req, true)
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

	done := make(chan struct{})
	go func() { defer close(done); d.runDownload(ctx, req, false) }()

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
