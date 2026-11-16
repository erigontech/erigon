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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	callTimesMu     sync.Mutex
	columnCallTimes []time.Time
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
	s.callTimesMu.Lock()
	s.columnCallTimes = append(s.columnCallTimes, time.Now())
	s.callTimesMu.Unlock()
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
	rpcClient := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
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

func TestRunDownloadDoesNotRepeatRequestsToPeerThatAnsweredNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		cfg.AltairForkEpoch = 0
		cfg.BellatrixForkEpoch = 0
		cfg.CapellaForkEpoch = 0
		cfg.DenebForkEpoch = 0
		cfg.ElectraForkEpoch = 0
		cfg.FuluForkEpoch = 0
		cfg.InitializeForkSchedule()
		initTestBeaconConfig(&cfg)
		rpcClient, sentinel := newColumnResponseRPC(t, &cfg, 100, nil)
		d := &peerdas{
			rpc:           rpcClient,
			beaconConfig:  &cfg,
			state:         peerdasstate.NewPeerDasState(&cfg, &clparams.NetworkConfig{}),
			columnStorage: blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()),
		}
		req := &downloadRequest{
			beaconConfig: &cfg,
			downloadTable: map[downloadTableEntry]map[uint64]bool{
				{blockRoot: common.Hash{1}, slot: 100}: {0: true},
			},
		}
		// Long enough to cross the 15 s peer refresh.
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		d.runDownload(ctx, req, false)
		synctest.Wait()

		sentinel.callTimesMu.Lock()
		defer sentinel.callTimesMu.Unlock()
		times := sentinel.columnCallTimes
		require.Greater(t, len(times), 1)
		for i := 1; i < len(times); i++ {
			require.GreaterOrEqual(t, times[i].Sub(times[i-1]), 2*time.Second, "gap before request %d", i)
		}
		require.Equal(t, 1, req.remainingEntriesCount())
	})
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
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, &cfg, clock)

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

// The mismatch rejection compares this signature against each column sidecar's header, so it has to
// come off the block itself — including through a wrapper, which a concrete type switch missed.
func TestNewBlobRecoveryMetadataCarriesTheSignature(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)

	block, root, _, _ := recoverableFuluDataAtSlot(t, &cfg, 100)
	signature := common.Bytes96{0xaa, 0xbb, 0xcc}
	block.Signature = signature
	require.NotEqual(t, common.Bytes96{}, block.BlockSignature(), "a zero signature would make this vacuous")

	direct, err := newBlobRecoveryMetadata(block, root)
	require.NoError(t, err)
	require.Equal(t, signature, direct.signature)

	wrapped, err := newBlobRecoveryMetadata(&blockHashCountingBlock{ColumnSyncableSignedBlock: block}, root)
	require.NoError(t, err)
	require.Equal(t, signature, wrapped.signature, "a wrapper must not drop the signature")
}

// failingWriteColumnStorage fails the write of one column index.
type failingWriteColumnStorage struct {
	blob_storage.DataColumnStorage
	index int64
}

func (s *failingWriteColumnStorage) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, sidecar *cltypes.DataColumnSidecar) error {
	if columnIndex == s.index {
		return errors.New("write failed")
	}
	return s.DataColumnStorage.WriteColumnSidecars(ctx, blockRoot, columnIndex, sidecar)
}

// fetchedBlock is a block whose columns 0 and 1 a by-root round asks for.
type fetchedBlock struct {
	slot    uint64
	root    common.Hash
	columns []*cltypes.DataColumnSidecar
	gloas   *gloasBlockData
}

func publishTestConfig() clparams.BeaconChainConfig {
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
	return cfg
}

func fuluFetchedBlock(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) fetchedBlock {
	t.Helper()
	_, root, _, columns := recoverableFuluDataAtSlot(t, cfg, slot)
	return fetchedBlock{slot: slot, root: root, columns: columns}
}

func gloasFetchedBlock(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) fetchedBlock {
	t.Helper()
	block, root, columns := recoverableGloasColumns(t, cfg, slot)
	return fetchedBlock{slot: slot, root: root, columns: columns, gloas: &gloasBlockData{
		BlobKzgCommitments:      block.GetBlobKzgCommitments(),
		SignedBeaconBlockHeader: block.SignedBeaconBlockHeader(),
	}}
}

// byRootRound is a custody node's by-root download round for blocks, answered by a peer with fixed responses. It
// runs inside a synctest bubble and records every publication the round schedules; a publication stays pending until
// the test ends.
type byRootRound struct {
	d         *peerdas
	sentinel  *maliciousColumnSentinel
	req       *downloadRequest
	store     blob_storage.DataColumnStorage
	mu        sync.Mutex
	published []recoveredColumnsPublish
}

func newByRootRound(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, store blob_storage.DataColumnStorage, blocks []fetchedBlock, response ...*cltypes.DataColumnSidecar) *byRootRound {
	t.Helper()
	return newByRootRoundWithResponses(t, cfg, currentSlot, store, blocks, response)
}

// newByRootRoundWithResponses is newByRootRound for a peer that answers its nth request with responses[n], and the last
// response from then on.
func newByRootRoundWithResponses(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, store blob_storage.DataColumnStorage, blocks []fetchedBlock, responses ...[]*cltypes.DataColumnSidecar) *byRootRound {
	t.Helper()
	table := make(map[downloadTableEntry]map[uint64]bool, len(blocks))
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("byRootPublish", len(blocks))
	require.NoError(t, err)
	responseForkEpoch := cfg.FuluForkEpoch
	for _, block := range blocks {
		table[downloadTableEntry{blockRoot: block.root, slot: block.slot}] = map[uint64]bool{0: true, 1: true}
		if block.gloas != nil {
			gloasDataCache.Add(block.root, block.gloas)
			responseForkEpoch = cfg.GloasForkEpoch
		}
	}
	rpcClient, sentinel := newColumnResponseRPCWithPeerAndForkEpoch(t, cfg, currentSlot, "", cfg.NumberOfColumns, responseForkEpoch, responses...)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	r := &byRootRound{
		sentinel: sentinel,
		req:      &downloadRequest{beaconConfig: cfg, downloadTable: table},
		store:    store,
		d: &peerdas{
			rpc:            rpcClient,
			beaconConfig:   cfg,
			caplinConfig:   &clparams.CaplinConfig{},
			state:          peerdasstate.NewPeerDasState(cfg, &clparams.NetworkConfig{}),
			columnStorage:  store,
			ethClock:       clock,
			gloasDataCache: gloasDataCache,
		},
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	r.d.SetColumnPublisher(func(ctx context.Context, sidecars []*cltypes.DataColumnSidecar) {
		r.mu.Lock()
		r.published = append(r.published, recoveredColumnsPublish{ctx: ctx, sidecars: sidecars})
		r.mu.Unlock()
		<-release
	})
	return r
}

func newPublishTestStore(cfg *clparams.BeaconChainConfig) blob_storage.DataColumnStorage {
	return blob_storage.NewDataColumnStore(afero.NewMemMapFs(), cfg, beaconevents.NewEventEmitter())
}

// run plays the round until it completes or times out, ends its context, and returns the publications it scheduled.
func (r *byRootRound) run(t *testing.T) []recoveredColumnsPublish {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	r.d.runDownload(ctx, r.req, r.d.IsArchivedMode())
	cancel()
	synctest.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.published)
}

// byRoot maps each published block root to the column indexes published for it, and requires one call per root.
func (r *byRootRound) byRoot(t *testing.T, published []recoveredColumnsPublish) map[common.Hash][]uint64 {
	t.Helper()
	columns := make(map[common.Hash][]uint64, len(published))
	for _, call := range published {
		require.NotEmpty(t, call.sidecars)
		_, first, _ := r.d.resolveColumnSidecarSlotAndRoot(call.sidecars[0])
		for _, sidecar := range call.sidecars {
			_, root, ok := r.d.resolveColumnSidecarSlotAndRoot(sidecar)
			require.True(t, ok)
			require.Equal(t, first, root, "a publish call carries one block root")
			columns[root] = append(columns[root], sidecar.Index)
		}
	}
	require.Len(t, columns, len(published), "one publish call per block root")
	return columns
}

func (r *byRootRound) requireStored(t *testing.T, block fetchedBlock, indexes ...int64) {
	t.Helper()
	for _, index := range indexes {
		exists, err := r.store.ColumnSidecarExists(t.Context(), block.slot, block.root, index)
		require.NoError(t, err)
		require.True(t, exists, "column %d of slot %d", index, block.slot)
	}
}

func TestRunDownloadPublishesColumnsFetchedByRoot(t *testing.T) {
	for _, test := range []struct {
		name  string
		slot  uint64
		fetch func(*testing.T, *clparams.BeaconChainConfig, uint64) fetchedBlock
	}{
		{name: "Fulu", slot: 40, fetch: fuluFetchedBlock},
		{name: "Gloas", slot: 70, fetch: gloasFetchedBlock},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := publishTestConfig()
				block := test.fetch(t, &cfg, test.slot)
				store := newPublishTestStore(&cfg)
				r := newByRootRound(t, &cfg, test.slot, store, []fetchedBlock{block}, block.columns[0], block.columns[1])

				published := r.run(t)

				require.Equal(t, map[common.Hash][]uint64{block.root: {0, 1}}, r.byRoot(t, published))
				require.NoError(t, published[0].ctx.Err(), "the publication outlives the round")
			})
		})
	}
}

func TestRunDownloadPublishesOnlyTheColumnsItStored(t *testing.T) {
	t.Run("a failed write", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := publishTestConfig()
			block := fuluFetchedBlock(t, &cfg, 40)
			store := newPublishTestStore(&cfg)
			r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{block}, block.columns[0], block.columns[1])
			r.d.columnStorage = &failingWriteColumnStorage{DataColumnStorage: store, index: 1}

			published := r.run(t)

			require.Equal(t, map[common.Hash][]uint64{block.root: {0}}, r.byRoot(t, published))
		})
	})
	t.Run("an invalid proof", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := publishTestConfig()
			block := fuluFetchedBlock(t, &cfg, 40)
			block.columns[1].KzgProofs.Get(0)[0] ^= 1
			require.False(t, VerifyDataColumnSidecarKZGProofs(block.columns[1]))
			store := newPublishTestStore(&cfg)
			r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{block}, block.columns[0], block.columns[1])

			published := r.run(t)

			require.Equal(t, map[common.Hash][]uint64{block.root: {0}}, r.byRoot(t, published))
		})
	})
	t.Run("a column stored from gossip while the request is in flight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := publishTestConfig()
			block := fuluFetchedBlock(t, &cfg, 40)
			store := newPublishTestStore(&cfg)
			r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{block}, block.columns[0], block.columns[1])
			r.sentinel.columnRequested = make(chan struct{})
			r.sentinel.releaseColumn = make(chan struct{})
			go func() {
				<-r.sentinel.columnRequested
				if err := store.WriteColumnSidecars(t.Context(), block.root, 0, block.columns[0]); err != nil {
					t.Error(err)
				}
				close(r.sentinel.releaseColumn)
			}()

			published := r.run(t)

			require.Equal(t, map[common.Hash][]uint64{block.root: {1}}, r.byRoot(t, published))
		})
	})
}

func TestRunDownloadPublishesOneCallPerBlockRoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := publishTestConfig()
		current := fuluFetchedBlock(t, &cfg, 40)
		previous := fuluFetchedBlock(t, &cfg, 39)
		store := newPublishTestStore(&cfg)
		r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{current, previous},
			current.columns[0], previous.columns[0], current.columns[1], previous.columns[1])

		published := r.run(t)

		require.Equal(t, map[common.Hash][]uint64{current.root: {0, 1}, previous.root: {0, 1}}, r.byRoot(t, published))
	})
}

// Each peer response is published on its own with the columns it stored, so a root answered twice gets two calls that
// share no column.
func TestRunDownloadPublishesEachPeerResponseOfARoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := publishTestConfig()
		block := fuluFetchedBlock(t, &cfg, 40)
		store := newPublishTestStore(&cfg)
		r := newByRootRoundWithResponses(t, &cfg, 40, store, []fetchedBlock{block},
			[]*cltypes.DataColumnSidecar{block.columns[0]}, []*cltypes.DataColumnSidecar{block.columns[1]})

		published := r.run(t)

		r.requireStored(t, block, 0, 1)
		columnsPerCall := make([][]uint64, 0, len(published))
		for _, call := range published {
			columnsPerCall = append(columnsPerCall, publishedColumnIndices(call.sidecars))
		}
		require.ElementsMatch(t, [][]uint64{{0}, {1}}, columnsPerCall)
	})
}

func TestRunDownloadPublishesOnlyRecentRoots(t *testing.T) {
	const currentSlot = 40
	for _, test := range []struct {
		name      string
		slot      uint64
		published bool
	}{
		{name: "current slot", slot: currentSlot, published: true},
		{name: "previous slot", slot: currentSlot - 1, published: true},
		{name: "two slots ago", slot: currentSlot - 2},
		{name: "future slot", slot: currentSlot + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := publishTestConfig()
				block := fuluFetchedBlock(t, &cfg, test.slot)
				store := newPublishTestStore(&cfg)
				r := newByRootRound(t, &cfg, currentSlot, store, []fetchedBlock{block}, block.columns[0], block.columns[1])

				published := r.run(t)

				r.requireStored(t, block, 0, 1)
				want := map[common.Hash][]uint64{}
				if test.published {
					want[block.root] = []uint64{0, 1}
				}
				require.Equal(t, want, r.byRoot(t, published))
			})
		})
	}

	t.Run("a recent and an old root in one response", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := publishTestConfig()
			recent := fuluFetchedBlock(t, &cfg, currentSlot)
			old := fuluFetchedBlock(t, &cfg, currentSlot-2)
			store := newPublishTestStore(&cfg)
			r := newByRootRound(t, &cfg, currentSlot, store, []fetchedBlock{recent, old},
				recent.columns[0], old.columns[0], recent.columns[1], old.columns[1])

			published := r.run(t)

			r.requireStored(t, old, 0, 1)
			require.Equal(t, map[common.Hash][]uint64{recent.root: {0, 1}}, r.byRoot(t, published))
		})
	})
}

func TestRunDownloadDoesNotPublishOnAnArchiveNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := publishTestConfig()
		block := fuluFetchedBlock(t, &cfg, 40)
		store := newPublishTestStore(&cfg)
		r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{block}, block.columns[0], block.columns[1])
		r.d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true}

		published := r.run(t)

		r.requireStored(t, block, 0, 1)
		require.Empty(t, published)
	})
}

func TestRunDownloadDoesNotPublishABackfilledRoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := publishTestConfig()
		block := fuluFetchedBlock(t, &cfg, 40)
		store := newPublishTestStore(&cfg)
		r := newByRootRound(t, &cfg, 40, store, []fetchedBlock{block}, block.columns[0], block.columns[1])
		r.d.blocksToCheckSync.add(block.root, &backfillColumnBlock{version: clparams.FuluVersion, slot: block.slot, root: block.root}, time.Time{})

		published := r.run(t)

		r.requireStored(t, block, 0, 1)
		require.Empty(t, published)
	})
}

// halfStoredRecoveryRound is a by-root round for block whose columns 2 to 63 are stored already, so that the two
// columns the round stores bring the block to half of its columns.
func halfStoredRecoveryRound(t *testing.T, cfg *clparams.BeaconChainConfig, currentSlot uint64, block fetchedBlock, supernode bool) *byRootRound {
	t.Helper()
	store := newPublishTestStore(cfg)
	for _, column := range block.columns[2 : cfg.NumberOfColumns/2] {
		require.NoError(t, store.WriteColumnSidecars(t.Context(), block.root, int64(column.Index), column))
	}
	r := newByRootRound(t, cfg, currentSlot, store, []fetchedBlock{block}, block.columns[0], block.columns[1])
	r.d.state.SetLocalNodeID(newCgcTestLocalNode(t))
	if supernode {
		r.d.state.SetCustodyGroupCount(cfg.NumberOfCustodyGroups, func() uint64 { return 0 })
	}
	require.Equal(t, supernode, r.d.state.IsSupernode())
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(gomock.NewController(t))
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), block.root).Return(uint32(0), nil).AnyTimes()
	r.d.blobStorage = blobStorage
	r.d.recoverBlobsQueue = make(chan recoverBlobsRequest, 1)
	return r
}

func TestRunDownloadSchedulesRecoveryOfAPublishedRoot(t *testing.T) {
	const currentSlot = 40
	for _, test := range []struct {
		name      string
		slot      uint64
		supernode bool
		backfill  bool
		scheduled bool
	}{
		{name: "supernode", slot: currentSlot, supernode: true, scheduled: true},
		{name: "custody node", slot: currentSlot},
		{name: "supernode, old root", slot: currentSlot - 2, supernode: true},
		{name: "supernode, backfilled root", slot: currentSlot, supernode: true, backfill: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := publishTestConfig()
				block := fuluFetchedBlock(t, &cfg, test.slot)
				r := halfStoredRecoveryRound(t, &cfg, currentSlot, block, test.supernode)
				if test.backfill {
					r.d.blocksToCheckSync.add(block.root, &backfillColumnBlock{version: clparams.FuluVersion, slot: block.slot, root: block.root}, time.Time{})
				}

				r.run(t)

				r.requireStored(t, block, 0, 1)
				if !test.scheduled {
					require.Empty(t, r.d.recoverBlobsQueue)
					require.NotContains(t, r.d.isRecovering, block.root)
					return
				}
				require.Len(t, r.d.recoverBlobsQueue, 1)
				request := <-r.d.recoverBlobsQueue
				require.Equal(t, block.root, request.blockRoot)
				require.Equal(t, block.slot, request.slot)
			})
		})
	}
}
