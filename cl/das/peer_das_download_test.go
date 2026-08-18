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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type columnTestSentinel struct {
	sentinelproto.SentinelClient
	metadataResponse []byte
	columnResponse   []byte

	metadataOnce  sync.Once
	metadataReady chan struct{}
	banOnce       sync.Once
	banned        chan struct{}

	columnRequestsStarted chan<- struct{}
}

func (s *columnTestSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{Peers: []*sentinelproto.Peer{{Pid: "column-test-peer"}}}, nil
}

func (s *columnTestSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.banOnce.Do(func() { close(s.banned) })
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *columnTestSentinel) SendPeerRequest(ctx context.Context, req *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	peer := &sentinelproto.Peer{Pid: "column-test-peer"}
	if strings.Contains(req.Topic, "metadata") {
		s.metadataOnce.Do(func() { close(s.metadataReady) })
		return &sentinelproto.ResponseData{Data: s.metadataResponse, Peer: peer}, nil
	}
	if s.columnRequestsStarted != nil {
		select {
		case s.columnRequestsStarted <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &sentinelproto.ResponseData{Data: s.columnResponse, Peer: peer}, nil
}

func newColumnTestSetup(t *testing.T) (*clparams.BeaconChainConfig, uint64, eth_clock.EthereumClock, []byte) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)

	currentSlot := (cfg.FuluForkEpoch + 1) * cfg.SlotsPerEpoch
	genesisTime := uint64(time.Now().Unix()) - currentSlot*cfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, &cfg)

	custodyGroupCount := cfg.NumberOfColumns
	syncnets := [1]byte{}
	metadata := &cltypes.Metadata{Syncnets: &syncnets, CustodyGroupCount: &custodyGroupCount}
	var metadataWire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&metadataWire, metadata))
	return &cfg, currentSlot, clock, metadataWire.Bytes()
}

func newSingleColumnDownloadRequest(cfg *clparams.BeaconChainConfig, blockRoot common.Hash, slot uint64) *downloadRequest {
	return &downloadRequest{
		beaconConfig: cfg,
		downloadTable: map[downloadTableEntry]map[uint64]bool{
			{blockRoot: blockRoot, slot: slot}: {0: true},
		},
	}
}

func TestRunDownloadSharedConcurrencyLimit(t *testing.T) {
	cfg, currentSlot, clock, metadataResponse := newColumnTestSetup(t)

	columnRequestsStarted := make(chan struct{}, maxConcurrentColumnRequests+1)
	sentinel := &columnTestSentinel{
		metadataResponse:      metadataResponse,
		metadataReady:         make(chan struct{}),
		banned:                make(chan struct{}),
		columnRequestsStarted: columnRequestsStarted,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, cfg, clock, nil)

	select {
	case <-sentinel.metadataReady:
	case <-time.After(30 * time.Second):
		t.Fatal("test peer was not added to the custody-peer queue")
	}

	d := &peerdas{
		rpc:            rpcClient,
		beaconConfig:   cfg,
		state:          peerdasstate.NewPeerDasState(cfg, &clparams.NetworkConfig{}),
		columnStorage:  blob_storage.NewDataColumnStore(afero.NewMemMapFs(), 0, cfg, clock, beaconevents.NewEventEmitter()),
		columnRPCSlots: make(chan struct{}, maxConcurrentColumnRequests),
	}
	done := make(chan error, 2)
	for i := byte(1); i <= 2; i++ {
		req := newSingleColumnDownloadRequest(cfg, common.BytesToHash([]byte{i}), currentSlot)
		go func() { done <- d.runDownload(ctx, req, false) }()
	}

	requestTimeout := time.NewTimer(5 * time.Second)
	defer requestTimeout.Stop()
	for range maxConcurrentColumnRequests {
		select {
		case <-columnRequestsStarted:
		case <-requestTimeout.C:
			t.Fatal("column requests did not reach the concurrency limit")
		}
	}

	exceededLimit := false
	select {
	case <-columnRequestsStarted:
		exceededLimit = true
	case <-time.After(3 * columnRequestInterval):
	}
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.False(t, exceededLimit, "column RPC concurrency exceeded %d", maxConcurrentColumnRequests)
}

// The real RPC decoder derives the sidecar schema from the peer-selected fork
// digest. A sidecar whose slot does not match that schema must be rejected before
// schema-specific fields are used.
func TestRunDownloadRejectsGloasSidecarWithPreGloasSlot(t *testing.T) {
	// Keep Gloas at FAR_FUTURE_EPOCH so its digest resolves without any slot
	// using its schema.
	cfg, currentSlot, clock, metadataResponse := newColumnTestSetup(t)

	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	decodeVersion, err := clock.StateVersionByForkDigest(gloasDigest)
	require.NoError(t, err)
	require.Equal(t, clparams.GloasVersion, decodeVersion, "far-future Gloas digest must resolve")

	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot = 0
	sidecar.BeaconBlockRoot = common.HexToHash("0x1234")
	require.Nil(t, sidecar.SignedBlockHeader, "Gloas schema must leave SignedBlockHeader unset")
	var columnWire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&columnWire, sidecar, gloasDigest[:]...))

	sentinel := &columnTestSentinel{
		metadataResponse: metadataResponse,
		columnResponse:   columnWire.Bytes(),
		metadataReady:    make(chan struct{}),
		banned:           make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, cfg, clock, nil)

	select {
	case <-sentinel.metadataReady:
	case <-time.After(30 * time.Second):
		t.Fatal("malicious peer was not added to the custody-peer queue")
	}

	// Gloas sidecar processing requires a non-nil cache.
	gloasDataCache, err := lru.New[common.Hash, *gloasBlockData]("gloasDataCacheTest", 8)
	require.NoError(t, err)
	d := &peerdas{
		rpc:            rpcClient,
		beaconConfig:   cfg,
		state:          peerdasstate.NewPeerDasState(cfg, &clparams.NetworkConfig{}),
		columnStorage:  blob_storage.NewDataColumnStore(afero.NewMemMapFs(), 0, cfg, clock, beaconevents.NewEventEmitter()),
		columnRPCSlots: make(chan struct{}, maxConcurrentColumnRequests),
		gloasDataCache: gloasDataCache,
	}
	blockRoot := common.HexToHash("0xbeef")
	req := newSingleColumnDownloadRequest(cfg, blockRoot, currentSlot)

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

	// Rejected sidecars must not reach storage.
	saved, err := d.columnStorage.GetSavedColumnIndex(context.Background(), currentSlot, blockRoot)
	require.NoError(t, err)
	require.Empty(t, saved)
}
