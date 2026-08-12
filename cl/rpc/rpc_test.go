package rpc

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type blockResponseSentinel struct {
	sentinelproto.SentinelClient
	response   []byte
	bannedPeer string
}

type blockingBlobSentinel struct {
	sentinelproto.SentinelClient
	active atomic.Int64
	max    atomic.Int64
	enter  chan struct{}
}

func (s *blockingBlobSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	active := s.active.Add(1)
	for current := s.max.Load(); active > current && !s.max.CompareAndSwap(current, active); current = s.max.Load() {
	}
	s.enter <- struct{}{}
	<-ctx.Done()
	s.active.Add(-1)
	return nil, ctx.Err()
}

func (s *blockResponseSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	return &sentinelproto.ResponseData{
		Data: s.response,
		Peer: &sentinelproto.Peer{Pid: "malicious-peer"},
	}, nil
}

func (s *blockResponseSentinel) BanPeer(_ context.Context, peer *sentinelproto.Peer, _ ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.bannedPeer = peer.Pid
	return &sentinelproto.EmptyMessage{}, nil
}

func TestExecutionPayloadEnvelopeRequestsRejectOverLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxRequestPayloads = 1
	rpc := &BeaconRpcP2P{beaconConfig: &cfg}

	_, _, err := rpc.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 10, 2)
	require.ErrorContains(t, err, "MAX_REQUEST_PAYLOADS")

	_, _, err = rpc.SendExecutionPayloadEnvelopesByRootReq(context.Background(), make([][32]byte, 2))
	require.ErrorContains(t, err, "MAX_REQUEST_PAYLOADS")
}

func TestMaxRequestPayloadsFallback(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxRequestPayloads = 0
	cfg.MaxRequestBlocksDeneb = 17
	rpc := &BeaconRpcP2P{beaconConfig: &cfg}

	require.Equal(t, uint64(17), rpc.MaxRequestPayloads())

	_, _, err := rpc.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 10, 18)
	require.ErrorContains(t, err, "17")

	_, _, err = rpc.SendExecutionPayloadEnvelopesByRootReq(context.Background(), make([][32]byte, 18))
	require.ErrorContains(t, err, "17")
}

func TestBlobSidecarByRootRequestsShareConcurrencyLimit(t *testing.T) {
	sentinel := &blockingBlobSentinel{enter: make(chan struct{}, 3)}
	client := &BeaconRpcP2P{ctx: t.Context(), sentinel: sentinel, beaconConfig: &clparams.MainnetBeaconConfig}
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](1, 40)
	req.Append(&cltypes.BlobIdentifier{})
	contexts := make([]context.Context, 3)
	cancels := make([]context.CancelFunc, 3)
	contexts[0], cancels[0] = context.WithCancel(t.Context())
	contexts[1], cancels[1] = context.WithCancel(t.Context())
	contexts[2], cancels[2] = context.WithCancel(t.Context())
	defer cancels[0]()
	defer cancels[1]()
	defer cancels[2]()
	done := make(chan struct{}, 3)
	launch := func(index int) {
		go func() {
			_, _, _ = client.SendBlobsSidecarByIdentifierReq(contexts[index], req)
			done <- struct{}{}
		}()
	}
	launch(0)
	<-sentinel.enter
	launch(1)
	<-sentinel.enter
	launch(2)
	select {
	case <-sentinel.enter:
		t.Fatal("third blob request crossed the shared concurrency boundary")
	case <-time.After(100 * time.Millisecond):
	}
	cancels[0]()
	select {
	case <-sentinel.enter:
	case <-time.After(time.Second):
		t.Fatal("waiting blob request did not acquire a released permit")
	}
	require.Equal(t, int64(2), sentinel.max.Load())
	cancels[1]()
	cancels[2]()
	for range 3 {
		<-done
	}
}

func TestSendBeaconBlocksByRangeReqRejectsForkSchemaSlotMismatch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)

	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)

	slot := (cfg.FuluForkEpoch + 1) * cfg.SlotsPerEpoch
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = slot

	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, block, gloasDigest[:]...))

	sentinel := &blockResponseSentinel{response: response.Bytes()}
	client := &BeaconRpcP2P{
		ctx:          context.Background(),
		sentinel:     sentinel,
		beaconConfig: &cfg,
		ethClock:     clock,
	}

	blocks, pid, err := client.SendBeaconBlocksByRangeReq(context.Background(), slot, 1)
	require.ErrorIs(t, err, errBlockForkSchemaSlotMismatch)
	require.Nil(t, blocks)
	require.Equal(t, "malicious-peer", pid)
	require.Equal(t, "malicious-peer", sentinel.bannedPeer)
}
