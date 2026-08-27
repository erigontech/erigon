package rpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type blockResponseSentinel struct {
	sentinelproto.SentinelClient
	response   []byte
	bannedPeer string
}

type contextRecordingSentinel struct {
	sentinelproto.SentinelClient
	deadline    time.Time
	hasDeadline bool
}

type emptyColumnResponseSentinel struct {
	sentinelproto.SentinelClient
	calls            int
	maxResponseBytes []uint64
	response         []byte
	bannedPeer       string
}

func (s *emptyColumnResponseSentinel) BanPeer(_ context.Context, peer *sentinelproto.Peer, _ ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.bannedPeer = peer.Pid
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *emptyColumnResponseSentinel) SendPeerRequest(_ context.Context, req *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls++
	s.maxResponseBytes = append(s.maxResponseBytes, req.MaxResponseBytes)
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: req.Pid}}, nil
}

type rawSSZBytes []byte

func (r rawSSZBytes) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, r...), nil
}

func (r rawSSZBytes) EncodingSizeSSZ() int {
	return len(r)
}

func (s *contextRecordingSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.deadline, s.hasDeadline = ctx.Deadline()
	return nil, errors.New("request stopped")
}

func (s *contextRecordingSentinel) SendPeerRequest(ctx context.Context, _ *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.deadline, s.hasDeadline = ctx.Deadline()
	return nil, errors.New("request stopped")
}

func TestReqRespRequestsPreserveCallerDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()

	for _, withPeer := range []bool{false, true} {
		sentinel := &contextRecordingSentinel{}
		client := &BeaconRpcP2P{sentinel: sentinel}
		if withPeer {
			_, _, _ = client.sendRequestWithPeer(ctx, "topic", nil, "peer", 0)
		} else {
			_, _, _ = client.sendRequest(ctx, "topic", nil, 0)
		}
		require.True(t, sentinel.hasDeadline)
		require.Equal(t, deadline, sentinel.deadline)
	}
}

func TestReqRespRequestsBoundContextsWithoutDeadline(t *testing.T) {
	for _, withPeer := range []bool{false, true} {
		sentinel := &contextRecordingSentinel{}
		client := &BeaconRpcP2P{sentinel: sentinel}
		before := time.Now().Add(30 * time.Second)
		if withPeer {
			_, _, _ = client.sendRequestWithPeer(t.Context(), "topic", nil, "peer", 0)
		} else {
			_, _, _ = client.sendRequest(t.Context(), "topic", nil, 0)
		}
		after := time.Now().Add(30 * time.Second)
		require.True(t, sentinel.hasDeadline)
		require.False(t, sentinel.deadline.Before(before))
		require.False(t, sentinel.deadline.After(after))
	}
}

func TestColumnSidecarsRequestSnapshotReflectsPeerMaskAndPreservesWrapper(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	sentinel := &emptyColumnResponseSentinel{}
	client := &BeaconRpcP2P{
		sentinel:     sentinel,
		beaconConfig: &cfg,
		columnDataPeers: &columnDataPeers{
			beaconConfig: &cfg,
			peersQueue: []peerData{{
				pid:  "partial-peer",
				mask: map[uint64]bool{0: true},
			}},
		},
	}
	root := common.HexToHash("0x1234")
	request := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	identifier := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: root,
		Columns:   solid.NewUint64ListSSZ(int(cfg.NumberOfColumns)),
	}
	identifier.Columns.Append(0)
	identifier.Columns.Append(1)
	request.Append(identifier)

	sidecars, pid, snapshot, err := client.SendColumnSidecarsByRootIdentifierReqWithSnapshot(t.Context(), request)
	require.NoError(t, err)
	require.Empty(t, sidecars)
	require.Equal(t, "partial-peer", pid)
	require.Equal(t, 1, snapshot.Len())
	require.Equal(t, []uint64{communication.MaxWireResponseBytes(client.columnSidecarRawBytes(), 1)}, sentinel.maxResponseBytes)
	snapshot.Range(func(_ int, item *cltypes.DataColumnsByRootIdentifier, _ int) bool {
		require.Equal(t, root, common.Hash(item.BlockRoot))
		require.Equal(t, 1, item.Columns.Length())
		require.Equal(t, uint64(0), item.Columns.Get(0))
		return true
	})

	sidecars, pid, err = client.SendColumnSidecarsByRootIdentifierReq(t.Context(), request)
	require.NoError(t, err)
	require.Empty(t, sidecars)
	require.Equal(t, "partial-peer", pid)
	require.Equal(t, 2, sentinel.calls)
}

func TestColumnSidecarsRequestRejectsOverCardinalityBeforeSidecarDecode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	digest, err := clock.ComputeForkDigest(cfg.FuluForkEpoch)
	require.NoError(t, err)
	valid := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, valid, digest[:]...))
	require.NoError(t, response.WriteByte(0))
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, rawSSZBytes{0xff}, digest[:]...))

	sentinel := &emptyColumnResponseSentinel{response: response.Bytes()}
	client := &BeaconRpcP2P{
		sentinel:     sentinel,
		beaconConfig: &cfg,
		ethClock:     clock,
		columnDataPeers: &columnDataPeers{
			beaconConfig: &cfg,
			peersQueue: []peerData{{
				pid:  "cap-ignoring-peer",
				mask: map[uint64]bool{0: true},
			}},
		},
	}
	request := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	identifier := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.HexToHash("0x1234"),
		Columns:   solid.NewUint64ListSSZ(int(cfg.NumberOfColumns)),
	}
	identifier.Columns.Append(0)
	request.Append(identifier)

	sidecars, pid, snapshot, err := client.SendColumnSidecarsByRootIdentifierReqWithSnapshot(t.Context(), request)
	require.ErrorContains(t, err, "response count 2 exceeds requested column count 1")
	require.Nil(t, sidecars)
	require.Equal(t, "cap-ignoring-peer", pid)
	require.Equal(t, 1, snapshot.Len())
	require.Equal(t, "cap-ignoring-peer", sentinel.bannedPeer)
}

func TestColumnSidecarsRequestAcceptsExactGloasForkDigest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	digest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot = cfg.GloasForkEpoch * cfg.SlotsPerEpoch
	sidecar.BeaconBlockRoot = common.HexToHash("0x1234")
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, sidecar, digest[:]...))

	sentinel := &emptyColumnResponseSentinel{response: response.Bytes()}
	client := &BeaconRpcP2P{
		sentinel:     sentinel,
		beaconConfig: &cfg,
		ethClock:     clock,
		columnDataPeers: &columnDataPeers{
			beaconConfig: &cfg,
			peersQueue: []peerData{{
				pid:  "gloas-peer",
				mask: map[uint64]bool{0: true},
			}},
		},
	}
	request := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	identifier := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: sidecar.BeaconBlockRoot,
		Columns:   solid.NewUint64ListSSZ(int(cfg.NumberOfColumns)),
	}
	identifier.Columns.Append(0)
	request.Append(identifier)

	sidecars, pid, snapshot, err := client.SendColumnSidecarsByRootIdentifierReqWithSnapshot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, "gloas-peer", pid)
	require.Equal(t, 1, snapshot.Len())
	require.Len(t, sidecars, 1)
	require.Equal(t, clparams.GloasVersion, sidecars[0].Version())
	require.Equal(t, sidecar.Slot, sidecars[0].Slot)
	require.Empty(t, sentinel.bannedPeer)
}

func TestColumnSidecarsRequestRejectsUnknownForkDigest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	unknownDigest := common.Bytes4{0xde, 0xad, 0xbe, 0xef}
	_, err := clock.StateVersionByForkDigest(unknownDigest)
	require.Error(t, err)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, rawSSZBytes{0}, unknownDigest[:]...))

	sentinel := &emptyColumnResponseSentinel{response: response.Bytes()}
	client := &BeaconRpcP2P{
		sentinel:     sentinel,
		beaconConfig: &cfg,
		ethClock:     clock,
		columnDataPeers: &columnDataPeers{
			beaconConfig: &cfg,
			peersQueue: []peerData{{
				pid:  "unknown-digest-peer",
				mask: map[uint64]bool{0: true},
			}},
		},
	}
	request := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	identifier := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.HexToHash("0x1234"),
		Columns:   solid.NewUint64ListSSZ(int(cfg.NumberOfColumns)),
	}
	identifier.Columns.Append(0)
	request.Append(identifier)

	sidecars, pid, snapshot, err := client.SendColumnSidecarsByRootIdentifierReqWithSnapshot(t.Context(), request)
	require.ErrorContains(t, err, "unknown fork digest deadbeef")
	require.Nil(t, sidecars)
	require.Equal(t, "unknown-digest-peer", pid)
	require.Equal(t, 1, snapshot.Len())
}

func TestColumnSidecarsSparseMultiRootCapPreservesFilteredOrderAndWrapper(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	sentinel := &emptyColumnResponseSentinel{}
	client := &BeaconRpcP2P{
		sentinel:     sentinel,
		beaconConfig: &cfg,
		columnDataPeers: &columnDataPeers{
			beaconConfig: &cfg,
			peersQueue: []peerData{{
				pid:  "sparse-peer",
				mask: map[uint64]bool{1: true, 7: true},
			}},
		},
	}
	request := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](2)
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	for _, item := range []struct {
		root    common.Hash
		columns []uint64
	}{
		{root: rootB, columns: []uint64{9, 7}},
		{root: rootA, columns: []uint64{7, 1, 3}},
	} {
		identifier := &cltypes.DataColumnsByRootIdentifier{
			BlockRoot: item.root,
			Columns:   solid.NewUint64ListSSZ(int(cfg.NumberOfColumns)),
		}
		for _, column := range item.columns {
			identifier.Columns.Append(column)
		}
		request.Append(identifier)
	}

	_, _, snapshot, err := client.SendColumnSidecarsByRootIdentifierReqWithSnapshot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, 2, snapshot.Len())
	require.Equal(t, rootB, common.Hash(snapshot.Get(0).BlockRoot))
	require.Equal(t, rootA, common.Hash(snapshot.Get(1).BlockRoot))
	filtered := make(map[common.Hash][]uint64)
	snapshot.Range(func(_ int, item *cltypes.DataColumnsByRootIdentifier, _ int) bool {
		columns := make([]uint64, item.Columns.Length())
		item.Columns.Range(func(i int, column uint64, _ int) bool {
			columns[i] = column
			return true
		})
		filtered[item.BlockRoot] = columns
		return true
	})
	require.Equal(t, []uint64{7, 1}, filtered[rootA])
	require.Equal(t, []uint64{7}, filtered[rootB])
	wantCap := communication.MaxWireResponseBytes(client.columnSidecarRawBytes(), 3)
	require.Equal(t, []uint64{wantCap}, sentinel.maxResponseBytes)

	_, _, err = client.SendColumnSidecarsByRootIdentifierReq(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, []uint64{wantCap, wantCap}, sentinel.maxResponseBytes)
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

func TestExecutionPayloadEnvelopeRequestsRejectPreGloasResponseVersion(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	fuluDigest, err := clock.ComputeForkDigest(cfg.FuluForkEpoch)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&cfg),
	}
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, envelope, fuluDigest[:]...))

	for _, request := range []func(*BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error){
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 1, 1)
		},
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}})
		},
	} {
		client := &BeaconRpcP2P{
			ctx:          context.Background(),
			sentinel:     &blockResponseSentinel{response: response.Bytes()},
			beaconConfig: &cfg,
			ethClock:     clock,
		}
		envelopes, pid, err := request(client)
		require.ErrorContains(t, err, "unsupported execution payload envelope consensus version")
		require.Empty(t, envelopes)
		require.Equal(t, "malicious-peer", pid)
	}
}

func TestExecutionPayloadEnvelopeRequestsRejectConfiguredRequestLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, envelope, gloasDigest[:]...))

	for _, request := range []func(*BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error){
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 1, 1)
		},
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}})
		},
	} {
		client := &BeaconRpcP2P{ctx: context.Background(), sentinel: &blockResponseSentinel{response: response.Bytes()}, beaconConfig: &cfg, ethClock: clock}
		envelopes, pid, err := request(client)
		require.ErrorContains(t, err, "builder deposits")
		require.Empty(t, envelopes)
		require.Equal(t, "malicious-peer", pid)
	}
}

func TestExecutionPayloadEnvelopeRequestsRejectOversizedDecompressedChunk(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)

	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, rawSSZ(make([]byte, clparams.MaxChunkSize+1)), gloasDigest[:]...))

	for _, request := range []func(*BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error){
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 1, 1)
		},
		func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
			return client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}})
		},
	} {
		client := &BeaconRpcP2P{
			ctx:          context.Background(),
			sentinel:     &blockResponseSentinel{response: response.Bytes()},
			beaconConfig: &cfg,
			ethClock:     clock,
		}
		envelopes, pid, err := request(client)
		require.ErrorContains(t, err, "exceeds max chunk size")
		require.Empty(t, envelopes)
		require.Equal(t, "malicious-peer", pid)
	}
}

type rawSSZ []byte

func (r rawSSZ) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, r...), nil
}

func (r rawSSZ) EncodingSizeSSZ() int {
	return len(r)
}

func TestSendExecutionPayloadEnvelopesByRangeReqReturnsValidatedPrefixOnError(t *testing.T) {
	testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnError(t, func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
		return client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 1, 2)
	})
}

func TestSendExecutionPayloadEnvelopesByRootReqReturnsValidatedPrefixOnError(t *testing.T) {
	testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnError(t, func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
		return client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}, {2}})
	})
}

func TestSendExecutionPayloadEnvelopesByRangeReqReturnsValidatedPrefixOnFramingError(t *testing.T) {
	testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnFramingError(t, func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
		return client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 1, 2)
	})
}

func TestSendExecutionPayloadEnvelopesByRootReqReturnsValidatedPrefixOnFramingError(t *testing.T) {
	testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnFramingError(t, func(client *BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
		return client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}, {2}})
	})
}

func testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnFramingError(
	t *testing.T,
	request func(*BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error),
) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)

	valid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	valid.Message.BeaconBlockRoot = common.HexToHash("0x01")
	for _, test := range []struct {
		name string
		tail []byte
	}{
		{"dangling response code", []byte{0}},
		{"truncated fork digest", []byte{0, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var response bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&response, valid, gloasDigest[:]...))
			_, err := response.Write(test.tail)
			require.NoError(t, err)

			client := &BeaconRpcP2P{
				ctx:          context.Background(),
				sentinel:     &blockResponseSentinel{response: response.Bytes()},
				beaconConfig: &cfg,
				ethClock:     clock,
			}
			envelopes, pid, err := request(client)
			require.Error(t, err)
			require.Equal(t, "malicious-peer", pid)
			require.Len(t, envelopes, 1)
			require.Equal(t, valid.Message.BeaconBlockRoot, envelopes[0].Message.BeaconBlockRoot)
		})
	}
}

func testExecutionPayloadEnvelopeRequestReturnsValidatedPrefixOnError(
	t *testing.T,
	request func(*BeaconRpcP2P) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error),
) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	fuluDigest, err := clock.ComputeForkDigest(cfg.FuluForkEpoch)
	require.NoError(t, err)

	valid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	valid.Message.BeaconBlockRoot = common.HexToHash("0x01")
	unsupported := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	configInvalid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	configInvalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	configInvalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})

	tests := []struct {
		name    string
		invalid ssz.Marshaler
		digest  [4]byte
	}{
		{name: "unsupported version", invalid: unsupported, digest: fuluDigest},
		{name: "malformed SSZ", invalid: rawSSZ{0}, digest: gloasDigest},
		{name: "config invalid", invalid: configInvalid, digest: gloasDigest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var response bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&response, valid, gloasDigest[:]...))
			require.NoError(t, response.WriteByte(0))
			require.NoError(t, ssz_snappy.EncodeAndWrite(&response, tt.invalid, tt.digest[:]...))

			client := &BeaconRpcP2P{
				ctx:          context.Background(),
				sentinel:     &blockResponseSentinel{response: response.Bytes()},
				beaconConfig: &cfg,
				ethClock:     clock,
			}
			envelopes, pid, err := request(client)
			require.Error(t, err)
			require.Equal(t, "malicious-peer", pid)
			require.Len(t, envelopes, 1)
			require.Equal(t, valid.Message.BeaconBlockRoot, envelopes[0].Message.BeaconBlockRoot)
		})
	}
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

func TestSendBeaconBlocksByRangeReqRejectsDanglingResponseCodeWithoutPartialResult(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	fuluDigest, err := clock.ComputeForkDigest(cfg.FuluForkEpoch)
	require.NoError(t, err)

	slot := cfg.FuluForkEpoch * cfg.SlotsPerEpoch
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = slot
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, block, fuluDigest[:]...))
	require.NoError(t, response.WriteByte(0))

	client := &BeaconRpcP2P{
		ctx:          context.Background(),
		sentinel:     &blockResponseSentinel{response: response.Bytes()},
		beaconConfig: &cfg,
		ethClock:     clock,
	}
	blocks, pid, err := client.SendBeaconBlocksByRangeReq(context.Background(), slot, 1)
	require.Error(t, err)
	require.Nil(t, blocks)
	require.Equal(t, "malicious-peer", pid)
}
