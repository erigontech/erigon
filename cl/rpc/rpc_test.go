package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type rawSSZ []byte

func (r rawSSZ) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, r...), nil }
func (r rawSSZ) EncodingSizeSSZ() int                 { return len(r) }

type blockResponseSentinel struct {
	sentinelproto.SentinelClient
	response   []byte
	bannedPeer string
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

func TestExecutionPayloadEnvelopeRequestsRejectNonCanonicalSSZ(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&cfg),
	}
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	const signedEnvelopeFixedSize = 4 + 96
	dynamicGap := make([]byte, 0, len(encoded)+1)
	dynamicGap = append(dynamicGap, encoded[:signedEnvelopeFixedSize]...)
	dynamicGap = append(dynamicGap, 0)
	dynamicGap = append(dynamicGap, encoded[signedEnvelopeFixedSize:]...)
	binary.LittleEndian.PutUint32(dynamicGap[:4], signedEnvelopeFixedSize+1)

	for _, test := range []struct {
		name string
		ssz  []byte
	}{
		{name: "dynamic offset gap", ssz: dynamicGap},
		{name: "trailing byte", ssz: append(append([]byte(nil), encoded...), 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var response bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&response, rawSSZ(test.ssz), gloasDigest[:]...))
			sentinel := &blockResponseSentinel{response: response.Bytes()}
			client := &BeaconRpcP2P{
				ctx:          context.Background(),
				sentinel:     sentinel,
				beaconConfig: &cfg,
				ethClock:     clock,
			}

			_, _, rangeErr := client.SendExecutionPayloadEnvelopesByRangeReq(context.Background(), 10, 1)
			require.Error(t, rangeErr)

			_, _, rootErr := client.SendExecutionPayloadEnvelopesByRootReq(context.Background(), [][32]byte{{1}})
			require.Error(t, rootErr)
		})
	}
}
