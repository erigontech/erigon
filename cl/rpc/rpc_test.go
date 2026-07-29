package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

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
