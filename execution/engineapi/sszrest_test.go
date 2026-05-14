// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func TestSSZRESTCapabilitiesCodecRoundTrip(t *testing.T) {
	in := newSSZCapabilities([]string{"engine_newPayloadV1", "POST /engine/v1/payloads"})
	enc, err := in.EncodeSSZ(nil)
	require.NoError(t, err)

	var out sszCapabilities
	require.NoError(t, out.DecodeSSZ(enc, 0))
	require.Equal(t, []string{"engine_newPayloadV1", "POST /engine/v1/payloads"}, out.names())
}

func TestSSZRESTPayloadStatusEnumRoundTrip(t *testing.T) {
	latest := common.HexToHash("0x1234")
	wire, err := payloadStatusToSSZ(&engine_types.PayloadStatus{
		Status:          engine_types.AcceptedStatus,
		LatestValidHash: &latest,
		ValidationError: engine_types.NewStringifiedErrorFromString("no"),
	})
	require.NoError(t, err)
	enc, err := wire.EncodeSSZ(nil)
	require.NoError(t, err)

	var out sszPayloadStatus
	require.NoError(t, out.DecodeSSZ(enc, 0))
	require.Equal(t, uint8(3), out.Status)
	require.Equal(t, []common.Hash{latest}, out.LatestValidHash.hashes())
	require.Equal(t, []byte("no"), out.ValidationError.Bytes())
}

func TestSSZRESTRequestCodecsRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
		obj     interface {
			EncodeSSZ([]byte) ([]byte, error)
			DecodeSSZ([]byte, int) error
		}
		empty func() interface {
			DecodeSSZ([]byte, int) error
		}
	}{
		{"newPayloadV1", clparams.BellatrixVersion, newSSZNewPayloadRequest(clparams.BellatrixVersion), func() interface{ DecodeSSZ([]byte, int) error } {
			return newSSZNewPayloadRequest(clparams.BellatrixVersion)
		}},
		{"newPayloadV2", clparams.CapellaVersion, newSSZNewPayloadRequest(clparams.CapellaVersion), func() interface{ DecodeSSZ([]byte, int) error } {
			return newSSZNewPayloadRequest(clparams.CapellaVersion)
		}},
		{"newPayloadV3", clparams.DenebVersion, newSSZNewPayloadRequest(clparams.DenebVersion), func() interface{ DecodeSSZ([]byte, int) error } {
			return newSSZNewPayloadRequest(clparams.DenebVersion)
		}},
		{"newPayloadV5", clparams.GloasVersion, newSSZNewPayloadRequest(clparams.GloasVersion), func() interface{ DecodeSSZ([]byte, int) error } {
			return newSSZNewPayloadRequest(clparams.GloasVersion)
		}},
		{"forkchoiceV1", clparams.BellatrixVersion, &sszForkchoiceRequest{version: clparams.BellatrixVersion}, func() interface{ DecodeSSZ([]byte, int) error } {
			return &sszForkchoiceRequest{}
		}},
		{"forkchoiceV4", clparams.GloasVersion, &sszForkchoiceRequest{version: clparams.GloasVersion}, func() interface{ DecodeSSZ([]byte, int) error } {
			return &sszForkchoiceRequest{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.obj.EncodeSSZ(nil)
			require.NoError(t, err)
			require.NoError(t, tc.empty().DecodeSSZ(enc, int(tc.version)))
		})
	}
}

func TestSSZRESTGetBlobsCodecsRoundTrip(t *testing.T) {
	blob := bytes.Repeat([]byte{0x11}, sszBlobBytes)
	proof := bytes.Repeat([]byte{0x22}, sszKZGBytes)
	proofs := make([]hexutil.Bytes, sszCellsPerExtBlob)
	for i := range proofs {
		proofs[i] = proof
	}

	for _, tc := range []struct {
		name string
		obj  interface {
			EncodeSSZ([]byte) ([]byte, error)
			DecodeSSZ([]byte, int) error
		}
		empty func() interface {
			DecodeSSZ([]byte, int) error
		}
	}{
		{
			name: "v1",
			obj:  newSSZGetBlobsV1Response([]*engine_types.BlobAndProofV1{{Blob: blob, Proof: proof}, nil}),
			empty: func() interface{ DecodeSSZ([]byte, int) error } {
				return &sszGetBlobsV1Response{}
			},
		},
		{
			name: "v2",
			obj:  newSSZGetBlobsV2Response([]*engine_types.BlobAndProofV2{{Blob: blob, CellProofs: proofs}, nil}),
			empty: func() interface{ DecodeSSZ([]byte, int) error } {
				return &sszGetBlobsV2Response{}
			},
		},
		{
			name: "v3",
			obj:  newSSZGetBlobsV3Response([]*engine_types.BlobAndProofV2{{Blob: blob, CellProofs: proofs}, nil}),
			empty: func() interface{ DecodeSSZ([]byte, int) error } {
				return &sszGetBlobsV3Response{}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.obj.EncodeSSZ(nil)
			require.NoError(t, err)
			require.NoError(t, tc.empty().DecodeSSZ(enc, 0))
		})
	}
}

func TestSSZRESTCapabilitiesRoute(t *testing.T) {
	srv := NewEngineServer(log.New(), &chain.Config{}, nil, nil, false, true, false, false, nil, 0, 0)
	body, err := newSSZCapabilities([]string{"engine_newPayloadV1"}).EncodeSSZ(nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/engine/v1/capabilities", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
	var resp sszCapabilities
	require.NoError(t, resp.DecodeSSZ(rec.Body.Bytes(), 0))
	require.Contains(t, resp.names(), "engine_newPayloadV1")
	require.Contains(t, resp.names(), "POST /engine/v4/payloads")
	require.NotContains(t, resp.names(), "engine_exchangeCapabilities")
}

func TestSSZRESTAdvertisedRoutes(t *testing.T) {
	srv := NewEngineServer(log.New(), &chain.Config{}, nil, nil, false, true, false, false, nil, 0, 0)
	for _, route := range []struct {
		method string
		path   string
		code   int
	}{
		{http.MethodPost, "/engine/v1/payloads", http.StatusBadRequest},
		{http.MethodPost, "/engine/v2/payloads", http.StatusBadRequest},
		{http.MethodPost, "/engine/v3/payloads", http.StatusBadRequest},
		{http.MethodPost, "/engine/v4/payloads", http.StatusBadRequest},
		{http.MethodPost, "/engine/v5/payloads", http.StatusBadRequest},
		{http.MethodGet, "/engine/v1/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodGet, "/engine/v2/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodGet, "/engine/v3/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodGet, "/engine/v4/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodGet, "/engine/v5/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodGet, "/engine/v6/payloads/not-a-payload-id", http.StatusBadRequest},
		{http.MethodPost, "/engine/v1/forkchoice", http.StatusBadRequest},
		{http.MethodPost, "/engine/v2/forkchoice", http.StatusBadRequest},
		{http.MethodPost, "/engine/v3/forkchoice", http.StatusBadRequest},
		{http.MethodPost, "/engine/v4/forkchoice", http.StatusBadRequest},
		{http.MethodPost, "/engine/v1/blobs", http.StatusBadRequest},
		{http.MethodPost, "/engine/v2/blobs", http.StatusBadRequest},
		{http.MethodPost, "/engine/v3/blobs", http.StatusBadRequest},
		{http.MethodPost, "/engine/v1/client/version", http.StatusBadRequest},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			req := httptest.NewRequest(route.method, route.path, strings.NewReader("bad-ssz"))
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, req)
			require.Equal(t, route.code, rec.Code)
		})
	}
}

func TestSSZRESTEndpointVersionMapping(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(int) (clparams.StateVersion, bool)
		in   int
		want clparams.StateVersion
	}{
		{"newPayloadV1", sszNewPayloadVersion, 1, clparams.BellatrixVersion},
		{"newPayloadV2", sszNewPayloadVersion, 2, clparams.CapellaVersion},
		{"newPayloadV3", sszNewPayloadVersion, 3, clparams.DenebVersion},
		{"newPayloadV4", sszNewPayloadVersion, 4, clparams.ElectraVersion},
		{"newPayloadV5", sszNewPayloadVersion, 5, clparams.GloasVersion},
		{"getPayloadV1", sszGetPayloadVersion, 1, clparams.BellatrixVersion},
		{"getPayloadV2", sszGetPayloadVersion, 2, clparams.CapellaVersion},
		{"getPayloadV3", sszGetPayloadVersion, 3, clparams.DenebVersion},
		{"getPayloadV4", sszGetPayloadVersion, 4, clparams.ElectraVersion},
		{"getPayloadV5", sszGetPayloadVersion, 5, clparams.FuluVersion},
		{"getPayloadV6", sszGetPayloadVersion, 6, clparams.GloasVersion},
		{"forkchoiceV1", sszForkchoiceVersion, 1, clparams.BellatrixVersion},
		{"forkchoiceV2", sszForkchoiceVersion, 2, clparams.CapellaVersion},
		{"forkchoiceV3", sszForkchoiceVersion, 3, clparams.DenebVersion},
		{"forkchoiceV4", sszForkchoiceVersion, 4, clparams.GloasVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.fn(tc.in)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSSZRESTNewPayloadV5UsesGloasPayloadSchema(t *testing.T) {
	req := newSSZNewPayloadRequest(clparams.GloasVersion)
	req.Payload.Block.SlotNumber = 123
	req.Payload.Block.BlockAccessList = solid.NewByteListSSZ(sszMaxBytesPerTransaction)
	require.NoError(t, req.Payload.Block.BlockAccessList.SetBytes([]byte{0x01, 0x02, 0x03}))

	enc, err := req.EncodeSSZ(nil)
	require.NoError(t, err)

	wireVersion, ok := sszNewPayloadVersion(5)
	require.True(t, ok)
	out := newSSZNewPayloadRequest(wireVersion)
	require.NoError(t, out.DecodeSSZ(enc, int(wireVersion)))

	payload := sszToExecutionPayload(out.Payload)
	require.NotNil(t, payload.SlotNumber)
	require.Equal(t, hexutil.Uint64(123), *payload.SlotNumber)
	require.Equal(t, hexutil.Bytes{0x01, 0x02, 0x03}, payload.BlockAccessList)
}

func TestSSZRESTForkchoiceV4UsesGloasPayloadAttributesSchema(t *testing.T) {
	attrs := &sszPayloadAttributes{
		Timestamp:             1,
		SuggestedFeeRecipient: common.HexToAddress("0x1234"),
		Withdrawals:           &sszWithdrawalList{},
		SlotNumber:            456,
		version:               clparams.GloasVersion,
	}
	req := &sszForkchoiceRequest{
		version:   clparams.GloasVersion,
		AttrsList: solid.NewDynamicListSSZ[*sszPayloadAttributes](1),
	}
	req.AttrsList.Append(attrs)

	enc, err := req.EncodeSSZ(nil)
	require.NoError(t, err)

	wireVersion, ok := sszForkchoiceVersion(4)
	require.True(t, ok)
	var out sszForkchoiceRequest
	require.NoError(t, out.DecodeSSZ(enc, int(wireVersion)))
	out.version = wireVersion

	engineAttrs := out.payloadAttributes()
	require.NotNil(t, engineAttrs)
	require.NotNil(t, engineAttrs.SlotNumber)
	require.Equal(t, hexutil.Uint64(456), *engineAttrs.SlotNumber)
}

func TestExchangeCapabilitiesAdvertisesJSONRPCAndSSZREST(t *testing.T) {
	srv := NewEngineServer(log.New(), &chain.Config{}, nil, nil, false, true, false, false, nil, 0, 0)
	caps := srv.ExchangeCapabilities([]string{"engine_newPayloadV1"})
	require.Contains(t, caps, "engine_newPayloadV1")
	require.Contains(t, caps, "engine_getPayloadV6")
	require.Contains(t, caps, "POST /engine/v1/capabilities")
	require.Contains(t, caps, "GET /engine/v6/payloads/{payload_id}")
	require.NotContains(t, caps, "engine_exchangeCapabilities")
}

func TestSSZRESTGetPayloadIDPathParsing(t *testing.T) {
	id, err := parsePayloadIDPath("0x0102030405060708")
	require.NoError(t, err)
	require.Equal(t, hexutil.Bytes{1, 2, 3, 4, 5, 6, 7, 8}, id)

	_, err = parsePayloadIDPath("0x01")
	require.Error(t, err)
}

func TestSSZRESTErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{
		{&engine_helpers.UnknownPayloadErr, http.StatusNotFound},
		{&engine_helpers.InvalidForkchoiceStateErr, http.StatusConflict},
		{&engine_helpers.InvalidPayloadAttributesErr, http.StatusUnprocessableEntity},
		{&engine_helpers.TooLargeRequestErr, http.StatusRequestEntityTooLarge},
		{errors.New("boom"), http.StatusInternalServerError},
	} {
		rec := httptest.NewRecorder()
		writeEngineError(rec, tc.err)
		require.Equal(t, tc.code, rec.Code)
	}
}
