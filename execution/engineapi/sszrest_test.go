// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func allForksConfig() *chain.Config {
	return &chain.Config{
		ShanghaiTime:  common.NewUint64(100),
		CancunTime:    common.NewUint64(200),
		PragueTime:    common.NewUint64(300),
		OsakaTime:     common.NewUint64(400),
		AmsterdamTime: common.NewUint64(500),
	}
}

func newTestEngineServer(cfg *chain.Config, consuming bool) *EngineServer {
	return NewEngineServer(log.New(), cfg, nil, nil, false, true, false, consuming, nil, nil, 0, 0)
}

func newSSZRequest(method, path string, body []byte) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", sszRestContentType)
	return req
}

func problemType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	var problem struct {
		Type string `json:"type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem))
	return problem.Type
}

func TestSSZRESTForkRegistry(t *testing.T) {
	for name, want := range map[string]clparams.StateVersion{
		"paris":     clparams.BellatrixVersion,
		"shanghai":  clparams.CapellaVersion,
		"cancun":    clparams.DenebVersion,
		"prague":    clparams.ElectraVersion,
		"osaka":     clparams.FuluVersion,
		"amsterdam": clparams.GloasVersion,
	} {
		got, ok := engineForkVersion(name)
		require.True(t, ok, name)
		require.Equal(t, want, got, name)
	}
	_, ok := engineForkVersion("bellatrix")
	require.False(t, ok)
}

func TestSSZRESTForkNameAtTime(t *testing.T) {
	cfg := allForksConfig()
	for ts, want := range map[uint64]string{
		0:   "paris",
		99:  "paris",
		100: "shanghai",
		250: "cancun",
		350: "prague",
		450: "osaka",
		500: "amsterdam",
		999: "amsterdam",
	} {
		require.Equal(t, want, forkNameAtTime(cfg, ts), "ts=%d", ts)
	}
}

func TestSSZRESTSupportedForkNames(t *testing.T) {
	require.Equal(t, []string{"paris"}, supportedForkNames(&chain.Config{}))
	require.Equal(t,
		[]string{"paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"},
		supportedForkNames(allForksConfig()))
	require.Equal(t,
		[]string{"paris", "shanghai", "cancun"},
		supportedForkNames(&chain.Config{ShanghaiTime: common.NewUint64(0), CancunTime: common.NewUint64(0)}))
}

func TestSSZRESTNewPayloadEnvelopeRoundTrip(t *testing.T) {
	for _, version := range []clparams.StateVersion{
		clparams.BellatrixVersion,
		clparams.CapellaVersion,
		clparams.DenebVersion,
		clparams.ElectraVersion,
		clparams.FuluVersion,
		clparams.GloasVersion,
	} {
		t.Run(version.String(), func(t *testing.T) {
			payload := engine_types.NewExecutionPayloadSSZ(version)
			if version >= clparams.GloasVersion {
				slot := hexutil.Uint64(123)
				payload.SlotNumber = &slot
				bal := hexutil.Bytes{0x01, 0x02, 0x03}
				payload.BlockAccessList = &bal
			}
			root := common.HexToHash("0xaa")
			requests := []hexutil.Bytes{{0x00, 0x01}, {0x01, 0x02}}

			enc, err := encodeNewPayloadEnvelope(version, payload, root, requests)
			require.NoError(t, err)

			outPayload, outRoot, outRequests, err := decodeNewPayloadEnvelope(enc, version)
			require.NoError(t, err)

			payloadBytes, err := payload.EncodeSSZ(nil)
			require.NoError(t, err)
			switch {
			case version < clparams.DenebVersion:
				// {payload} — just an offset plus the payload bytes
				require.Len(t, enc, 4+len(payloadBytes))
				require.Equal(t, common.Hash{}, outRoot)
				require.Empty(t, outRequests)
			case version < clparams.ElectraVersion:
				// {payload, parent_beacon_block_root}
				require.Len(t, enc, 4+32+len(payloadBytes))
				require.Equal(t, root, outRoot)
				require.Empty(t, outRequests)
			default:
				// {payload, parent_beacon_block_root, execution_requests}
				require.Equal(t, root, outRoot)
				require.Equal(t, requests, outRequests)
			}
			if version >= clparams.GloasVersion {
				require.NotNil(t, outPayload.SlotNumber)
				require.Equal(t, hexutil.Uint64(123), *outPayload.SlotNumber)
				require.NotNil(t, outPayload.BlockAccessList)
				require.Equal(t, hexutil.Bytes{0x01, 0x02, 0x03}, *outPayload.BlockAccessList)
			}
			require.Equal(t, payload.BlockHash, outPayload.BlockHash)
		})
	}
}

func TestSSZRESTBlobVersionedHashesFromTxs(t *testing.T) {
	hashes := blobVersionedHashesFromTxs(nil)
	require.NotNil(t, hashes)
	require.Empty(t, hashes)

	// undecodable transactions yield an empty (non-nil) list and no panic;
	// newPayload re-decodes and reports INVALID itself
	hashes = blobVersionedHashesFromTxs([]hexutil.Bytes{{0xff, 0xfe}})
	require.NotNil(t, hashes)
	require.Empty(t, hashes)
}

func TestSSZRESTForkchoiceUpdateRoundTrip(t *testing.T) {
	state := engine_types.ForkChoiceState{
		HeadHash:           common.HexToHash("0x01"),
		SafeBlockHash:      common.HexToHash("0x02"),
		FinalizedBlockHash: common.HexToHash("0x03"),
	}

	t.Run("paris no attributes", func(t *testing.T) {
		enc, err := encodeForkchoiceUpdate(clparams.BellatrixVersion, &state, nil, nil)
		require.NoError(t, err)
		outState, outAttrs, outCustody, err := decodeForkchoiceUpdate(enc, clparams.BellatrixVersion)
		require.NoError(t, err)
		require.Equal(t, state, outState)
		require.Nil(t, outAttrs)
		require.Nil(t, outCustody)
	})

	t.Run("prague attributes use the cancun schema", func(t *testing.T) {
		root := common.HexToHash("0x04")
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             350,
			PrevRandao:            common.HexToHash("0x05"),
			SuggestedFeeRecipient: common.HexToAddress("0x06"),
			Withdrawals:           []*types.Withdrawal{{Index: 1, Validator: 2, Address: common.HexToAddress("0x07"), Amount: 3}},
			ParentBeaconBlockRoot: &root,
		}
		attrs.SSZVersion = clparams.ElectraVersion
		// timestamp(8) + prev_randao(32) + fee_recipient(20) + withdrawals offset(4) + root(32) + one withdrawal(44)
		attrBytes, err := attrs.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Len(t, attrBytes, 140)

		enc, err := encodeForkchoiceUpdate(clparams.ElectraVersion, &state, attrs, nil)
		require.NoError(t, err)
		_, outAttrs, outCustody, err := decodeForkchoiceUpdate(enc, clparams.ElectraVersion)
		require.NoError(t, err)
		require.Nil(t, outCustody)
		require.NotNil(t, outAttrs)
		require.NotNil(t, outAttrs.ParentBeaconBlockRoot)
		require.Equal(t, root, *outAttrs.ParentBeaconBlockRoot)
		require.Nil(t, outAttrs.SlotNumber)
		require.Nil(t, outAttrs.TargetGasLimit)
		require.Len(t, outAttrs.Withdrawals, 1)
	})

	t.Run("amsterdam attributes and custody columns", func(t *testing.T) {
		root := common.HexToHash("0x04")
		slot := hexutil.Uint64(456)
		targetGasLimit := hexutil.Uint64(36000000)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             500,
			SuggestedFeeRecipient: common.HexToAddress("0x06"),
			ParentBeaconBlockRoot: &root,
			SlotNumber:            &slot,
			TargetGasLimit:        &targetGasLimit,
		}
		attrs.SSZVersion = clparams.GloasVersion
		custody := bytes.Repeat([]byte{0xab}, 16)

		enc, err := encodeForkchoiceUpdate(clparams.GloasVersion, &state, attrs, custody)
		require.NoError(t, err)
		outState, outAttrs, outCustody, err := decodeForkchoiceUpdate(enc, clparams.GloasVersion)
		require.NoError(t, err)
		require.Equal(t, state, outState)
		require.NotNil(t, outAttrs)
		require.NotNil(t, outAttrs.SlotNumber)
		require.Equal(t, slot, *outAttrs.SlotNumber)
		require.NotNil(t, outAttrs.TargetGasLimit)
		require.Equal(t, targetGasLimit, *outAttrs.TargetGasLimit)
		require.Equal(t, custody, outCustody)
	})

	t.Run("amsterdam without custody columns", func(t *testing.T) {
		enc, err := encodeForkchoiceUpdate(clparams.GloasVersion, &state, nil, nil)
		require.NoError(t, err)
		_, outAttrs, outCustody, err := decodeForkchoiceUpdate(enc, clparams.GloasVersion)
		require.NoError(t, err)
		require.Nil(t, outAttrs)
		require.Nil(t, outCustody)
	})
}

// Pin the exact wire examples from execution-apis#793. Example A: a VALID
// PayloadStatus with latest_valid_hash present and no validation_error is 41 bytes.
func TestSSZRESTPayloadStatusSpecExample(t *testing.T) {
	latest := common.HexToHash("0xcd")
	status := &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &latest}
	enc, err := status.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Len(t, enc, 41)
	require.Equal(t, byte(0), enc[0])
	require.Equal(t, uint32(9), binary.LittleEndian.Uint32(enc[1:5]))
	require.Equal(t, uint32(41), binary.LittleEndian.Uint32(enc[5:9]))
	require.Equal(t, latest[:], enc[9:41])

	// Example B: INVALID with validation_error "bad state root" and no
	// latest_valid_hash is 27 bytes; note the inner 4-byte offset before the text.
	invalid := &engine_types.PayloadStatus{
		Status:          engine_types.InvalidStatus,
		ValidationError: engine_types.NewStringifiedErrorFromString("bad state root"),
	}
	encB, err := invalid.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Len(t, encB, 27)
	require.Equal(t, byte(1), encB[0])
	require.Equal(t, uint32(9), binary.LittleEndian.Uint32(encB[1:5]))  // offset[latest_valid_hash]
	require.Equal(t, uint32(9), binary.LittleEndian.Uint32(encB[5:9]))  // offset[validation_error]
	require.Equal(t, uint32(4), binary.LittleEndian.Uint32(encB[9:13])) // inner String offset
	require.Equal(t, []byte("bad state root"), encB[13:27])
}

func TestSSZRESTPayloadStatusEnumRoundTrip(t *testing.T) {
	latest := common.HexToHash("0x1234")
	wire := &engine_types.PayloadStatus{
		Status:          engine_types.AcceptedStatus,
		LatestValidHash: &latest,
		ValidationError: engine_types.NewStringifiedErrorFromString("no"),
	}
	enc, err := wire.EncodeSSZ(nil)
	require.NoError(t, err)
	// validation_error is Optional[String] = List[List[byte, 1024], 1]:
	// present means a 4-byte inner offset precedes the message bytes
	require.Equal(t, append([]byte{4, 0, 0, 0}, []byte("no")...), enc[41:])

	var out engine_types.PayloadStatus
	require.NoError(t, out.DecodeSSZ(enc, 0))
	require.Equal(t, engine_types.AcceptedStatus, out.Status)
	require.NotNil(t, out.LatestValidHash)
	require.Equal(t, latest, *out.LatestValidHash)
	require.Equal(t, "no", out.ValidationError.Error().Error())

	// absent validation_error stays distinguishable from an empty message
	noErr := &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}
	enc, err = noErr.EncodeSSZ(nil)
	require.NoError(t, err)
	var out2 engine_types.PayloadStatus
	require.NoError(t, out2.DecodeSSZ(enc, 0))
	require.Equal(t, engine_types.SyncingStatus, out2.Status)
	require.Nil(t, out2.LatestValidHash)
	require.Nil(t, out2.ValidationError)
}

func TestSSZRESTForkchoiceResponseRoundTrip(t *testing.T) {
	latest := common.HexToHash("0x11")
	payloadID := hexutil.Bytes{1, 2, 3, 4, 5, 6, 7, 8}
	resp := &engine_types.ForkChoiceUpdatedResponse{
		PayloadStatus: &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &latest},
		PayloadId:     &payloadID,
	}
	enc, err := encodeForkchoiceResponse(resp)
	require.NoError(t, err)
	out, err := decodeForkchoiceResponse(enc)
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, out.PayloadStatus.Status)
	require.Equal(t, latest, *out.PayloadStatus.LatestValidHash)
	require.Equal(t, payloadID, *out.PayloadId)

	resp.PayloadId = nil
	resp.PayloadStatus = &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}
	enc, err = encodeForkchoiceResponse(resp)
	require.NoError(t, err)
	out, err = decodeForkchoiceResponse(enc)
	require.NoError(t, err)
	require.Equal(t, engine_types.SyncingStatus, out.PayloadStatus.Status)
	require.Nil(t, out.PayloadId)
}

func TestSSZRESTBuiltPayloadRoundTrip(t *testing.T) {
	blockValue := uint256.NewInt(112233).ToBig()
	bundle := &engine_types.BlobsBundle{
		Commitments: []hexutil.Bytes{bytes.Repeat([]byte{0x01}, sszKZGBytes)},
		Proofs:      []hexutil.Bytes{bytes.Repeat([]byte{0x02}, sszKZGBytes)},
		Blobs:       []hexutil.Bytes{bytes.Repeat([]byte{0x03}, sszBlobBytes)},
	}
	requests := []hexutil.Bytes{{0x00, 0xaa}, {0x01, 0xbb}}

	for _, tc := range []struct {
		version      clparams.StateVersion
		wantSOB      bool
		wantBundle   bool
		wantRequests bool
	}{
		{clparams.BellatrixVersion, false, false, false},
		{clparams.CapellaVersion, false, false, false},
		{clparams.DenebVersion, true, true, false},
		{clparams.ElectraVersion, true, true, true},
		{clparams.FuluVersion, true, true, true},
		{clparams.GloasVersion, true, true, true},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			payload := engine_types.NewExecutionPayloadSSZ(tc.version)
			if tc.version >= clparams.GloasVersion {
				slot := hexutil.Uint64(7)
				payload.SlotNumber = &slot
				bal := hexutil.Bytes{0x0a}
				payload.BlockAccessList = &bal
			}
			resp := &engine_types.GetPayloadResponse{
				ExecutionPayload:      payload,
				BlockValue:            (*hexutil.Big)(blockValue),
				ShouldOverrideBuilder: true,
			}
			if tc.wantBundle {
				resp.BlobsBundle = bundle
			}
			if tc.wantRequests {
				resp.ExecutionRequests = requests
			}
			enc, err := encodeBuiltPayload(resp, tc.version)
			require.NoError(t, err)
			out, err := decodeBuiltPayload(enc, tc.version)
			require.NoError(t, err)
			require.Equal(t, blockValue, out.BlockValue.ToInt())
			require.Equal(t, payload.BlockHash, out.ExecutionPayload.BlockHash)
			require.Equal(t, tc.wantSOB, out.ShouldOverrideBuilder)
			if tc.wantBundle {
				require.NotNil(t, out.BlobsBundle)
				require.Equal(t, bundle.Commitments, out.BlobsBundle.Commitments)
			}
			if tc.wantRequests {
				require.Equal(t, requests, out.ExecutionRequests)
			}
			if tc.version >= clparams.GloasVersion {
				require.NotNil(t, out.ExecutionPayload.BlockAccessList)
				require.Equal(t, hexutil.Bytes{0x0a}, *out.ExecutionPayload.BlockAccessList)
			}
		})
	}
}

func TestSSZRESTBodiesResponseRoundTrip(t *testing.T) {
	withdrawal := &types.Withdrawal{Index: 1, Validator: 2, Address: common.HexToAddress("0x03"), Amount: 4}
	body := &engine_types.ExecutionPayloadBodyV2{
		Transactions:    []hexutil.Bytes{{0x01, 0x02}},
		Withdrawals:     []*types.Withdrawal{withdrawal},
		BlockAccessList: hexutil.Bytes{0xba, 0x11},
	}

	for _, tc := range []struct {
		version         clparams.StateVersion
		wantWithdrawals bool
		wantBAL         bool
	}{
		{clparams.BellatrixVersion, false, false},
		{clparams.DenebVersion, true, false},
		{clparams.FuluVersion, true, false},
		{clparams.GloasVersion, true, true},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			enc, err := encodeBodiesResponse([]*engine_types.ExecutionPayloadBodyV2{body, nil}, tc.version)
			require.NoError(t, err)
			// BodiesResponse is a single-field container: the body opens with a
			// 4-byte offset (=4) to the wrapped entries list.
			require.Equal(t, uint32(4), binary.LittleEndian.Uint32(enc[:4]))
			entries, err := decodeBodiesResponse(enc, tc.version)
			require.NoError(t, err)
			require.Len(t, entries, 2)

			require.True(t, entries[0].Available)
			require.Equal(t, body.Transactions, entries[0].Transactions)
			if tc.wantWithdrawals {
				require.Len(t, entries[0].Withdrawals, 1)
				require.Equal(t, *withdrawal, *entries[0].Withdrawals[0])
			} else {
				require.Empty(t, entries[0].Withdrawals)
			}
			if tc.wantBAL {
				require.Equal(t, body.BlockAccessList, entries[0].BlockAccessList)
			} else {
				require.Empty(t, entries[0].BlockAccessList)
			}

			require.False(t, entries[1].Available)
			require.Empty(t, entries[1].Transactions)
		})
	}
}

func TestSSZRESTBlobsResponseRoundTrip(t *testing.T) {
	blob := bytes.Repeat([]byte{0x11}, sszBlobBytes)
	proof := bytes.Repeat([]byte{0x22}, sszKZGBytes)
	proofs := make([]hexutil.Bytes, sszCellsPerExtBlob)
	for i := range proofs {
		proofs[i] = proof
	}

	t.Run("v1", func(t *testing.T) {
		enc, err := encodeBlobsV1Response([]*engine_types.BlobAndProofV1{{Blob: blob, Proof: proof}, nil})
		require.NoError(t, err)
		// BlobsV1Response is a single-field container wrapping the entries list.
		require.Equal(t, uint32(4), binary.LittleEndian.Uint32(enc[:4]))
		out, err := decodeBlobsV1Response(enc)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.NotNil(t, out[0])
		require.Equal(t, hexutil.Bytes(blob), out[0].Blob)
		require.Equal(t, hexutil.Bytes(proof), out[0].Proof)
		require.Nil(t, out[1])
	})

	t.Run("v2", func(t *testing.T) {
		enc, err := encodeBlobsV2Response([]*engine_types.BlobAndProofV2{{Blob: blob, CellProofs: proofs}, nil})
		require.NoError(t, err)
		// BlobsV2Response (shared by /v3) is a single-field container.
		require.Equal(t, uint32(4), binary.LittleEndian.Uint32(enc[:4]))
		out, err := decodeBlobsV2Response(enc)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.NotNil(t, out[0])
		require.Equal(t, hexutil.Bytes(blob), out[0].Blob)
		require.Len(t, out[0].CellProofs, sszCellsPerExtBlob)
		require.Nil(t, out[1])
	})
}

func TestSSZRESTHashListRequestRoundTrip(t *testing.T) {
	hashes := []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")}
	for _, limit := range []int{sszMaxBodiesRequest, sszMaxGetBlobHashes} {
		enc, err := encodeHashListRequest(hashes, limit)
		require.NoError(t, err)
		// single-field container: a 4-byte offset (=4) precedes the hash list body
		require.Equal(t, uint32(4), binary.LittleEndian.Uint32(enc[:4]))
		require.Len(t, enc, 4+len(hashes)*32)
		out, err := decodeHashListRequest(enc, limit)
		require.NoError(t, err)
		require.Equal(t, hashes, out)
	}

	// an empty request is just the 4-byte offset, no hashes
	enc, err := encodeHashListRequest(nil, sszMaxBodiesRequest)
	require.NoError(t, err)
	require.Len(t, enc, 4)
	out, err := decodeHashListRequest(enc, sszMaxBodiesRequest)
	require.NoError(t, err)
	require.Empty(t, out)

	// decode enforces the limit itself, independent of any caller-side size guard
	oversized, err := encodeHashListRequest(make([]common.Hash, sszMaxBodiesRequest+1), sszMaxBodiesRequest+1)
	require.NoError(t, err)
	_, err = decodeHashListRequest(oversized, sszMaxBodiesRequest)
	require.Error(t, err)
}

// A reused PayloadStatus must not leak optional fields from a previous decode.
func TestSSZRESTPayloadStatusDecodeResetsOptionals(t *testing.T) {
	latest := common.HexToHash("0xab")
	full := &engine_types.PayloadStatus{
		Status:          engine_types.InvalidStatus,
		LatestValidHash: &latest,
		ValidationError: engine_types.NewStringifiedErrorFromString("bad"),
	}
	encFull, err := full.EncodeSSZ(nil)
	require.NoError(t, err)
	bare := &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}
	encBare, err := bare.EncodeSSZ(nil)
	require.NoError(t, err)

	var reused engine_types.PayloadStatus
	require.NoError(t, reused.DecodeSSZ(encFull, 0))
	require.NotNil(t, reused.LatestValidHash)
	require.NotNil(t, reused.ValidationError)

	require.NoError(t, reused.DecodeSSZ(encBare, 0))
	require.Equal(t, engine_types.SyncingStatus, reused.Status)
	require.Nil(t, reused.LatestValidHash)
	require.Nil(t, reused.ValidationError)
}

func TestSSZRESTCapabilitiesRoute(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), false)
	req := httptest.NewRequest(http.MethodGet, "/engine/v2/capabilities", nil)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var caps struct {
		SupportedForks         []string            `json:"supported_forks"`
		ForkScopedEndpoints    []string            `json:"fork_scoped_endpoints"`
		IndependentlyVersioned map[string][]string `json:"independently_versioned"`
		UnscopedEndpoints      []string            `json:"unscoped_endpoints"`
		Limits                 map[string]uint64   `json:"limits"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &caps))
	require.Equal(t, []string{"paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"}, caps.SupportedForks)
	require.Equal(t, []string{"payloads", "forkchoice", "bodies"}, caps.ForkScopedEndpoints)
	require.Equal(t, []string{"v1", "v2", "v3"}, caps.IndependentlyVersioned["blobs"])
	require.Equal(t, []string{"capabilities", "identity"}, caps.UnscopedEndpoints)
	// pin the spec's MAX_* values: MAX_BODIES_REQUEST, MAX_VERSIONED_HASHES_PER_REQUEST,
	// MAX_REQUEST_BODY_SIZE (2**26 = 64 MiB).
	require.Equal(t, uint64(32), caps.Limits["bodies.max_count"])
	require.Equal(t, uint64(128), caps.Limits["blobs.max_versioned_hashes"])
	require.Equal(t, uint64(67108864), caps.Limits["payload.max_bytes"])
}

func TestSSZRESTIdentityRoute(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{}, false)
	req := httptest.NewRequest(http.MethodGet, "/engine/v2/identity", nil)
	req.Header.Set("X-Engine-Client-Version", "LH/v9.9.9")
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var versions []engine_types.ClientVersionV1
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &versions))
	require.Len(t, versions, 1)
	require.Equal(t, "EG", versions[0].Code)
}

func TestSSZRESTRoutingErrors(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), false)
	for _, tc := range []struct {
		name        string
		method      string
		path        string
		contentType string
		code        int
		problem     string
	}{
		{"legacy 764 payloads endpoint", http.MethodPost, "/engine/v1/payloads", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"legacy 764 forkchoice endpoint", http.MethodPost, "/engine/v1/forkchoice", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"legacy 764 capabilities endpoint", http.MethodPost, "/engine/v1/capabilities", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"unknown fork", http.MethodPost, "/engine/v2/foobar/payloads", sszRestContentType, http.StatusBadRequest, problemUnsupportedFork},
		{"trailing slash", http.MethodPost, "/engine/v2/cancun/payloads/", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"method mismatch on capabilities", http.MethodPost, "/engine/v2/capabilities", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"wrong content type", http.MethodPost, "/engine/v2/cancun/payloads", "application/json", http.StatusUnsupportedMediaType, problemUnsupportedMediaType},
		{"bad ssz body", http.MethodPost, "/engine/v2/cancun/payloads", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bad payload id", http.MethodGet, "/engine/v2/cancun/payloads/not-an-id", "", http.StatusBadRequest, problemInvalidRequest},
		{"bodies range missing params", http.MethodGet, "/engine/v2/cancun/bodies", "", http.StatusBadRequest, problemInvalidRequest},
		{"bodies range zero count", http.MethodGet, "/engine/v2/cancun/bodies?from=1&count=0", "", http.StatusUnprocessableEntity, problemInvalidBody},
		{"bodies range too large", http.MethodGet, "/engine/v2/cancun/bodies?from=1&count=33", "", http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{"bodies hash too many", http.MethodPost, "/engine/v2/cancun/bodies/hash", sszRestContentType, http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{"blobs unknown revision", http.MethodPost, "/engine/v2/blobs/v9", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"blobs too many hashes", http.MethodPost, "/engine/v2/blobs/v1", sszRestContentType, http.StatusRequestEntityTooLarge, problemRequestTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			switch tc.name {
			case "bad ssz body":
				body = []byte("bad-ssz")
			case "bodies hash too many":
				body = bytes.Repeat([]byte{0x01}, (sszMaxBodiesRequest+1)*32)
			case "blobs too many hashes":
				body = bytes.Repeat([]byte{0x01}, (sszMaxGetBlobHashes+1)*32)
			}
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, req)
			require.Equal(t, tc.code, rec.Code)
			require.Equal(t, tc.problem, problemType(t, rec))
		})
	}
}

// A fork that is valid but not scheduled on this chain must be rejected
// with 400 unsupported-fork.
func TestSSZRESTUnscheduledFork(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{ShanghaiTime: common.NewUint64(0), CancunTime: common.NewUint64(0)}, false)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(http.MethodPost, "/engine/v2/amsterdam/payloads", nil))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, problemUnsupportedFork, problemType(t, rec))
}

// An empty (but well-formed) Paris payload travels through decode and dispatch
// and comes back as an SSZ INVALID status ("invalid block hash") over HTTP 200.
func TestSSZRESTNewPayloadDispatch(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), true)
	payload := engine_types.NewExecutionPayloadSSZ(clparams.BellatrixVersion)
	body, err := encodeNewPayloadEnvelope(clparams.BellatrixVersion, payload, common.Hash{}, nil)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(http.MethodPost, "/engine/v2/paris/payloads", body))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
	var status engine_types.PayloadStatus
	require.NoError(t, status.DecodeSSZ(rec.Body.Bytes(), 0))
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.Contains(t, status.ValidationError.Error().Error(), "invalid block hash")
}

// A forkchoice with payload attributes whose timestamp belongs to a different
// fork than the URL must be rejected with 400 unsupported-fork.
func TestSSZRESTForkchoiceURLForkMustMatchAttributes(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), true)
	root := common.HexToHash("0x04")
	attrs := &engine_types.PayloadAttributes{
		Timestamp:             150, // shanghai era
		SuggestedFeeRecipient: common.HexToAddress("0x06"),
		ParentBeaconBlockRoot: &root,
	}
	attrs.SSZVersion = clparams.DenebVersion
	body, err := encodeForkchoiceUpdate(clparams.DenebVersion, &engine_types.ForkChoiceState{}, attrs, nil)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(http.MethodPost, "/engine/v2/cancun/forkchoice", body))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, problemUnsupportedFork, problemType(t, rec))
}

func TestSSZRESTBlobsPoolDisabled(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), true)
	body, err := encodeHashListRequest([]common.Hash{common.HexToHash("0x01")}, sszMaxGetBlobHashes)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(http.MethodPost, "/engine/v2/blobs/v1", body))
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Empty(t, rec.Body.Bytes())
}

func TestSSZRESTErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err     error
		code    int
		problem string
	}{
		{&engine_helpers.UnknownPayloadErr, http.StatusNotFound, problemUnknownPayload},
		{&engine_helpers.InvalidForkchoiceStateErr, http.StatusConflict, problemInvalidForkchoice},
		{&engine_helpers.InvalidPayloadAttributesErr, http.StatusUnprocessableEntity, problemInvalidAttributes},
		{&engine_helpers.TooLargeRequestErr, http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{&engine_helpers.ReorgTooDeepErr, http.StatusConflict, problemReorgTooDeep},
		{&rpc.UnsupportedForkError{Message: "no"}, http.StatusBadRequest, problemUnsupportedFork},
		{&rpc.InvalidParamsError{Message: "no"}, http.StatusUnprocessableEntity, problemInvalidBody},
		{errors.New("boom"), http.StatusInternalServerError, problemInternal},
	} {
		rec := httptest.NewRecorder()
		writeEngineProblem(rec, tc.err)
		require.Equal(t, tc.code, rec.Code)
		require.Equal(t, tc.problem, problemType(t, rec))
	}
}

func TestSSZRESTGetPayloadIDPathParsing(t *testing.T) {
	id, err := parsePayloadIDPath("0x0102030405060708")
	require.NoError(t, err)
	require.Equal(t, hexutil.Bytes{1, 2, 3, 4, 5, 6, 7, 8}, id)

	_, err = parsePayloadIDPath("0x01")
	require.Error(t, err)
}

func TestExchangeCapabilitiesDropsLegacySSZEndpoints(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{}, false)
	caps := srv.ExchangeCapabilities([]string{"engine_newPayloadV1"})
	require.Contains(t, caps, "engine_newPayloadV1")
	require.Contains(t, caps, "engine_getPayloadV6")
	require.NotContains(t, caps, "engine_exchangeCapabilities")
	for _, capability := range caps {
		require.NotContains(t, capability, "/engine/v")
	}
}
