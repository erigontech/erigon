// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	commonssz "github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/txpool"
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

func newBodiesTestEngineServer(t *testing.T, module execmodule.ExecutionModule) *EngineServer {
	cfg := allForksConfig()
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), module, nil, nil, cfg, ethconfig.Sync{}, nil)
	return NewEngineServer(log.New(), cfg, module, downloader, false, true, false, true, nil, nil, 0, 0)
}

type bodiesExecutionModule struct {
	execmodule.ExecutionModule
	bodies        map[common.Hash]*execmodule.PayloadBody
	headers       map[uint64]*types.Header
	headersByHash map[common.Hash]*types.Header
	bodyRequests  [][]common.Hash
}

func (m *bodiesExecutionModule) GetPayloadBodiesByHash(_ context.Context, hashes []common.Hash) ([]*execmodule.PayloadBody, error) {
	m.bodyRequests = append(m.bodyRequests, slices.Clone(hashes))
	bodies := make([]*execmodule.PayloadBody, len(hashes))
	for i, hash := range hashes {
		bodies[i] = m.bodies[hash]
	}
	return bodies, nil
}

func (m *bodiesExecutionModule) GetHeader(_ context.Context, hash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	if hash != nil {
		return m.headersByHash[*hash], nil
	}
	if blockNumber == nil {
		return nil, nil
	}
	return m.headers[*blockNumber], nil
}

func (m *bodiesExecutionModule) GetHeadersByRange(_ context.Context, start, count uint64) ([]*types.Header, error) {
	var head uint64
	for number := range m.headers {
		head = max(head, number)
	}
	headers := make([]*types.Header, 0, count)
	for i := range count {
		number := start + i
		if number < start || number > head {
			break
		}
		headers = append(headers, m.headers[number])
	}
	return headers, nil
}

func newSSZRequest(ctx context.Context, method, path, fork string, body []byte) *http.Request {
	req := httptest.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", sszRestContentType)
	if fork != "" {
		req.Header.Set("Eth-Execution-Version", fork)
	}
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

func TestSSZRESTCapabilitiesIndependentOfForkSchedule(t *testing.T) {
	for _, cfg := range []*chain.Config{{}, allForksConfig(), {ShanghaiTime: common.NewUint64(0)}} {
		srv := newTestEngineServer(cfg, false)
		rec := httptest.NewRecorder()
		srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/capabilities", "", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var caps sszRestCapabilities
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &caps))
		require.Equal(t, []string{"paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"}, caps.SupportedForks)
	}
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

	t.Run("paris canonical attributes decode", func(t *testing.T) {
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             99,
			PrevRandao:            common.HexToHash("0x04"),
			SuggestedFeeRecipient: common.HexToAddress("0x05"),
			SSZVersion:            clparams.BellatrixVersion,
		}
		stateBytes, err := state.EncodeSSZ(nil)
		require.NoError(t, err)
		attrBytes, err := attrs.EncodeSSZ(nil)
		require.NoError(t, err)
		enc := binary.LittleEndian.AppendUint32(stateBytes, 100)
		enc = append(enc, attrBytes...)

		outState, outAttrs, outCustody, err := decodeForkchoiceUpdate(enc, clparams.BellatrixVersion)
		require.NoError(t, err)
		require.Equal(t, state, outState)
		require.Equal(t, attrs, outAttrs)
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

func TestSSZRESTForkchoiceUpdateRejectsShortCustodyColumns(t *testing.T) {
	for length := 1; length < 16; length++ {
		t.Run(fmt.Sprintf("%d bytes", length), func(t *testing.T) {
			enc, err := encodeForkchoiceUpdate(clparams.GloasVersion, &engine_types.ForkChoiceState{}, nil, make([]byte, length))
			require.NoError(t, err)

			_, _, _, err = decodeForkchoiceUpdate(enc, clparams.GloasVersion)
			require.ErrorContains(t, err, "custody columns bitvector length")
		})
	}
}

// Pin the PayloadStatus byte examples from refactor-ssz.md. A VALID status
// with latest_valid_hash present and no validation_error is 41 bytes.
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
}

func TestSSZRESTPayloadStatusTruncatesValidationError(t *testing.T) {
	message := strings.Repeat("x", 2000)
	status := &engine_types.PayloadStatus{
		Status:          engine_types.InvalidStatus,
		ValidationError: engine_types.NewStringifiedErrorFromString(message),
	}
	encoded, err := status.EncodeSSZ(nil)
	require.NoError(t, err)

	var decoded engine_types.PayloadStatus
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	require.NotNil(t, decoded.ValidationError)
	require.Equal(t, message[:1024], decoded.ValidationError.Error().Error())
}

func TestSSZRESTPayloadStatusRejectsMultipleLatestValidHashes(t *testing.T) {
	latest := common.HexToHash("0x01")
	status := &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &latest}
	encoded, err := status.EncodeSSZ(nil)
	require.NoError(t, err)

	extraHash := common.HexToHash("0x02")
	encoded = append(encoded, extraHash[:]...)
	const validationErrorOffsetPosition = 5
	binary.LittleEndian.PutUint32(encoded[validationErrorOffsetPosition:], uint32(len(encoded)))

	var decoded engine_types.PayloadStatus
	require.ErrorContains(t, decoded.DecodeSSZ(encoded, 0), "latest_valid_hash")
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
	blockValue := uint256.NewInt(112233)
	bundle := &engine_types.BlobsBundle{
		Commitments: []hexutil.Bytes{bytes.Repeat([]byte{0x01}, sszKZGBytes)},
		Proofs:      []hexutil.Bytes{bytes.Repeat([]byte{0x02}, sszKZGBytes)},
		Blobs:       []hexutil.Bytes{bytes.Repeat([]byte{0x03}, sszBlobBytes)},
	}
	requests := []hexutil.Bytes{{0x00, 0xaa}, {0x01, 0xbb}}

	for _, tc := range []struct {
		version           clparams.StateVersion
		wantSOB           bool
		wantBundle        bool
		wantRequests      bool
		wantPayloadOffset uint32
	}{
		{clparams.BellatrixVersion, false, false, false, 36},
		{clparams.CapellaVersion, false, false, false, 36},
		{clparams.DenebVersion, true, true, false, 41},
		{clparams.ElectraVersion, true, true, true, 45},
		{clparams.FuluVersion, true, true, true, 45},
		{clparams.GloasVersion, true, true, true, 45},
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
				BlockValue:            (*hexutil.U256)(blockValue),
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
			require.Equal(t, tc.wantPayloadOffset, binary.LittleEndian.Uint32(enc[:4]))
			if tc.wantSOB {
				require.Equal(t, byte(1), enc[tc.wantPayloadOffset-1], "should_override_builder must end the fixed section")
			}
			out, err := decodeBuiltPayload(enc, tc.version)
			require.NoError(t, err)
			require.Equal(t, blockValue, (*uint256.Int)(out.BlockValue))
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
	blockAccessList := hexutil.Bytes{0xc0}
	body := &engine_types.ExecutionPayloadBodyV2{
		Transactions:    []hexutil.Bytes{{0x01, 0x02}},
		Withdrawals:     []*types.Withdrawal{withdrawal},
		BlockAccessList: &blockAccessList,
	}
	bodyWithoutBAL := *body
	bodyWithoutBAL.BlockAccessList = nil

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
			enc, err := encodeBodiesResponse([]*engine_types.ExecutionPayloadBodyV2{body, &bodyWithoutBAL, nil}, tc.version)
			require.NoError(t, err)
			// BodiesResponse is a single-field container: the body opens with a
			// 4-byte offset (=4) to the wrapped entries list.
			require.Equal(t, uint32(4), binary.LittleEndian.Uint32(enc[:4]))
			entries, err := decodeBodiesResponse(enc, tc.version)
			require.NoError(t, err)
			require.Len(t, entries, 3)

			require.True(t, entries[0].Available)
			require.Equal(t, body.Transactions, entries[0].Transactions)
			if tc.wantWithdrawals {
				require.Len(t, entries[0].Withdrawals, 1)
				require.Equal(t, *withdrawal, *entries[0].Withdrawals[0])
			} else {
				require.Empty(t, entries[0].Withdrawals)
			}
			if tc.wantBAL {
				require.Equal(t, *body.BlockAccessList, entries[0].BlockAccessList)
			} else {
				require.Empty(t, entries[0].BlockAccessList)
			}

			if tc.wantBAL {
				require.False(t, entries[1].Available)
				require.Empty(t, entries[1].Transactions)
				require.Empty(t, entries[1].Withdrawals)
			} else {
				require.True(t, entries[1].Available)
				require.Equal(t, entries[0].Transactions, entries[1].Transactions)
				require.Equal(t, entries[0].Withdrawals, entries[1].Withdrawals)
			}
			require.Empty(t, entries[1].BlockAccessList)

			require.False(t, entries[2].Available)
			require.Empty(t, entries[2].Transactions)
		})
	}
}

func TestSSZRESTBodiesRequestLimit(t *testing.T) {
	body, err := encodeHashListRequest(make([]common.Hash, 32), 32)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{"range", http.MethodGet, "/engine/v1/bodies?from=1&count=32", nil},
		{"hash", http.MethodPost, "/engine/v1/bodies/hash", body},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &bodiesExecutionModule{
				headers: map[uint64]*types.Header{
					32: {Number: *uint256.NewInt(32), Time: 200},
				},
			}
			srv := newBodiesTestEngineServer(t, module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), tc.method, tc.path, "cancun", tc.body))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
			require.NoError(t, err)
			require.Len(t, entries, 32)
		})
	}
}

func TestSSZRESTBodiesExcludePoW(t *testing.T) {
	pow := &types.Header{Number: *uint256.NewInt(10), Time: 98, Difficulty: *uint256.NewInt(131072)}
	pos := &types.Header{Number: *uint256.NewInt(11), Time: 99, ParentHash: pow.Hash()}
	hashes := []common.Hash{pow.Hash(), pos.Hash()}
	body, err := encodeHashListRequest(hashes, sszMaxBodiesRequest)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{"hash", http.MethodPost, "/engine/v1/bodies/hash", body},
		{"range", http.MethodGet, "/engine/v1/bodies?from=10&count=2", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &bodiesExecutionModule{
				headers:       map[uint64]*types.Header{10: pow, 11: pos},
				headersByHash: map[common.Hash]*types.Header{pow.Hash(): pow, pos.Hash(): pos},
				bodies: map[common.Hash]*execmodule.PayloadBody{
					pow.Hash(): {Transactions: [][]byte{{0x01}}},
					pos.Hash(): {Transactions: [][]byte{{0x02}}},
				},
			}
			srv := newBodiesTestEngineServer(t, module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), tc.method, tc.path, "paris", tc.body))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.BellatrixVersion)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			require.False(t, entries[0].Available)
			require.Empty(t, entries[0].Transactions)
			require.True(t, entries[1].Available)
			require.Equal(t, []hexutil.Bytes{{0x02}}, entries[1].Transactions)
			require.Equal(t, [][]common.Hash{{pos.Hash()}}, module.bodyRequests)
		})
	}
}

func TestSSZRESTBodiesByHashFiltersForkEra(t *testing.T) {
	shanghai := &types.Header{Time: 199}
	cancun := &types.Header{Time: 200}
	prague := &types.Header{Time: 300}
	module := &bodiesExecutionModule{
		bodies: map[common.Hash]*execmodule.PayloadBody{
			prague.Hash():   {Transactions: [][]byte{{0x03}}},
			cancun.Hash():   {Transactions: [][]byte{{0x02}}},
			shanghai.Hash(): {Transactions: [][]byte{{0x01}}},
		},
		headersByHash: map[common.Hash]*types.Header{
			shanghai.Hash(): shanghai,
			cancun.Hash():   cancun,
			prague.Hash():   prague,
		},
	}
	srv := newBodiesTestEngineServer(t, module)
	hashes := []common.Hash{prague.Hash(), cancun.Hash(), shanghai.Hash(), {0xff}}
	body, err := encodeHashListRequest(hashes, sszMaxBodiesRequest)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/bodies/hash", "cancun", body))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, [][]common.Hash{{cancun.Hash()}}, module.bodyRequests)
	entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
	require.NoError(t, err)
	require.Len(t, entries, len(hashes))
	for i, available := range []bool{false, true, false, false} {
		require.Equal(t, available, entries[i].Available, "entry %d", i)
		if available {
			require.Equal(t, []hexutil.Bytes{{0x02}}, entries[i].Transactions)
		} else {
			require.Empty(t, entries[i].Transactions)
			require.Empty(t, entries[i].Withdrawals)
			require.Empty(t, entries[i].BlockAccessList)
		}
	}
}

func TestSSZRESTBodiesByHashOmitsAKnownBadBlock(t *testing.T) {
	valid := &types.Header{Time: 200}
	invalid := &types.Header{Time: 201}
	module := &bodiesExecutionModule{
		bodies: map[common.Hash]*execmodule.PayloadBody{
			valid.Hash():   {Transactions: [][]byte{{0x01}}},
			invalid.Hash(): {Transactions: [][]byte{{0x02}}},
		},
		headersByHash: map[common.Hash]*types.Header{valid.Hash(): valid, invalid.Hash(): invalid},
	}
	srv := newBodiesTestEngineServer(t, module)
	srv.blockDownloader.ReportBadHeader(invalid.Hash(), common.Hash{}, "invalid block")
	body, err := encodeHashListRequest([]common.Hash{valid.Hash(), invalid.Hash(), valid.Hash()}, sszMaxBodiesRequest)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/bodies/hash", "cancun", body))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for i, available := range []bool{true, false, true} {
		require.Equal(t, available, entries[i].Available, "entry %d", i)
	}
}

func TestSSZRESTBodiesByRangePreservesPositions(t *testing.T) {
	headers := map[uint64]*types.Header{
		10: {Number: *uint256.NewInt(10), Time: 199},
		11: {Number: *uint256.NewInt(11), Time: 200},
		13: {Number: *uint256.NewInt(13), Time: 299},
		14: {Number: *uint256.NewInt(14), Time: 300},
	}
	module := &bodiesExecutionModule{
		bodies: map[common.Hash]*execmodule.PayloadBody{
			headers[10].Hash(): {Transactions: [][]byte{{0x01}}},
			headers[11].Hash(): {Transactions: [][]byte{{0x02}}},
			headers[13].Hash(): {Transactions: [][]byte{{0x04}}},
			headers[14].Hash(): {Transactions: [][]byte{{0x05}}},
		},
		headers: headers,
	}
	srv := NewEngineServer(log.New(), allForksConfig(), module, nil, false, true, false, true, nil, nil, 0, 0)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/bodies?from=10&count=6", "cancun", nil))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, [][]common.Hash{{headers[11].Hash(), headers[13].Hash()}}, module.bodyRequests)
	entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
	require.NoError(t, err)
	require.Len(t, entries, 5)
	for i, available := range []bool{false, true, false, true, false} {
		require.Equal(t, available, entries[i].Available, "entry %d", i)
		if available {
			require.Equal(t, []hexutil.Bytes{{byte(i + 1)}}, entries[i].Transactions)
		} else {
			require.Empty(t, entries[i].Transactions)
			require.Empty(t, entries[i].Withdrawals)
			require.Empty(t, entries[i].BlockAccessList)
		}
	}
}

func TestSSZRESTBodiesRejectHeaderReadErrors(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(10), Time: 200}
	hash := header.Hash()
	body, err := encodeHashListRequest([]common.Hash{hash, {0xff}}, sszMaxBodiesRequest)
	require.NoError(t, err)
	headerErr := errors.New("header lookup failed")
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{"hash", http.MethodPost, "/engine/v1/bodies/hash", body},
		{"range", http.MethodGet, "/engine/v1/bodies?from=10&count=2", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodiesRead := false
			module := &stubExecutionModule{
				getHeaderFunc: func(_ context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
					if (blockHash != nil && *blockHash == hash) || (blockNumber != nil && *blockNumber == 10) {
						return header, nil
					}
					return nil, headerErr
				},
				getHeadersByRangeFunc: func(context.Context, uint64, uint64) ([]*types.Header, error) {
					return nil, headerErr
				},
				getPayloadBodiesByHashFunc: func(_ context.Context, hashes []common.Hash) ([]*execmodule.PayloadBody, error) {
					bodiesRead = true
					return make([]*execmodule.PayloadBody, len(hashes)), nil
				},
			}
			srv := newBodiesTestEngineServer(t, module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), tc.method, tc.path, "cancun", tc.body))

			require.Equal(t, http.StatusInternalServerError, rec.Code)
			require.Equal(t, problemInternal, problemType(t, rec))
			require.Contains(t, rec.Body.String(), headerErr.Error())
			require.False(t, bodiesRead)
		})
	}
}

func TestSSZRESTBodiesSkipRetrievalOutsideForkEra(t *testing.T) {
	headers := map[uint64]*types.Header{
		10: {Number: *uint256.NewInt(10), Time: 500},
		11: {Number: *uint256.NewInt(11), Time: 501},
	}
	body, err := encodeHashListRequest([]common.Hash{headers[10].Hash(), headers[11].Hash(), {0xff}}, sszMaxBodiesRequest)
	require.NoError(t, err)
	for _, tc := range []struct {
		name        string
		method      string
		path        string
		body        []byte
		wantEntries int
	}{
		{"hash", http.MethodPost, "/engine/v1/bodies/hash", body, 3},
		{"range", http.MethodGet, "/engine/v1/bodies?from=10&count=3", nil, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &bodiesExecutionModule{
				bodies: map[common.Hash]*execmodule.PayloadBody{
					headers[10].Hash(): {Transactions: [][]byte{{0x01}}},
					headers[11].Hash(): {Transactions: [][]byte{{0x02}}},
				},
				headers: headers,
				headersByHash: map[common.Hash]*types.Header{
					headers[10].Hash(): headers[10],
					headers[11].Hash(): headers[11],
				},
			}
			srv := newBodiesTestEngineServer(t, module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), tc.method, tc.path, "cancun", tc.body))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Empty(t, module.bodyRequests)
			entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
			require.NoError(t, err)
			require.Len(t, entries, tc.wantEntries)
			for _, entry := range entries {
				require.False(t, entry.Available)
				require.Empty(t, entry.Transactions)
				require.Empty(t, entry.Withdrawals)
				require.Empty(t, entry.BlockAccessList)
			}
		})
	}
}

func TestSSZRESTBodiesByRangeReorg(t *testing.T) {
	for _, tc := range []struct {
		fork      string
		version   clparams.StateVersion
		available bool
	}{
		{"shanghai", clparams.CapellaVersion, true},
		{"cancun", clparams.DenebVersion, false},
	} {
		t.Run(tc.fork, func(t *testing.T) {
			shanghai := []*types.Header{
				{Number: *uint256.NewInt(10), Time: 198},
				{Number: *uint256.NewInt(11), Time: 199},
			}
			shanghai[1].ParentHash = shanghai[0].Hash()
			cancun := []*types.Header{
				{Number: *uint256.NewInt(10), Time: 200},
				{Number: *uint256.NewInt(11), Time: 201},
			}
			cancun[1].ParentHash = cancun[0].Hash()
			canonical := shanghai
			bodies := map[common.Hash]*execmodule.PayloadBody{
				shanghai[0].Hash(): {Transactions: [][]byte{{0x01}}},
				shanghai[1].Hash(): {Transactions: [][]byte{{0x02}}},
				cancun[0].Hash():   {Transactions: [][]byte{{0x03}}},
				cancun[1].Hash():   {Transactions: [][]byte{{0x04}}},
			}
			module := &stubExecutionModule{
				getHeaderFunc: func(_ context.Context, _ *common.Hash, number *uint64) (*types.Header, error) {
					header := canonical[*number-10]
					canonical = cancun
					return header, nil
				},
				getHeadersByRangeFunc: func(_ context.Context, start, count uint64) ([]*types.Header, error) {
					headers := canonical[start-10 : start-10+count]
					canonical = cancun
					return headers, nil
				},
				getPayloadBodiesByHashFunc: func(_ context.Context, hashes []common.Hash) ([]*execmodule.PayloadBody, error) {
					result := make([]*execmodule.PayloadBody, len(hashes))
					for i, hash := range hashes {
						result[i] = bodies[hash]
					}
					return result, nil
				},
			}
			srv := NewEngineServer(log.New(), allForksConfig(), module, nil, false, true, false, true, nil, nil, 0, 0)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/bodies?from=10&count=2", tc.fork, nil))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			entries, err := decodeBodiesResponse(rec.Body.Bytes(), tc.version)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			for i, entry := range entries {
				require.Equal(t, tc.available, entry.Available, "entry %d", i)
				if tc.available {
					require.Equal(t, []hexutil.Bytes{{byte(i + 1)}}, entry.Transactions)
				} else {
					require.Empty(t, entry.Transactions)
				}
			}
		})
	}
}

func TestSSZRESTBodiesByRangePreservesTrailingUnavailableBody(t *testing.T) {
	const (
		from      = uint64(10)
		blockTime = uint64(250)
	)
	header := &types.Header{Number: *uint256.NewInt(from), Time: blockTime}
	module := &bodiesExecutionModule{
		bodies: map[common.Hash]*execmodule.PayloadBody{header.Hash(): {Transactions: [][]byte{{0x01}}}},
		headers: map[uint64]*types.Header{
			from:     header,
			from + 1: {Number: *uint256.NewInt(from + 1), Time: blockTime},
		},
	}
	srv := NewEngineServer(log.New(), allForksConfig(), module, nil, false, true, false, true, nil, nil, 0, 0)

	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/bodies?from=10&count=3", "cancun", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	entries, err := decodeBodiesResponse(rec.Body.Bytes(), clparams.DenebVersion)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.True(t, entries[0].Available)
	require.False(t, entries[1].Available)
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

// Unavailable entries carry SSZ zero values: a zero-filled blob and proof for
// v1, or a zero-filled blob and empty proof list for the v2/v3 entry schema.
// The v2/v3 unavailable entry is used by /blobs/v3; /blobs/v2 returns 204 on a miss.
func TestSSZRESTBlobsUnavailableEntryByteLayout(t *testing.T) {
	t.Run("v1", func(t *testing.T) {
		enc, err := encodeBlobsV1Response([]*engine_types.BlobAndProofV1{nil})
		require.NoError(t, err)
		want := make([]byte, 4+1+sszBlobBytes+sszKZGBytes)
		binary.LittleEndian.PutUint32(want, 4) // offset of the entries list
		// available=false and the zero-filled blob and proof stay zero
		require.Len(t, enc, len(want))
		require.Equal(t, want, enc)
	})

	t.Run("v2", func(t *testing.T) {
		enc, err := encodeBlobsV2Response([]*engine_types.BlobAndProofV2{nil})
		require.NoError(t, err)
		want := make([]byte, 4+4+1+4+sszBlobBytes+4)
		binary.LittleEndian.PutUint32(want, 4)                                // offset of the entries list
		binary.LittleEndian.PutUint32(want[4:], 4)                            // offset of the single entry
		binary.LittleEndian.PutUint32(want[9:], 5)                            // offset of contents within the entry
		binary.LittleEndian.PutUint32(want[13+sszBlobBytes:], sszBlobBytes+4) // empty cell_proofs list offset
		require.Len(t, enc, len(want))
		require.Equal(t, want, enc)
	})
}

func TestSSZRESTHashListRequestRoundTrip(t *testing.T) {
	hashes := []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")}
	for _, limit := range []int{sszMaxBodiesRequest, maxGetBlobHashes} {
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

func TestSSZRESTHashListRequestRejectsInvalidOffset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int
		offset uint32
	}{
		{"before fixed section", 32, 0},
		{"after fixed section", 40, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := make([]byte, tc.size)
			binary.LittleEndian.PutUint32(body, tc.offset)

			_, err := decodeHashListRequest(body, sszMaxBodiesRequest)

			require.ErrorIs(t, err, commonssz.ErrBadOffset)
		})
	}
}

func TestSSZRESTNewPayloadEnvelopeRejectsInvalidOffset(t *testing.T) {
	payload := engine_types.NewExecutionPayloadSSZ(clparams.BellatrixVersion)
	body, err := encodeNewPayloadEnvelope(clparams.BellatrixVersion, payload, common.Hash{}, nil)
	require.NoError(t, err)

	malformed := make([]byte, len(body)+4)
	binary.LittleEndian.PutUint32(malformed, 8)
	copy(malformed[8:], body[4:])

	_, _, _, err = decodeNewPayloadEnvelope(malformed, clparams.BellatrixVersion)
	require.ErrorIs(t, err, commonssz.ErrBadOffset)
}

func TestSSZRESTForkchoiceUpdateRejectsInvalidOffset(t *testing.T) {
	body, err := encodeForkchoiceUpdate(clparams.BellatrixVersion, &engine_types.ForkChoiceState{}, nil, nil)
	require.NoError(t, err)

	malformed := make([]byte, len(body)+4)
	copy(malformed, body)
	binary.LittleEndian.PutUint32(malformed[96:], uint32(len(malformed)))

	_, _, _, err = decodeForkchoiceUpdate(malformed, clparams.BellatrixVersion)
	require.ErrorIs(t, err, commonssz.ErrBadOffset)
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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/engine/v1/capabilities", nil)
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
	require.Equal(t, []string{"v1", "v2", "v3", "v4"}, caps.IndependentlyVersioned["blobs"])
	require.Equal(t, []string{"capabilities", "identity", "inclusion-list"}, caps.UnscopedEndpoints)
	// pin the spec's MAX_* values: MAX_BODIES_REQUEST, MAX_VERSIONED_HASHES_PER_REQUEST,
	// MAX_REQUEST_BODY_SIZE (2**26 = 64 MiB).
	require.Equal(t, uint64(32), caps.Limits["bodies.max_count"])
	require.Equal(t, uint64(128), caps.Limits["blobs.max_versioned_hashes"])
	require.Equal(t, uint64(67108864), caps.Limits["payload.max_bytes"])
}

func TestSSZRESTIdentityRoute(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{}, false)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/engine/v1/identity", nil)
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

// Unscoped endpoints ignore Eth-Execution-Version, even a bogus value, per the spec.
func TestSSZRESTUnscopedIgnoreForkHeader(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), false)
	for _, path := range []string{"/engine/v1/capabilities", "/engine/v1/identity"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		req.Header.Set("Eth-Execution-Version", "not-a-fork")
		rec := httptest.NewRecorder()
		srv.SSZRESTHandler().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"), path)
	}
}

func TestSSZRESTRoutingErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		method      string
		path        string
		fork        string
		contentType string
		code        int
		problem     string
	}{
		{"unknown top-level endpoint", http.MethodPost, "/engine/v1/foobar", "cancun", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"unknown fork header", http.MethodPost, "/engine/v1/payloads", "foobar", sszRestContentType, http.StatusBadRequest, problemUnsupportedFork},
		{"missing fork header", http.MethodPost, "/engine/v1/payloads", "", sszRestContentType, http.StatusBadRequest, problemUnsupportedFork},
		{"trailing slash", http.MethodPost, "/engine/v1/payloads/", "cancun", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"unknown scoped route without fork", http.MethodPost, "/engine/v1/bodies/not-a-route", "", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"method mismatch on scoped route without fork", http.MethodGet, "/engine/v1/forkchoice", "", "", http.StatusNotFound, problemMethodNotFound},
		{"method mismatch on capabilities", http.MethodPost, "/engine/v1/capabilities", "", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"wrong content type", http.MethodPost, "/engine/v1/payloads", "cancun", "application/json", http.StatusUnsupportedMediaType, problemUnsupportedMediaType},
		{"bad ssz body", http.MethodPost, "/engine/v1/payloads", "cancun", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bad forkchoice body", http.MethodPost, "/engine/v1/forkchoice", "cancun", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bodies range missing params", http.MethodGet, "/engine/v1/bodies", "cancun", "", http.StatusBadRequest, problemInvalidRequest},
		{"bodies range zero start", http.MethodGet, "/engine/v1/bodies?from=0&count=1", "cancun", "", http.StatusUnprocessableEntity, problemInvalidBody},
		{"bodies range zero count", http.MethodGet, "/engine/v1/bodies?from=1&count=0", "cancun", "", http.StatusUnprocessableEntity, problemInvalidBody},
		{"bodies range too large", http.MethodGet, "/engine/v1/bodies?from=1&count=33", "cancun", "", http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{"bodies hash too many", http.MethodPost, "/engine/v1/bodies/hash", "cancun", sszRestContentType, http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{"bad bodies hash body", http.MethodPost, "/engine/v1/bodies/hash", "cancun", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"blobs unknown revision", http.MethodPost, "/engine/v1/blobs/v9", "", sszRestContentType, http.StatusNotFound, problemMethodNotFound},
		{"blobs too many hashes", http.MethodPost, "/engine/v1/blobs/v1", "", sszRestContentType, http.StatusRequestEntityTooLarge, problemRequestTooLarge},
		{"bad blobs v1 body", http.MethodPost, "/engine/v1/blobs/v1", "", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bad blobs v2 body", http.MethodPost, "/engine/v1/blobs/v2", "", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bad blobs v3 body", http.MethodPost, "/engine/v1/blobs/v3", "", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
		{"bad blobs v4 body", http.MethodPost, "/engine/v1/blobs/v4", "", sszRestContentType, http.StatusBadRequest, problemSSZDecodeError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestEngineServer(allForksConfig(), false)
			srv.executionService = &bodiesExecutionModule{}
			var logs bytes.Buffer
			srv.logger.SetHandler(log.StreamHandler(&logs, log.JsonFormat()))
			var body []byte
			switch tc.name {
			case "bad ssz body":
				body = []byte("bad-ssz")
			case "bodies hash too many":
				body = bytes.Repeat([]byte{0x01}, (sszMaxBodiesRequest+1)*32)
			case "blobs too many hashes":
				body = bytes.Repeat([]byte{0x01}, (maxGetBlobHashes+1)*32)
			}
			req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, bytes.NewReader(body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.fork != "" {
				req.Header.Set("Eth-Execution-Version", tc.fork)
			}
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, req)
			require.Equal(t, tc.code, rec.Code)
			require.Equal(t, tc.problem, problemType(t, rec))
			if tc.problem == problemSSZDecodeError {
				require.JSONEq(t, `{"type":"/engine-api/errors/ssz-decode-error"}`, rec.Body.String())
				require.NotEmpty(t, logs.String(), "SSZ decode failures must be logged")
				var record map[string]any
				require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
				require.Equal(t, log.LvlDebug.String(), record["lvl"])
				require.Equal(t, tc.path, record["path"])
				require.Equal(t, tc.fork, record["fork"])
				require.Contains(t, record["err"], commonssz.ErrLowBufferSize.Error())
			}
		})
	}
}

func TestSSZRESTUnscheduledForkSchema(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{}, false)
	for _, fork := range []string{"paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"} {
		t.Run(fork, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/payloads", fork, nil))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, problemSSZDecodeError, problemType(t, rec))
		})
	}
}

func TestSSZRESTNewPayloadForkMatching(t *testing.T) {
	for _, tc := range []struct {
		fork      string
		timestamp hexutil.Uint64
		code      int
	}{
		{"paris", 99, http.StatusOK},
		{"paris", 100, http.StatusBadRequest},
		{"shanghai", 99, http.StatusBadRequest},
		{"shanghai", 100, http.StatusOK},
		{"shanghai", 200, http.StatusBadRequest},
		{"cancun", 199, http.StatusBadRequest},
		{"cancun", 200, http.StatusOK},
		{"cancun", 300, http.StatusBadRequest},
		{"prague", 299, http.StatusBadRequest},
		{"prague", 300, http.StatusOK},
		{"prague", 400, http.StatusBadRequest},
		{"osaka", 399, http.StatusBadRequest},
		{"osaka", 400, http.StatusOK},
		{"osaka", 500, http.StatusBadRequest},
		{"amsterdam", 499, http.StatusBadRequest},
		{"amsterdam", 500, http.StatusOK},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.fork, tc.timestamp), func(t *testing.T) {
			version, ok := engineForkVersion(tc.fork)
			require.True(t, ok)
			payload := engine_types.NewExecutionPayloadSSZ(version)
			payload.Timestamp = tc.timestamp
			if version >= clparams.GloasVersion {
				payload.BlockAccessList = &hexutil.Bytes{0xc0}
			}
			body, err := encodeNewPayloadEnvelope(version, payload, common.Hash{}, nil)
			require.NoError(t, err)
			srv := newTestEngineServer(allForksConfig(), true)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/payloads", tc.fork, body))

			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			if tc.code == http.StatusBadRequest {
				require.Equal(t, problemUnsupportedFork, problemType(t, rec))
				return
			}
			require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
			var status engine_types.PayloadStatus
			require.NoError(t, status.DecodeSSZ(rec.Body.Bytes(), 0))
			require.Equal(t, engine_types.InvalidStatus, status.Status)
			require.ErrorContains(t, status.ValidationError.Error(), "invalid block hash")
		})
	}
}

func TestSSZRESTNewPayloadRejectsExcessTransactionsBeforeDecoding(t *testing.T) {
	const txCount = 10_000
	var tx bytes.Buffer
	require.NoError(t, (&types.LegacyTx{}).MarshalBinary(&tx))
	txs := make([]hexutil.Bytes, txCount)
	for i := range txs {
		txs[i] = tx.Bytes()
	}

	for _, tc := range []struct {
		fork      string
		version   clparams.StateVersion
		timestamp hexutil.Uint64
	}{
		{"cancun", clparams.DenebVersion, 200},
		{"prague", clparams.ElectraVersion, 300},
		{"osaka", clparams.FuluVersion, 400},
		{"amsterdam", clparams.GloasVersion, 500},
	} {
		t.Run(tc.fork, func(t *testing.T) {
			payload := engine_types.NewExecutionPayloadSSZ(tc.version)
			payload.Timestamp = tc.timestamp
			payload.GasLimit = 21_000
			payload.Transactions = txs
			if tc.version >= clparams.GloasVersion {
				payload.BlockAccessList = &hexutil.Bytes{0xc0}
			}
			body, err := encodeNewPayloadEnvelope(tc.version, payload, common.Hash{}, nil)
			require.NoError(t, err)
			srv := newTestEngineServer(allForksConfig(), true)
			handler := srv.SSZRESTHandler()
			var rec *httptest.ResponseRecorder
			allocs := testing.AllocsPerRun(1, func() {
				rec = httptest.NewRecorder()
				handler.ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/payloads", tc.fork, body))
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var status engine_types.PayloadStatus
			require.NoError(t, status.DecodeSSZ(rec.Body.Bytes(), 0))
			require.Equal(t, engine_types.InvalidStatus, status.Status)
			require.ErrorContains(t, status.ValidationError.Error(), "gas limit reached: 10000 transactions")
			require.Less(t, allocs, float64(txCount/10), "rejected payload must not allocate transaction objects")
		})
	}
}

func TestSSZRESTNewPayloadBlobValidation(t *testing.T) {
	const blobLimit = 10
	for _, tc := range []struct {
		name           string
		blobHash       common.Hash
		blobCounts     []int
		frame          bool
		wrongBlockHash bool
		wantStatus     engine_types.EngineStatus
		wantError      string
	}{
		{name: "valid versioned hash", blobHash: common.Hash{0x01, 0x01}, wantStatus: engine_types.SyncingStatus},
		{name: "invalid versioned hash", blobHash: common.Hash{0x02, 0x01}, wantStatus: engine_types.InvalidStatus, wantError: misc.ErrInvalidVersionedHash.Error()},
		{name: "frame transaction", blobHash: common.Hash{0x01, 0x01}, frame: true, wantStatus: engine_types.SyncingStatus},
		{name: "at configured limit", blobHash: common.Hash{0x01, 0x01}, blobCounts: []int{blobLimit}, wantStatus: engine_types.SyncingStatus},
		{name: "at configured limit across transactions", blobHash: common.Hash{0x01, 0x01}, blobCounts: []int{4, blobLimit - 4}, wantStatus: engine_types.SyncingStatus},
		{name: "too many blobs in one transaction", blobHash: common.Hash{0x01, 0x01}, blobCounts: []int{blobLimit + 1}, wantStatus: engine_types.InvalidStatus, wantError: misc.ErrMaxBlobGasUsed.Error()},
		{name: "too many blobs across transactions", blobHash: common.Hash{0x01, 0x01}, blobCounts: []int{4, blobLimit - 3}, wantStatus: engine_types.InvalidStatus, wantError: misc.ErrMaxBlobGasUsed.Error()},
		{name: "block hash checked before blob limit", blobHash: common.Hash{0x01, 0x01}, blobCounts: []int{blobLimit + 1}, wrongBlockHash: true, wantStatus: engine_types.InvalidStatus, wantError: "invalid block hash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := prePragueChainConfig()
			cfg.BlobSchedule = map[string]*params.BlobConfig{"cancun": {Max: blobLimit}}
			version, fork := clparams.DenebVersion, "cancun"
			blobCounts := tc.blobCounts
			if blobCounts == nil {
				blobCounts = []int{1}
			}
			var blobCount int
			txs := make(types.Transactions, 0, len(blobCounts))
			for _, count := range blobCounts {
				hashes := make([]common.Hash, count)
				for i := range hashes {
					hashes[i] = tc.blobHash
				}
				txs = append(txs, &types.BlobTx{
					DynamicFeeTransaction: types.DynamicFeeTransaction{
						CommonTx: types.CommonTx{GasLimit: 21_000, To: &common.Address{0x01}},
					},
					BlobVersionedHashes: hashes,
				})
				blobCount += count
			}
			if tc.frame {
				cfg = allForksChainConfig()
				cfg.BogotaTime = common.NewUint64(0)
				version, fork = clparams.GloasVersion, "amsterdam"
				txs[0] = &types.FrameTransaction{
					ChainID: *cfg.ChainID, NonceKeys: nil, SenderAddress: common.Address{0x01},
					Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 21_000}}},
					BlobVersionedHashes: []common.Hash{tc.blobHash},
				}
			}
			header := makeParentHeader(1001)
			header.ParentHash = common.Hash{0x01}
			header.UncleHash = empty.UncleHash
			header.TxHash = types.DeriveSha(txs)
			withdrawals := types.Withdrawals{}
			withdrawalsHash := types.DeriveSha(withdrawals)
			header.WithdrawalsHash = &withdrawalsHash
			header.BlobGasUsed = common.NewUint64(misc.GetBlobGasUsed(blobCount))
			header.ExcessBlobGas = new(uint64)
			header.ParentBeaconBlockRoot = &common.Hash{}
			if tc.frame {
				header.RequestsHash = (types.FlatRequests{}).Hash()
				balHash := empty.BlockAccessListHash
				header.BlockAccessListHash = &balHash
				header.SlotNumber = new(uint64)
			}
			block := types.NewBlockFromStorage(header.Hash(), header, txs, nil, withdrawals, nil)
			payload, err := engine_types.ExecutionPayloadFromBlock(block)
			require.NoError(t, err)
			payload.SSZVersion = version
			if tc.wrongBlockHash {
				payload.BlockHash[0] ^= 0xff
			}
			if tc.frame {
				payload.BlockAccessList = &hexutil.Bytes{0xc0}
			}
			body, err := encodeNewPayloadEnvelope(version, payload, *header.ParentBeaconBlockRoot, nil)
			require.NoError(t, err)
			stub := &stubExecutionModule{}
			downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
			srv.test = true
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/payloads", fork, body))
			require.Equal(t, http.StatusOK, rec.Code)
			var status engine_types.PayloadStatus
			require.NoError(t, status.DecodeSSZ(rec.Body.Bytes(), 0))
			var validationErr error
			if status.ValidationError != nil {
				validationErr = status.ValidationError.Error()
			}
			require.Equal(t, tc.wantStatus, status.Status, "validation error: %v", validationErr)
			if tc.wantError != "" {
				require.EqualError(t, status.ValidationError.Error(), tc.wantError)
				if tc.wantError == misc.ErrMaxBlobGasUsed.Error() {
					require.Equal(t, &header.ParentHash, status.LatestValidHash)
				}
				return
			}
			require.Nil(t, status.ValidationError)

			newPayload := func(hashes []common.Hash) (*engine_types.PayloadStatus, error) {
				if tc.frame {
					return srv.NewPayloadV5(t.Context(), payload, hashes, header.ParentBeaconBlockRoot, []hexutil.Bytes{})
				}
				return srv.NewPayloadV3(t.Context(), payload, hashes, header.ParentBeaconBlockRoot)
			}
			_, err = newPayload(nil)
			require.ErrorContains(t, err, "nil blob hashes array")
			response, err := newPayload([]common.Hash{})
			require.NoError(t, err)
			require.Equal(t, engine_types.InvalidStatus, response.Status)
			require.EqualError(t, response.ValidationError.Error(), misc.ErrMismatchBlobHashes.Error())
		})
	}
}

func TestSSZRESTForkchoiceStartsPayloadBuild(t *testing.T) {
	const payloadID = uint64(0x0102030405060708)
	cfg := allForksConfig()
	cfg.TerminalTotalDifficulty = uint256.NewInt(0)
	state := &engine_types.ForkChoiceState{
		HeadHash:           common.Hash{0x01},
		SafeBlockHash:      common.Hash{0x02},
		FinalizedBlockHash: common.Hash{0x03},
	}
	attrs := goldenSampleAttrs(clparams.GloasVersion)
	attrs.Timestamp = 500
	module := &stubExecutionModule{
		getForkChoiceFunc: func(context.Context) (execmodule.ForkChoiceState, error) {
			return execmodule.ForkChoiceState{
				HeadHash: state.HeadHash, SafeHash: state.SafeBlockHash, FinalizedHash: state.FinalizedBlockHash,
			}, nil
		},
		getHeaderFunc: func(_ context.Context, hash *common.Hash, _ *uint64) (*types.Header, error) {
			require.Equal(t, &state.HeadHash, hash)
			return &types.Header{Time: 499}, nil
		},
		assembleBlockFunc: func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			require.Equal(t, &builder.Parameters{
				ParentHash:            state.HeadHash,
				Timestamp:             uint64(attrs.Timestamp),
				PrevRandao:            attrs.PrevRandao,
				SuggestedFeeRecipient: attrs.SuggestedFeeRecipient,
				Withdrawals:           attrs.Withdrawals,
				ParentBeaconBlockRoot: attrs.ParentBeaconBlockRoot,
				SlotNumber:            (*uint64)(attrs.SlotNumber),
				TargetGasLimit:        (*uint64)(attrs.TargetGasLimit),
			}, params)
			return execmodule.AssembleBlockResult{PayloadID: payloadID}, nil
		},
	}
	srv := newProposingEngineServerWithConfig(cfg, module)
	body, err := encodeForkchoiceUpdate(clparams.GloasVersion, state, attrs, nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/forkchoice", "amsterdam", body))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
	response, err := decodeForkchoiceResponse(rec.Body.Bytes())
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, response.PayloadStatus.Status)
	require.Equal(t, &state.HeadHash, response.PayloadStatus.LatestValidHash)
	require.Nil(t, response.PayloadStatus.ValidationError)
	require.NotNil(t, response.PayloadId)
	require.Equal(t, hexutil.Bytes{1, 2, 3, 4, 5, 6, 7, 8}, *response.PayloadId)
}

// REST treats a mismatch between Eth-Execution-Version and the fork determined by
// payload_attributes.timestamp as a request validation error. Known and unknown
// heads must return 400 unsupported-fork before applying forkchoice state or
// starting a payload build.
func TestSSZRESTForkchoiceHeaderForkMustMatchAttributes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fork      string
		version   clparams.StateVersion
		timestamp hexutil.Uint64
	}{
		{"cancun/older_header", "shanghai", clparams.CapellaVersion, 200},
		{"cancun/newer_header", "cancun", clparams.DenebVersion, 199},
		{"amsterdam/older_header", "osaka", clparams.FuluVersion, 500},
		{"amsterdam/newer_header", "amsterdam", clparams.GloasVersion, 499},
	} {
		for _, knownHead := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/known_head=%t", tc.name, knownHead), func(t *testing.T) {
				cfg := allForksConfig()
				cfg.TerminalTotalDifficulty = uint256.NewInt(0)
				state := &engine_types.ForkChoiceState{
					HeadHash:           common.Hash{0x01},
					SafeBlockHash:      common.Hash{0x02},
					FinalizedBlockHash: common.Hash{0x03},
				}
				attrs := goldenSampleAttrs(tc.version)
				attrs.Timestamp = tc.timestamp
				var forkchoiceReads, forkchoiceUpdates, payloadBuilds int
				module := &stubExecutionModule{
					getForkChoiceFunc: func(context.Context) (execmodule.ForkChoiceState, error) {
						forkchoiceReads++
						// Keep the current state different so a known head would need a forkchoice update.
						return execmodule.ForkChoiceState{}, nil
					},
					getHeaderFunc: func(context.Context, *common.Hash, *uint64) (*types.Header, error) {
						if !knownHead {
							return nil, nil
						}
						return &types.Header{Time: uint64(attrs.Timestamp) - 1}, nil
					},
					headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
						if !knownHead {
							return nil, nil
						}
						return common.NewUint64(1), nil
					},
					updateForkChoiceFunc: func(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
						forkchoiceUpdates++
						return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusSuccess, LatestValidHash: state.HeadHash}, nil
					},
					assembleBlockFunc: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
						payloadBuilds++
						return execmodule.AssembleBlockResult{PayloadID: 1}, nil
					},
				}
				srv := newProposingEngineServerWithConfig(cfg, module)
				srv.blockDownloader = engine_block_downloader.NewEngineBlockDownloader(
					t.Context(), log.New(), module, nil, nil, cfg, ethconfig.Sync{}, nil,
				)
				srv.test = true // Disable downloads when the requested head is unknown.
				body, err := encodeForkchoiceUpdate(tc.version, state, attrs, nil)
				require.NoError(t, err)
				rec := httptest.NewRecorder()
				srv.SSZRESTHandler().ServeHTTP(rec,
					newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/forkchoice", tc.fork, body))

				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				require.Equal(t, problemUnsupportedFork, problemType(t, rec))
				require.Zero(t, forkchoiceUpdates, "a fork mismatch must not apply forkchoice state")
				require.Zero(t, payloadBuilds, "a fork mismatch must not start a payload build")
				require.Zero(t, forkchoiceReads, "a fork mismatch must be rejected before forkchoice processing")
			})
		}
	}
}

func TestSSZRESTBlobsV2V3Availability(t *testing.T) {
	hash, bundle, _ := getBlobsV4Fixture(t, 7)
	getter := blobGetterMap{hash: bundle}
	for _, revision := range []string{"v2", "v3"} {
		t.Run(revision, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				getter txpool.BlobGetter
				hashes []common.Hash
				v2Code int
				v3Code int
			}{
				{"available", getter, []common.Hash{hash}, http.StatusOK, http.StatusOK},
				{"one missing", getter, []common.Hash{hash, {}, hash}, http.StatusNoContent, http.StatusOK},
				{"empty request", getter, nil, http.StatusOK, http.StatusOK},
				{"disabled pool", nil, []common.Hash{hash}, http.StatusNoContent, http.StatusNoContent},
				{"unavailable pool", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle { return nil }), []common.Hash{hash}, http.StatusNoContent, http.StatusNoContent},
			} {
				t.Run(tc.name, func(t *testing.T) {
					srv := newTestEngineServer(allForksConfig(), true)
					srv.blobGetter = tc.getter
					body, err := encodeHashListRequest(tc.hashes, maxGetBlobHashes)
					require.NoError(t, err)
					rec := httptest.NewRecorder()
					srv.SSZRESTHandler().ServeHTTP(rec,
						newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/blobs/"+revision, "", body))

					wantCode := tc.v2Code
					if revision == "v3" {
						wantCode = tc.v3Code
					}
					require.Equal(t, wantCode, rec.Code)
					if wantCode == http.StatusNoContent {
						require.Empty(t, rec.Body.Bytes())
						return
					}
					require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
					blobs, err := decodeBlobsV2Response(rec.Body.Bytes())
					require.NoError(t, err)
					require.Len(t, blobs, len(tc.hashes))
					for i, blob := range blobs {
						if tc.hashes[i] != hash {
							require.Nil(t, blob)
							continue
						}
						require.NotNil(t, blob)
						require.Equal(t, hexutil.Bytes(bundle.Blob), blob.Blob)
						require.Len(t, blob.CellProofs, len(bundle.Proofs))
						for j, proof := range bundle.Proofs {
							require.Equal(t, hexutil.Bytes(proof[:]), blob.CellProofs[j])
						}
					}
				})
			}
		})
	}
}

func TestSSZRESTBlobsPoolDisabled(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), true)
	body, err := encodeHashListRequest([]common.Hash{common.HexToHash("0x01")}, maxGetBlobHashes)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/blobs/v1", "", body))
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
		{errCaplinEnabled, http.StatusInternalServerError, problemInternal},
		{fmt.Errorf("engine: %w", errCaplinEnabled), http.StatusInternalServerError, problemInternal},
	} {
		rec := httptest.NewRecorder()
		writeEngineProblem(rec, tc.err)
		require.Equal(t, tc.code, rec.Code)
		require.Equal(t, tc.problem, problemType(t, rec))
	}
}

func TestEncodeBuiltPayloadIgnoresExecutionRequestsBeforeElectra(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.CapellaVersion, clparams.DenebVersion} {
		t.Run(version.String(), func(t *testing.T) {
			resp := &engine_types.GetPayloadResponse{
				ExecutionPayload:  engine_types.NewExecutionPayloadSSZ(version),
				ExecutionRequests: []hexutil.Bytes{{0xff, 0x00}},
			}

			encoded, err := encodeBuiltPayload(resp, version)
			require.NoError(t, err)
			resp.ExecutionRequests = nil
			withoutRequests, err := encodeBuiltPayload(resp, version)
			require.NoError(t, err)
			require.Equal(t, withoutRequests, encoded)
		})
	}
}

func TestSSZRESTGetPayload(t *testing.T) {
	const payloadID uint64 = 0x0102030405060708
	request := make(hexutil.Bytes, 1+types.WithdrawalRequestDataLen)
	request[0], request[1] = types.WithdrawalRequestType, 0x42
	block := minimalPayloadBlock(1, types.FlatRequests{{Type: request[0], RequestData: request[1:]}})
	blockValue := uint256.NewInt(112233)
	module := &stubExecutionModule{
		getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
			require.Equal(t, payloadID, id)
			return execmodule.AssembledBlockResult{Block: block, BlockValue: blockValue}, nil
		},
	}
	srv := newProposingEngineServerForGetPayloadTests(module)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec,
		newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/payloads/0x0102030405060708", "prague", nil))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	response, err := decodeBuiltPayload(rec.Body.Bytes(), clparams.ElectraVersion)
	require.NoError(t, err)
	require.Equal(t, block.Block.Hash(), response.ExecutionPayload.BlockHash)
	require.Equal(t, hexutil.Uint64(1), response.ExecutionPayload.Timestamp)
	require.Equal(t, blockValue, (*uint256.Int)(response.BlockValue))
	require.Equal(t, []hexutil.Bytes{request}, response.ExecutionRequests)
}

func TestSSZRESTGetPayloadErrorsAreNotCached(t *testing.T) {
	const payloadID = "0x0102030405060708"
	for _, tc := range []struct {
		name      string
		fork      string
		payloadID string
		lookupErr error
		code      int
		problem   string
	}{
		{"missing fork header", "", payloadID, nil, http.StatusBadRequest, problemUnsupportedFork},
		{"unknown fork header", "unknown", payloadID, nil, http.StatusBadRequest, problemUnsupportedFork},
		{"malformed payload ID", "prague", "not-an-id", nil, http.StatusBadRequest, problemInvalidRequest},
		{"unknown payload", "prague", payloadID, nil, http.StatusNotFound, problemUnknownPayload},
		{"backend error", "prague", payloadID, errors.New("payload lookup failed"), http.StatusInternalServerError, problemInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &stubExecutionModule{
				getAssembledBlockFunc: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
					return execmodule.AssembledBlockResult{}, tc.lookupErr
				},
			}
			srv := newProposingEngineServerForGetPayloadTests(module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/payloads/"+tc.payloadID, tc.fork, nil))

			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			require.Equal(t, tc.problem, problemType(t, rec))
			response := rec.Result()
			defer response.Body.Close()
			require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
		})
	}
}

func TestSSZRESTGetPayloadForkMatching(t *testing.T) {
	for _, tc := range []struct {
		fork      string
		timestamp uint64
		code      int
	}{
		{"paris", 99, http.StatusOK},
		{"paris", 100, http.StatusBadRequest},
		{"shanghai", 99, http.StatusBadRequest},
		{"shanghai", 100, http.StatusOK},
		{"shanghai", 200, http.StatusBadRequest},
		{"cancun", 199, http.StatusBadRequest},
		{"cancun", 300, http.StatusBadRequest},
		{"prague", 299, http.StatusBadRequest},
		{"prague", 400, http.StatusBadRequest},
		{"osaka", 399, http.StatusBadRequest},
		{"osaka", 500, http.StatusBadRequest},
		{"amsterdam", 499, http.StatusBadRequest},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.fork, tc.timestamp), func(t *testing.T) {
			module := &stubExecutionModule{
				getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
					return execmodule.AssembledBlockResult{
						Block:      minimalPayloadBlock(tc.timestamp, types.FlatRequests{}),
						BlockValue: uint256.NewInt(0),
					}, nil
				},
			}
			cfg := allForksConfig()
			cfg.TerminalTotalDifficulty = uint256.NewInt(0)
			srv := newProposingEngineServerWithConfig(cfg, module)
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec,
				newSSZRequest(t.Context(), http.MethodGet, "/engine/v1/payloads/0x000000000000002a", tc.fork, nil))

			require.Equal(t, tc.code, rec.Code)
			httpResponse := rec.Result()
			defer httpResponse.Body.Close()
			require.Equal(t, "no-store", httpResponse.Header.Get("Cache-Control"))
			if tc.code == http.StatusBadRequest {
				require.Equal(t, problemUnsupportedFork, problemType(t, rec))
				return
			}
			version, ok := engineForkVersion(tc.fork)
			require.True(t, ok)
			response, err := decodeBuiltPayload(rec.Body.Bytes(), version)
			require.NoError(t, err)
			require.Equal(t, hexutil.Uint64(tc.timestamp), response.ExecutionPayload.Timestamp)
		})
	}
}

func TestExchangeCapabilitiesDropsLegacySSZEndpoints(t *testing.T) {
	srv := newTestEngineServer(&chain.Config{}, false)
	caps := srv.ExchangeCapabilities([]string{"engine_newPayloadV1"})
	require.Contains(t, caps, "engine_newPayloadV1")
	require.Contains(t, caps, "engine_getPayloadV6")
	require.Contains(t, caps, "engine_getBlobsV4")
	require.Contains(t, caps, "engine_getInclusionListV1")
	require.NotContains(t, caps, "engine_exchangeCapabilities")
	for _, capability := range caps {
		require.NotContains(t, capability, "/engine/v")
	}
}

func TestSSZRESTExecutionRequestsBound(t *testing.T) {
	// execution_requests is bounded at MAX_EXECUTION_REQUESTS_PER_PAYLOAD (256);
	// a longer list must be rejected on decode.
	mkRequests := func(n int) []hexutil.Bytes {
		reqs := make([]hexutil.Bytes, n)
		for i := range reqs {
			reqs[i] = hexutil.Bytes{byte(i)}
		}
		return reqs
	}
	payload := engine_types.NewExecutionPayloadSSZ(clparams.ElectraVersion)

	enc, err := encodeNewPayloadEnvelope(clparams.ElectraVersion, payload, common.Hash{}, mkRequests(256))
	require.NoError(t, err)
	_, _, out, err := decodeNewPayloadEnvelope(enc, clparams.ElectraVersion)
	require.NoError(t, err)
	require.Len(t, out, 256)

	enc, err = encodeNewPayloadEnvelope(clparams.ElectraVersion, payload, common.Hash{}, mkRequests(257))
	require.NoError(t, err)
	_, _, _, err = decodeNewPayloadEnvelope(enc, clparams.ElectraVersion)
	require.Error(t, err)
}

func TestSSZRESTReadBodyNonSizeError(t *testing.T) {
	// a non-size read error (truncated body / client disconnect) is 400, not 413
	srv := newTestEngineServer(allForksConfig(), false)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/engine/v1/forkchoice", iotest.ErrReader(errors.New("boom")))
	req.Header.Set("Content-Type", sszRestContentType)
	req.Header.Set("Eth-Execution-Version", "cancun")
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, problemInvalidRequest, problemType(t, rec))
}

func TestSSZRESTRejectsOversizedContentLengthBeforeRead(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), false)
	for _, tc := range []struct {
		name string
		path string
		fork string
		max  int64
	}{
		{"payload", "/engine/v1/payloads", "cancun", 64 << 20},
		{"forkchoice", "/engine/v1/forkchoice", "amsterdam", 1 << 10},
		{"bodies", "/engine/v1/bodies/hash", "cancun", 4 + sszMaxBodiesRequest*32},
		{"blobs", "/engine/v1/blobs/v1", "", 4 + maxGetBlobHashes*32},
		{"blob cells", "/engine/v1/blobs/v4", "", 4 + 16 + maxGetBlobHashes*32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, iotest.ErrReader(errors.New("body must not be read")))
			req.ContentLength = tc.max + 1
			req.Header.Set("Content-Type", sszRestContentType)
			if tc.fork != "" {
				req.Header.Set("Eth-Execution-Version", tc.fork)
			}
			rec := httptest.NewRecorder()

			srv.SSZRESTHandler().ServeHTTP(rec, req)

			require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
			require.Equal(t, problemRequestTooLarge, problemType(t, rec))
		})
	}
}

func TestSSZRESTRejectsOversizedStreamingBody(t *testing.T) {
	srv := newTestEngineServer(allForksConfig(), false)
	req := newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/forkchoice", "amsterdam",
		make([]byte, sszMaxForkchoiceRequestBody+1))
	req.ContentLength = -1
	rec := httptest.NewRecorder()

	srv.SSZRESTHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Equal(t, problemRequestTooLarge, problemType(t, rec))
}

// A maximal valid ForkchoiceUpdate — max withdrawals with every optional
// field set — must fit within sszMaxForkchoiceRequestBody.
func TestSSZRESTMaximalForkchoiceUpdateFitsBodyLimit(t *testing.T) {
	root := common.HexToHash("0x01")
	slot := hexutil.Uint64(1)
	targetGasLimit := hexutil.Uint64(30000000)
	withdrawals := make([]*types.Withdrawal, clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload)
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{Index: hexutil.Uint64(i), Validator: hexutil.Uint64(i), Address: common.Address{byte(i)}, Amount: hexutil.Uint64(i)}
	}
	attrs := &engine_types.PayloadAttributes{
		Timestamp:             500,
		PrevRandao:            common.HexToHash("0x02"),
		SuggestedFeeRecipient: common.HexToAddress("0x03"),
		Withdrawals:           withdrawals,
		ParentBeaconBlockRoot: &root,
		SlotNumber:            &slot,
		TargetGasLimit:        &targetGasLimit,
	}
	custody := bytes.Repeat([]byte{0xff}, sszCellBitvectorBytes)

	enc, err := encodeForkchoiceUpdate(clparams.GloasVersion, &engine_types.ForkChoiceState{}, attrs, custody)
	require.NoError(t, err)
	require.LessOrEqual(t, len(enc), sszMaxForkchoiceRequestBody)
}

func TestSSZRESTGetInclusionListRoute(t *testing.T) {
	txns := types.Transactions{types.NewTransaction(0, common.Address{1}, uint256.NewInt(0), 21_000, uint256.NewInt(1), nil)}
	srv := NewEngineServer(log.New(), &chain.Config{BogotaTime: common.NewUint64(0)}, &stubExecutionModule{
		inclusionListFunc: func(context.Context) (types.Transactions, error) { return txns, nil },
	}, nil, false, true, false, false, nil, nil, 0, 0)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/engine/v1/inclusion-list", nil)
	rec := httptest.NewRecorder()
	srv.SSZRESTHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
	var resp solid.TransactionsSSZ
	require.NoError(t, resp.DecodeSSZ(rec.Body.Bytes(), 0))
	want, err := types.MarshalTransactionsBinary(txns)
	require.NoError(t, err)
	require.Equal(t, []hexutil.Bytes{want[0]}, transactionsBytes(&resp))
}
