// Copyright 2025 The Erigon Authors
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

package engineapi

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// SszRestServer implements the EIP-8161 SSZ-REST Engine API transport.
// It runs alongside the JSON-RPC Engine API server and shares the same
// EngineServer for method dispatch.
type SszRestServer struct {
	engine    *EngineServer
	logger    log.Logger
	jwtSecret []byte
	addr      string
	port      int
	server    *http.Server
}

// NewSszRestServer creates a new SSZ-REST server.
func NewSszRestServer(engine *EngineServer, logger log.Logger, jwtSecret []byte, addr string, port int) *SszRestServer {
	return &SszRestServer{
		engine:    engine,
		logger:    logger,
		jwtSecret: jwtSecret,
		addr:      addr,
		port:      port,
	}
}

// sszErrorResponse writes a JSON error response for non-200 status codes per EIP-8161.
func sszErrorResponse(w http.ResponseWriter, code int, jsonRpcCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    jsonRpcCode,
		Message: message,
	}
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

// sszResponse writes a successful SSZ-encoded response.
func sszResponse(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(data) //nolint:errcheck
}

// Start starts the SSZ-REST HTTP server. It blocks until ctx is cancelled.
func (s *SszRestServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	handler := s.jwtMiddleware(mux)

	listenAddr := fmt.Sprintf("%s:%d", s.addr, s.port)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("SSZ-REST server failed to listen on %s: %w", listenAddr, err)
	}

	s.server = &http.Server{
		Handler: handler,
	}

	s.logger.Info("[SSZ-REST] Engine API server started", "addr", listenAddr)

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		s.server.Close()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// jwtMiddleware wraps an http.Handler with JWT authentication using the same
// secret and validation logic as the JSON-RPC Engine API (EIP-8161 requirement).
func (s *SszRestServer) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rpc.CheckJwtSecret(w, r, s.jwtSecret) {
			return // CheckJwtSecret already wrote the error response
		}
		// Recover from panics in handlers (e.g., nil pointer dereferences
		// when engine dependencies are not fully initialized)
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("[SSZ-REST] panic in handler", "panic", rec, "path", r.URL.Path)
				sszErrorResponse(w, http.StatusInternalServerError, -32603, fmt.Sprintf("internal error: %v", rec))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// registerRoutes registers all SSZ-REST endpoint routes per EIP-8161.
func (s *SszRestServer) registerRoutes(mux *http.ServeMux) {
	// newPayload versions
	mux.HandleFunc("POST /engine/v1/new_payload", s.handleNewPayloadV1)
	mux.HandleFunc("POST /engine/v2/new_payload", s.handleNewPayloadV2)
	mux.HandleFunc("POST /engine/v3/new_payload", s.handleNewPayloadV3)
	mux.HandleFunc("POST /engine/v4/new_payload", s.handleNewPayloadV4)
	mux.HandleFunc("POST /engine/v5/new_payload", s.handleNewPayloadV5)

	// forkchoiceUpdated versions
	mux.HandleFunc("POST /engine/v1/forkchoice_updated", s.handleForkchoiceUpdatedV1)
	mux.HandleFunc("POST /engine/v2/forkchoice_updated", s.handleForkchoiceUpdatedV2)
	mux.HandleFunc("POST /engine/v3/forkchoice_updated", s.handleForkchoiceUpdatedV3)

	// getPayload versions
	mux.HandleFunc("POST /engine/v1/get_payload", s.handleGetPayloadV1)
	mux.HandleFunc("POST /engine/v2/get_payload", s.handleGetPayloadV2)
	mux.HandleFunc("POST /engine/v3/get_payload", s.handleGetPayloadV3)
	mux.HandleFunc("POST /engine/v4/get_payload", s.handleGetPayloadV4)
	mux.HandleFunc("POST /engine/v5/get_payload", s.handleGetPayloadV5)

	// getBlobs
	mux.HandleFunc("POST /engine/v1/get_blobs", s.handleGetBlobsV1)

	// exchangeCapabilities
	mux.HandleFunc("POST /engine/v1/exchange_capabilities", s.handleExchangeCapabilities)

	// getClientVersion
	mux.HandleFunc("POST /engine/v1/get_client_version", s.handleGetClientVersion)

	// getClientCommunicationChannels (deprecated, kept for backward compat)
	mux.HandleFunc("POST /engine/v1/get_client_communication_channels", s.handleGetClientCommunicationChannels)

	// exchangeCapabilitiesV2 (EIP-8160)
	mux.HandleFunc("POST /engine/v2/exchange_capabilities", s.handleExchangeCapabilitiesV2)
}

// readBody reads the request body with a size limit.
func readBody(r *http.Request, maxSize int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, maxSize))
}

// --- newPayload handlers ---

func (s *SszRestServer) handleNewPayloadV1(w http.ResponseWriter, r *http.Request) {
	s.handleNewPayload(w, r, 1)
}

func (s *SszRestServer) handleNewPayloadV2(w http.ResponseWriter, r *http.Request) {
	s.handleNewPayload(w, r, 2)
}

func (s *SszRestServer) handleNewPayloadV3(w http.ResponseWriter, r *http.Request) {
	s.handleNewPayload(w, r, 3)
}

func (s *SszRestServer) handleNewPayloadV4(w http.ResponseWriter, r *http.Request) {
	s.handleNewPayload(w, r, 4)
}

func (s *SszRestServer) handleNewPayloadV5(w http.ResponseWriter, r *http.Request) {
	s.handleNewPayload(w, r, 5)
}

func (s *SszRestServer) handleNewPayload(w http.ResponseWriter, r *http.Request, version int) {
	s.logger.Info("[SSZ-REST] Received NewPayload", "version", version)

	body, err := readBody(r, 16*1024*1024) // 16 MB max
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	if len(body) == 0 {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "empty request body")
		return
	}

	// Decode the SSZ request: V1/V2 is just ExecutionPayload, V3/V4 is a wrapper container
	ep, blobHashes, parentBeaconBlockRoot, executionRequests, err := engine_types.DecodeNewPayloadRequestSSZ(body, version)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, fmt.Sprintf("SSZ decode error: %v", err))
		return
	}

	ctx := r.Context()
	var result *engine_types.PayloadStatus

	switch version {
	case 1:
		result, err = s.engine.NewPayloadV1(ctx, ep)
	case 2:
		result, err = s.engine.NewPayloadV2(ctx, ep)
	case 3:
		result, err = s.engine.NewPayloadV3(ctx, ep, blobHashes, parentBeaconBlockRoot)
	case 4, 5:
		// Determine the correct fork version from the payload timestamp.
		// The SSZ payload format is the same (Deneb) for V4/V5, but the engine
		// does a fork-version check internally.
		ts := uint64(ep.Timestamp)
		forkVersion := clparams.ElectraVersion
		if s.engine.config.IsAmsterdam(ts) {
			forkVersion = clparams.GloasVersion
		} else if s.engine.config.IsOsaka(ts) {
			forkVersion = clparams.FuluVersion
		} else if s.engine.config.IsPrague(ts) {
			forkVersion = clparams.ElectraVersion
		}
		s.logger.Info("[SSZ-REST] NewPayload fork check", "timestamp", ts, "forkVersion", forkVersion, "urlVersion", version)
		result, err = s.engine.newPayload(ctx, ep, blobHashes, parentBeaconBlockRoot, executionRequests, forkVersion)
	default:
		sszErrorResponse(w, http.StatusBadRequest, -32601, fmt.Sprintf("unsupported newPayload version: %d", version))
		return
	}

	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	// Encode PayloadStatus response
	ps := engine_types.PayloadStatusToSSZ(result)
	sszResponse(w, ps.EncodeSSZ())
}

// --- forkchoiceUpdated handlers ---

func (s *SszRestServer) handleForkchoiceUpdatedV1(w http.ResponseWriter, r *http.Request) {
	s.handleForkchoiceUpdated(w, r, 1)
}

func (s *SszRestServer) handleForkchoiceUpdatedV2(w http.ResponseWriter, r *http.Request) {
	s.handleForkchoiceUpdated(w, r, 2)
}

func (s *SszRestServer) handleForkchoiceUpdatedV3(w http.ResponseWriter, r *http.Request) {
	s.handleForkchoiceUpdated(w, r, 3)
}

func (s *SszRestServer) handleForkchoiceUpdated(w http.ResponseWriter, r *http.Request, version int) {
	s.logger.Info("[SSZ-REST] Received ForkchoiceUpdated", "version", version)

	body, err := readBody(r, 1024*1024) // 1 MB max
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	// SSZ Container layout:
	// Fixed: forkchoice_state(96) + attributes_offset(4) = 100 bytes
	// Variable: Union[None, PayloadAttributes]
	const fixedSize = 100

	if len(body) < 96 {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "request body too short for ForkchoiceState")
		return
	}

	// Decode ForkchoiceState (first 96 bytes)
	fcs, err := engine_types.DecodeForkchoiceState(body[:96])
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
		return
	}

	var payloadAttributes *engine_types.PayloadAttributes

	if len(body) >= fixedSize {
		attrOffset := binary.LittleEndian.Uint32(body[96:100])
		if attrOffset <= uint32(len(body)) && attrOffset < uint32(len(body)) {
			// Union data at attrOffset
			unionData := body[attrOffset:]
			if len(unionData) > 0 {
				selector := unionData[0]
				if selector == 1 && len(unionData) > 1 {
					pa, err := decodePayloadAttributesSSZ(unionData[1:], version)
					if err != nil {
						sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
						return
					}
					payloadAttributes = pa
				}
				// selector == 0 means None
			}
		}
	}

	ctx := r.Context()
	var resp *engine_types.ForkChoiceUpdatedResponse

	switch version {
	case 1:
		resp, err = s.engine.ForkchoiceUpdatedV1(ctx, fcs, payloadAttributes)
	case 2:
		resp, err = s.engine.ForkchoiceUpdatedV2(ctx, fcs, payloadAttributes)
	case 3:
		resp, err = s.engine.ForkchoiceUpdatedV3(ctx, fcs, payloadAttributes)
	default:
		sszErrorResponse(w, http.StatusBadRequest, -32601, fmt.Sprintf("unsupported forkchoiceUpdated version: %d", version))
		return
	}

	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	// Encode response
	if resp.PayloadId != nil {
		s.logger.Info("[SSZ-REST] ForkchoiceUpdated response", "payloadId", fmt.Sprintf("%x", []byte(*resp.PayloadId)), "status", resp.PayloadStatus.Status)
	} else {
		s.logger.Info("[SSZ-REST] ForkchoiceUpdated response", "payloadId", "nil", "status", resp.PayloadStatus.Status)
	}
	respBytes := engine_types.EncodeForkchoiceUpdatedResponse(resp)
	s.logger.Info("[SSZ-REST] ForkchoiceUpdated encoded", "len", len(respBytes), "first20", fmt.Sprintf("%x", respBytes[:min(20, len(respBytes))]))
	sszResponse(w, respBytes)
}

// decodePayloadAttributesSSZ decodes PayloadAttributes from SSZ bytes.
// The version determines the layout:
//   - V1 (Bellatrix): timestamp(8) + prev_randao(32) + fee_recipient(20) = 60 bytes fixed
//   - V2 (Capella): timestamp(8) + prev_randao(32) + fee_recipient(20) + withdrawals_offset(4) = 64 bytes fixed + withdrawals
//   - V3 (Deneb/Electra): same as V2 + parent_beacon_block_root(32) = 96 bytes fixed + withdrawals
func decodePayloadAttributesSSZ(buf []byte, version int) (*engine_types.PayloadAttributes, error) {
	if len(buf) < 60 {
		return nil, fmt.Errorf("PayloadAttributes: buffer too short (%d < 60)", len(buf))
	}

	timestamp := binary.LittleEndian.Uint64(buf[0:8])
	pa := &engine_types.PayloadAttributes{
		Timestamp: hexutil.Uint64(timestamp),
	}
	copy(pa.PrevRandao[:], buf[8:40])
	copy(pa.SuggestedFeeRecipient[:], buf[40:60])

	if version == 1 {
		return pa, nil
	}

	// V2+: has withdrawals_offset at byte 60
	if len(buf) < 64 {
		return nil, fmt.Errorf("PayloadAttributes V2+: buffer too short (%d < 64)", len(buf))
	}
	withdrawalsOffset := binary.LittleEndian.Uint32(buf[60:64])

	if version >= 3 {
		// V3: has parent_beacon_block_root at bytes 64-96
		if len(buf) < 96 {
			return nil, fmt.Errorf("PayloadAttributes V3: buffer too short (%d < 96)", len(buf))
		}
		root := common.BytesToHash(buf[64:96])
		pa.ParentBeaconBlockRoot = &root
	}

	// Decode withdrawals from the offset
	if withdrawalsOffset <= uint32(len(buf)) {
		wdBuf := buf[withdrawalsOffset:]
		if len(wdBuf) > 0 {
			// Each withdrawal = 44 bytes (index:8 + validator:8 + address:20 + amount:8)
			if len(wdBuf)%44 != 0 {
				return nil, fmt.Errorf("PayloadAttributes: withdrawals buffer length %d not divisible by 44", len(wdBuf))
			}
			count := len(wdBuf) / 44
			pa.Withdrawals = make([]*types.Withdrawal, count)
			for i := 0; i < count; i++ {
				off := i * 44
				w := &types.Withdrawal{
					Index:     binary.LittleEndian.Uint64(wdBuf[off : off+8]),
					Validator: binary.LittleEndian.Uint64(wdBuf[off+8 : off+16]),
					Amount:    binary.LittleEndian.Uint64(wdBuf[off+36 : off+44]),
				}
				copy(w.Address[:], wdBuf[off+16:off+36])
				pa.Withdrawals[i] = w
			}
		} else {
			pa.Withdrawals = []*types.Withdrawal{}
		}
	}

	return pa, nil
}

// --- getPayload handlers ---

func (s *SszRestServer) handleGetPayloadV1(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayload(w, r, 1)
}

func (s *SszRestServer) handleGetPayloadV2(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayload(w, r, 2)
}

func (s *SszRestServer) handleGetPayloadV3(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayload(w, r, 3)
}

func (s *SszRestServer) handleGetPayloadV4(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayload(w, r, 4)
}

func (s *SszRestServer) handleGetPayloadV5(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayload(w, r, 5)
}

func (s *SszRestServer) handleGetPayload(w http.ResponseWriter, r *http.Request, version int) {
	s.logger.Info("[SSZ-REST] Received GetPayload", "version", version)

	body, err := readBody(r, 64)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	if len(body) != 8 {
		sszErrorResponse(w, http.StatusBadRequest, -32602, fmt.Sprintf("expected 8 bytes for payload ID, got %d", len(body)))
		return
	}

	// Payload ID is 8 bytes. The Engine API internally uses big-endian payload IDs
	// (see ConvertPayloadId), so we pass the raw bytes directly.
	payloadIdBytes := make(hexutil.Bytes, 8)
	copy(payloadIdBytes, body)

	ctx := r.Context()

	switch version {
	case 1:
		result, err := s.engine.GetPayloadV1(ctx, payloadIdBytes)
		if err != nil {
			s.handleEngineError(w, err)
			return
		}
		resp := &engine_types.GetPayloadResponse{ExecutionPayload: result}
		sszResponse(w, engine_types.EncodeGetPayloadResponseSSZ(resp, 1))
	case 2, 3, 4, 5:
		var result *engine_types.GetPayloadResponse
		// For SSZ encoding, v5 (Fulu) uses same payload format as v4 (Electra/Deneb).
		encodeVersion := version
		switch version {
		case 2:
			result, err = s.engine.GetPayloadV2(ctx, payloadIdBytes)
		case 3:
			result, err = s.engine.GetPayloadV3(ctx, payloadIdBytes)
		case 4:
			result, err = s.engine.GetPayloadV4(ctx, payloadIdBytes)
		case 5:
			// Fulu uses same payload layout as Electra (Deneb format for SSZ encoding).
			result, err = s.engine.GetPayloadV5(ctx, payloadIdBytes)
			encodeVersion = 4
		}
		if err != nil {
			s.handleEngineError(w, err)
			return
		}
		sszResponse(w, engine_types.EncodeGetPayloadResponseSSZ(result, encodeVersion))
	default:
		sszErrorResponse(w, http.StatusBadRequest, -32601, fmt.Sprintf("unsupported getPayload version: %d", version))
	}
}

// --- getBlobs handler ---

func (s *SszRestServer) handleGetBlobsV1(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("[SSZ-REST] Received GetBlobsV1")

	body, err := readBody(r, 1024*1024) // 1 MB max
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	hashes, err := engine_types.DecodeGetBlobsRequest(body)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
		return
	}

	ctx := r.Context()
	result, err := s.engine.GetBlobsV1(ctx, hashes)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	// Encode blobs response: count(4) + for each blob: has_blob(1) + blob(131072) + proof(48)
	respBuf := encodeGetBlobsV1Response(result)
	sszResponse(w, respBuf)
}

// encodeGetBlobsV1Response encodes the GetBlobsV1 response as an SSZ Container.
// Layout: list_offset(4) + N * BlobAndProof (each 131120 bytes = blob:131072 + proof:48)
// Only non-nil blobs are included in the list.
func encodeGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) []byte {
	const blobAndProofSize = 131072 + 48 // blob + KZG proof

	// Count non-nil blobs
	var count int
	for _, b := range blobs {
		if b != nil {
			count++
		}
	}

	// SSZ Container with a single List field
	fixedSize := 4 // list_offset
	listSize := count * blobAndProofSize
	buf := make([]byte, fixedSize+listSize)

	// Offset to the list data
	binary.LittleEndian.PutUint32(buf[0:4], uint32(fixedSize))

	// Write each non-nil BlobAndProof as fixed-size items
	pos := fixedSize
	for _, b := range blobs {
		if b == nil {
			continue
		}
		// Blob (131072 bytes, zero-padded if shorter)
		copy(buf[pos:pos+131072], b.Blob)
		pos += 131072
		// Proof (48 bytes, zero-padded if shorter)
		copy(buf[pos:pos+48], b.Proof)
		pos += 48
	}

	return buf
}

// --- exchangeCapabilities handler ---

func (s *SszRestServer) handleExchangeCapabilities(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("[SSZ-REST] Received ExchangeCapabilities")

	body, err := readBody(r, 1024*1024)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	capabilities, err := engine_types.DecodeCapabilities(body)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
		return
	}

	result := s.engine.ExchangeCapabilities(capabilities)
	sszResponse(w, engine_types.EncodeCapabilities(result))
}

// --- getClientVersion handler ---

func (s *SszRestServer) handleGetClientVersion(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("[SSZ-REST] Received GetClientVersion")

	body, err := readBody(r, 1024*1024)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	var callerVersion *engine_types.ClientVersionV1
	if len(body) > 0 {
		cv, err := engine_types.DecodeClientVersion(body)
		if err != nil {
			sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
			return
		}
		callerVersion = cv
	}

	ctx := r.Context()
	result, err := s.engine.GetClientVersionV1(ctx, callerVersion)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	sszResponse(w, engine_types.EncodeClientVersions(result))
}

// --- exchangeCapabilitiesV2 handler (EIP-8160) ---

func (s *SszRestServer) handleExchangeCapabilitiesV2(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("[SSZ-REST] Received ExchangeCapabilitiesV2")

	body, err := readBody(r, 1024*1024)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}

	capabilities, err := engine_types.DecodeCapabilities(body)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
		return
	}

	result := s.engine.ExchangeCapabilitiesV2(capabilities)
	// Encode as: capabilities SSZ + communication channels SSZ appended
	// For simplicity, use the same capabilities encoding followed by channels encoding.
	capBuf := engine_types.EncodeCapabilities(result.Capabilities)
	chanBuf := engine_types.EncodeCommunicationChannels(result.SupportedProtocols)

	// SSZ Container: capabilities_offset(4) + channels_offset(4) + capabilities_data + channels_data
	fixedSize := uint32(8)
	buf := make([]byte, 8+len(capBuf)+len(chanBuf))
	binary.LittleEndian.PutUint32(buf[0:4], fixedSize)
	binary.LittleEndian.PutUint32(buf[4:8], fixedSize+uint32(len(capBuf)))
	copy(buf[8:], capBuf)
	copy(buf[8+len(capBuf):], chanBuf)

	sszResponse(w, buf)
}

// --- getClientCommunicationChannels handler ---

func (s *SszRestServer) handleGetClientCommunicationChannels(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("[SSZ-REST] Received GetClientCommunicationChannels")

	ctx := r.Context()
	result, err := s.engine.GetClientCommunicationChannelsV1(ctx)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	sszResponse(w, engine_types.EncodeCommunicationChannels(result))
}

// handleEngineError converts engine errors to appropriate HTTP error responses.
func (s *SszRestServer) handleEngineError(w http.ResponseWriter, err error) {
	s.logger.Warn("[SSZ-REST] Engine error", "err", err)
	switch e := err.(type) {
	case *rpc.InvalidParamsError:
		sszErrorResponse(w, http.StatusBadRequest, -32602, e.Message)
	case *rpc.UnsupportedForkError:
		sszErrorResponse(w, http.StatusBadRequest, -32000, e.Message)
	default:
		sszErrorResponse(w, http.StatusInternalServerError, -32603, err.Error())
	}
}
