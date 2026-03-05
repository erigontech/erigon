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

// sszErrorResponse writes a text/plain error response per execution-apis SSZ spec.
func sszErrorResponse(w http.ResponseWriter, code int, _ int, message string) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	w.Write([]byte(message)) //nolint:errcheck
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

// registerRoutes registers all SSZ-REST endpoint routes per execution-apis SSZ spec.
// Uses RESTful resource-oriented paths (POST /engine/v{N}/payloads for newPayload,
// GET /engine/v{N}/payloads/{id} for getPayload, etc.)
func (s *SszRestServer) registerRoutes(mux *http.ServeMux) {
	// newPayload: POST /engine/v{N}/payloads
	mux.HandleFunc("POST /engine/v1/payloads", s.handleNewPayloadV1)
	mux.HandleFunc("POST /engine/v2/payloads", s.handleNewPayloadV2)
	mux.HandleFunc("POST /engine/v3/payloads", s.handleNewPayloadV3)
	mux.HandleFunc("POST /engine/v4/payloads", s.handleNewPayloadV4)
	mux.HandleFunc("POST /engine/v5/payloads", s.handleNewPayloadV5)

	// getPayload: GET /engine/v{N}/payloads/{payload_id}
	mux.HandleFunc("GET /engine/v1/payloads/", s.handleGetPayloadV1)
	mux.HandleFunc("GET /engine/v2/payloads/", s.handleGetPayloadV2)
	mux.HandleFunc("GET /engine/v3/payloads/", s.handleGetPayloadV3)
	mux.HandleFunc("GET /engine/v4/payloads/", s.handleGetPayloadV4)
	mux.HandleFunc("GET /engine/v5/payloads/", s.handleGetPayloadV5)
	mux.HandleFunc("GET /engine/v6/payloads/", s.handleGetPayloadV6)

	// forkchoiceUpdated: POST /engine/v{N}/forkchoice
	mux.HandleFunc("POST /engine/v1/forkchoice", s.handleForkchoiceUpdatedV1)
	mux.HandleFunc("POST /engine/v2/forkchoice", s.handleForkchoiceUpdatedV2)
	mux.HandleFunc("POST /engine/v3/forkchoice", s.handleForkchoiceUpdatedV3)

	// getBlobs: POST /engine/v{N}/blobs
	mux.HandleFunc("POST /engine/v1/blobs", s.handleGetBlobsV1)

	// capabilities: POST /engine/v1/capabilities
	mux.HandleFunc("POST /engine/v1/capabilities", s.handleExchangeCapabilities)

	// client version: POST /engine/v1/client/version
	mux.HandleFunc("POST /engine/v1/client/version", s.handleGetClientVersion)

	// Legacy paths (backwards compatibility with existing CL clients)
	mux.HandleFunc("POST /engine/v1/new_payload", s.handleNewPayloadV1)
	mux.HandleFunc("POST /engine/v2/new_payload", s.handleNewPayloadV2)
	mux.HandleFunc("POST /engine/v3/new_payload", s.handleNewPayloadV3)
	mux.HandleFunc("POST /engine/v4/new_payload", s.handleNewPayloadV4)
	mux.HandleFunc("POST /engine/v5/new_payload", s.handleNewPayloadV5)
	mux.HandleFunc("POST /engine/v1/forkchoice_updated", s.handleForkchoiceUpdatedV1)
	mux.HandleFunc("POST /engine/v2/forkchoice_updated", s.handleForkchoiceUpdatedV2)
	mux.HandleFunc("POST /engine/v3/forkchoice_updated", s.handleForkchoiceUpdatedV3)
	mux.HandleFunc("POST /engine/v1/get_payload", s.handleGetPayloadV1Legacy)
	mux.HandleFunc("POST /engine/v2/get_payload", s.handleGetPayloadV2Legacy)
	mux.HandleFunc("POST /engine/v3/get_payload", s.handleGetPayloadV3Legacy)
	mux.HandleFunc("POST /engine/v4/get_payload", s.handleGetPayloadV4Legacy)
	mux.HandleFunc("POST /engine/v5/get_payload", s.handleGetPayloadV5Legacy)
	mux.HandleFunc("POST /engine/v1/get_blobs", s.handleGetBlobsV1)
	mux.HandleFunc("POST /engine/v1/exchange_capabilities", s.handleExchangeCapabilities)
	mux.HandleFunc("POST /engine/v1/get_client_version", s.handleGetClientVersion)
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

	// SSZ Container layout per execution-apis spec:
	// Fixed: forkchoice_state(96) + payload_attributes_offset(4) = 100 bytes
	// Variable: List[PayloadAttributes, 1] (0 elements = no attributes, 1 element = attributes present)
	const fixedSize = 100

	if len(body) < fixedSize {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "request body too short for ForkchoiceUpdatedRequest")
		return
	}

	// Decode ForkchoiceState (first 96 bytes)
	fcs, err := engine_types.DecodeForkchoiceState(body[:96])
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, err.Error())
		return
	}

	var payloadAttributes *engine_types.PayloadAttributes

	attrOffset := binary.LittleEndian.Uint32(body[96:100])
	if attrOffset < uint32(len(body)) {
		attrData := body[attrOffset:]
		if len(attrData) > 0 {
			// List[PayloadAttributes, 1]: since PayloadAttributes is variable-size,
			// the list data is offset(4) + element. Skip the 4-byte list item offset.
			if len(attrData) < 4 {
				sszErrorResponse(w, http.StatusBadRequest, -32602, "payload attributes list too short")
				return
			}
			pa, err := decodePayloadAttributesSSZ(attrData[4:], version)
			if err != nil {
				sszErrorResponse(w, http.StatusUnprocessableEntity, -32602, err.Error())
				return
			}
			payloadAttributes = pa
		}
		// Empty list = no attributes (payloadAttributes stays nil)
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

// --- getPayload handlers (GET with payload_id in URL path) ---

func (s *SszRestServer) handleGetPayloadV1(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 1)
}

func (s *SszRestServer) handleGetPayloadV2(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 2)
}

func (s *SszRestServer) handleGetPayloadV3(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 3)
}

func (s *SszRestServer) handleGetPayloadV4(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 4)
}

func (s *SszRestServer) handleGetPayloadV5(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 5)
}

func (s *SszRestServer) handleGetPayloadV6(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromPath(w, r, 6)
}

// handleGetPayloadFromPath handles GET /engine/v{N}/payloads/{payload_id}
// where payload_id is hex-encoded Bytes8 (e.g., 0x0000000000000001).
func (s *SszRestServer) handleGetPayloadFromPath(w http.ResponseWriter, r *http.Request, version int) {
	// Extract payload_id from URL path: /engine/v{N}/payloads/{payload_id}
	path := r.URL.Path
	// Find the last path segment
	lastSlash := len(path) - 1
	for lastSlash > 0 && path[lastSlash] != '/' {
		lastSlash--
	}
	payloadIdHex := path[lastSlash+1:]
	if payloadIdHex == "" {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "missing payload_id in URL path")
		return
	}

	payloadIdBytes, err := hexutil.Decode(payloadIdHex)
	if err != nil || len(payloadIdBytes) != 8 {
		sszErrorResponse(w, http.StatusBadRequest, -32602, fmt.Sprintf("invalid payload_id: %s", payloadIdHex))
		return
	}

	s.logger.Info("[SSZ-REST] Received GetPayload", "version", version, "payloadId", payloadIdHex)
	s.doGetPayload(w, r, version, hexutil.Bytes(payloadIdBytes))
}

// --- getPayload legacy handlers (POST with payload_id in body) ---

func (s *SszRestServer) handleGetPayloadV1Legacy(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromBody(w, r, 1)
}
func (s *SszRestServer) handleGetPayloadV2Legacy(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromBody(w, r, 2)
}
func (s *SszRestServer) handleGetPayloadV3Legacy(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromBody(w, r, 3)
}
func (s *SszRestServer) handleGetPayloadV4Legacy(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromBody(w, r, 4)
}
func (s *SszRestServer) handleGetPayloadV5Legacy(w http.ResponseWriter, r *http.Request) {
	s.handleGetPayloadFromBody(w, r, 5)
}

func (s *SszRestServer) handleGetPayloadFromBody(w http.ResponseWriter, r *http.Request, version int) {
	s.logger.Info("[SSZ-REST] Received GetPayload (legacy POST)", "version", version)

	body, err := readBody(r, 64)
	if err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "failed to read request body")
		return
	}
	if len(body) != 8 {
		sszErrorResponse(w, http.StatusBadRequest, -32602, fmt.Sprintf("expected 8 bytes for payload ID, got %d", len(body)))
		return
	}
	payloadIdBytes := make(hexutil.Bytes, 8)
	copy(payloadIdBytes, body)
	s.doGetPayload(w, r, version, payloadIdBytes)
}

func (s *SszRestServer) doGetPayload(w http.ResponseWriter, r *http.Request, version int, payloadIdBytes hexutil.Bytes) {
	ctx := r.Context()
	var err error

	switch version {
	case 1:
		result, err := s.engine.GetPayloadV1(ctx, payloadIdBytes)
		if err != nil {
			s.handleGetPayloadError(w, err)
			return
		}
		resp := &engine_types.GetPayloadResponse{ExecutionPayload: result}
		sszResponse(w, engine_types.EncodeGetPayloadResponseSSZ(resp, 1))
	case 2, 3, 4, 5, 6:
		var result *engine_types.GetPayloadResponse
		encodeVersion := version
		switch version {
		case 2:
			result, err = s.engine.GetPayloadV2(ctx, payloadIdBytes)
		case 3:
			result, err = s.engine.GetPayloadV3(ctx, payloadIdBytes)
		case 4:
			result, err = s.engine.GetPayloadV4(ctx, payloadIdBytes)
		case 5:
			result, err = s.engine.GetPayloadV5(ctx, payloadIdBytes)
			encodeVersion = 4
		case 6:
			result, err = s.engine.GetPayloadV5(ctx, payloadIdBytes) // TODO: GetPayloadV6 when available
			encodeVersion = 4
		}
		if err != nil {
			s.handleGetPayloadError(w, err)
			return
		}
		sszResponse(w, engine_types.EncodeGetPayloadResponseSSZ(result, encodeVersion))
	default:
		sszErrorResponse(w, http.StatusBadRequest, -32601, fmt.Sprintf("unsupported getPayload version: %d", version))
	}
}

// handleGetPayloadError returns 404 for unknown payload ID, otherwise delegates.
func (s *SszRestServer) handleGetPayloadError(w http.ResponseWriter, err error) {
	if err != nil && err.Error() == "unknown payload" {
		sszErrorResponse(w, http.StatusNotFound, -32001, "Unknown payload ID")
		return
	}
	s.handleEngineError(w, err)
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

// handleEngineError converts engine errors to appropriate HTTP error responses.
func (s *SszRestServer) handleEngineError(w http.ResponseWriter, err error) {
	s.logger.Warn("[SSZ-REST] Engine error", "err", err)
	switch e := err.(type) {
	case *rpc.InvalidParamsError:
		sszErrorResponse(w, http.StatusBadRequest, -32602, e.Message)
	case *rpc.UnsupportedForkError:
		sszErrorResponse(w, http.StatusBadRequest, -32000, e.Message)
	default:
		errMsg := err.Error()
		if errMsg == "invalid forkchoice state" {
			sszErrorResponse(w, http.StatusConflict, -32000, errMsg)
		} else if errMsg == "invalid payload attributes" {
			sszErrorResponse(w, http.StatusUnprocessableEntity, -32000, errMsg)
		} else {
			sszErrorResponse(w, http.StatusInternalServerError, -32603, errMsg)
		}
	}
}
