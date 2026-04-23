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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
)

// SszRestServer implements the EIP-8161 SSZ-REST Engine API transport.
// Routes are registered on the same HTTP server as the JSON-RPC Engine API
// (path-based routing: /engine/* → SSZ-REST, / → JSON-RPC).
type SszRestServer struct {
	engine *EngineServer
	logger log.Logger
}

// NewSszRestHandler creates an http.Handler for SSZ-REST routes.
// JWT authentication is handled by the caller (the main engine API handler).
func NewSszRestHandler(engine *EngineServer, logger log.Logger) http.Handler {
	s := &SszRestServer{
		engine: engine,
		logger: logger,
	}
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	return mux
}

// sszErrorResponse writes a JSON error response per EIP-8161 spec.
func sszErrorResponse(w http.ResponseWriter, code int, jsonCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	body, _ := json.Marshal(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: jsonCode, Message: message})
	w.Write(body) //nolint:errcheck
}

// sszResponse writes a successful SSZ-encoded response.
func sszResponse(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(data) //nolint:errcheck
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
	ep, blobHashes, parentBeaconBlockRoot, executionRequests, err := engine_types.DecodeNewPayloadRequest(body, version)
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

	psBytes, _ := result.EncodeSSZ(nil)
	sszResponse(w, psBytes)
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

	const fixedSize = 100 // forkchoice_state(96) + payload_attributes_offset(4)
	if len(body) < fixedSize {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "request body too short for ForkchoiceUpdatedRequest")
		return
	}
	attrOffset := int(binary.LittleEndian.Uint32(body[96:]))
	if attrOffset < fixedSize || attrOffset > len(body) || (attrOffset < len(body) && len(body)-attrOffset < 4) {
		sszErrorResponse(w, http.StatusBadRequest, -32602, "invalid payload attributes list offset")
		return
	}

	fcs := &engine_types.ForkChoiceState{}
	payloadAttributesList := solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	if err := ssz2.UnmarshalSSZ(body, version, fcs, payloadAttributesList); err != nil {
		sszErrorResponse(w, http.StatusBadRequest, -32602, fmt.Sprintf("SSZ decode error: %v", err))
		return
	}
	var payloadAttributes *engine_types.PayloadAttributes
	if payloadAttributesList.Len() > 0 {
		payloadAttributes = payloadAttributesList.Get(0)
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
	if s.engine.txpool == nil {
		sszErrorResponse(w, http.StatusInternalServerError, -32603, "txpool unavailable")
		return
	}
	result, err := s.engine.GetBlobsV1(ctx, hashes)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	sszResponse(w, engine_types.EncodeGetBlobsV1Response(result))
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
