// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

// HTTP routing for the Engine REST API under /engine/v1.
// See https://github.com/ethereum/execution-apis/blob/22e87b3c689f4fd1696baf40328d3c94c02275dd/src/engine/refactor.md.

package engineapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

const (
	sszRestContentType = "application/octet-stream"
	jsonContentType    = "application/json"
	problemContentType = "application/problem+json"
	engineForkHeader   = "Eth-Execution-Version"
)

// RFC 7807 problem types, written as relative URIs per the spec.
const (
	problemInvalidRequest       = "/engine-api/errors/invalid-request"
	problemSSZDecodeError       = "/engine-api/errors/ssz-decode-error"
	problemUnsupportedFork      = "/engine-api/errors/unsupported-fork"
	problemMethodNotFound       = "/engine-api/errors/method-not-found"
	problemUnknownPayload       = "/engine-api/errors/unknown-payload"
	problemInvalidForkchoice    = "/engine-api/errors/invalid-forkchoice"
	problemReorgTooDeep         = "/engine-api/errors/reorg-too-deep"
	problemRequestTooLarge      = "/engine-api/errors/request-too-large"
	problemUnsupportedMediaType = "/engine-api/errors/unsupported-media-type"
	problemInvalidBody          = "/engine-api/errors/invalid-body"
	problemInvalidAttributes    = "/engine-api/errors/invalid-attributes"
	problemInternal             = "/engine-api/errors/internal"
)

func (e *EngineServer) SSZRESTHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.handleSSZREST(w, r)
	})
}

func (e *EngineServer) handleSSZREST(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	// an empty last segment (e.g. a trailing slash) matches no route
	if len(parts) < 3 || parts[0] != "engine" || parts[1] != "v1" || parts[len(parts)-1] == "" {
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
		return
	}
	rest := parts[2:]
	switch rest[0] {
	case "capabilities":
		if len(rest) == 1 && r.Method == http.MethodGet {
			e.handleSSZCapabilities(w)
			return
		}
	case "identity":
		if len(rest) == 1 && r.Method == http.MethodGet {
			e.handleSSZIdentity(w, r)
			return
		}
	case "inclusion-list":
		if len(rest) == 1 && r.Method == http.MethodGet {
			e.handleSSZGetInclusionList(w, r)
			return
		}
	case "blobs":
		if len(rest) == 2 && r.Method == http.MethodPost {
			e.handleSSZGetBlobs(w, r, rest[1])
			return
		}
	case "payloads", "forkchoice", "bodies":
		e.handleSSZForkScoped(w, r, rest)
		return
	}
	writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
}

type sszForkRoute uint8

const (
	sszForkRouteNotFound sszForkRoute = iota
	sszForkRouteNewPayload
	sszForkRouteNewPayloadWithWitness
	sszForkRouteGetPayload
	sszForkRouteForkchoice
	sszForkRouteBodiesByHash
	sszForkRouteBodiesByRange
)

func matchSSZForkRoute(method string, rest []string) sszForkRoute {
	switch {
	case len(rest) == 1 && rest[0] == "payloads" && method == http.MethodPost:
		return sszForkRouteNewPayload
	case len(rest) == 2 && rest[0] == "payloads" && rest[1] == "witness" && method == http.MethodPost:
		return sszForkRouteNewPayloadWithWitness
	case len(rest) == 2 && rest[0] == "payloads" && method == http.MethodGet:
		return sszForkRouteGetPayload
	case len(rest) == 1 && rest[0] == "forkchoice" && method == http.MethodPost:
		return sszForkRouteForkchoice
	case len(rest) == 2 && rest[0] == "bodies" && rest[1] == "hash" && method == http.MethodPost:
		return sszForkRouteBodiesByHash
	case len(rest) == 1 && rest[0] == "bodies" && method == http.MethodGet:
		return sszForkRouteBodiesByRange
	default:
		return sszForkRouteNotFound
	}
}

func (e *EngineServer) handleSSZForkScoped(w http.ResponseWriter, r *http.Request, rest []string) {
	route := matchSSZForkRoute(r.Method, rest)
	if route == sszForkRouteNotFound {
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
		return
	}
	if route == sszForkRouteGetPayload {
		// The caching rule applies to errors as well as successful payloads.
		w.Header().Set("Cache-Control", "no-store")
	}

	forkName := r.Header.Get(engineForkHeader)
	version, ok := engineForkVersion(forkName)
	if !ok {
		writeProblem(w, http.StatusBadRequest, problemUnsupportedFork, fmt.Sprintf("unsupported %s %q", engineForkHeader, forkName))
		return
	}
	switch route {
	case sszForkRouteNewPayload:
		e.handleSSZNewPayload(w, r, forkName, version)
	case sszForkRouteNewPayloadWithWitness:
		e.handleSSZNewPayloadWithWitness(w, r, forkName, version)
	case sszForkRouteGetPayload:
		e.handleSSZGetPayload(w, r, forkName, version, rest[1])
	case sszForkRouteForkchoice:
		e.handleSSZForkchoice(w, r, forkName, version)
	case sszForkRouteBodiesByHash:
		e.handleSSZBodiesByHash(w, r, forkName, version)
	case sszForkRouteBodiesByRange:
		e.handleSSZBodiesByRange(w, r, forkName, version)
	}
}

func writeProblem(w http.ResponseWriter, status int, problemType, detail string) {
	w.Header().Set("Content-Type", problemContentType)
	w.WriteHeader(status)
	problem := struct {
		Type   string `json:"type"`
		Detail string `json:"detail,omitempty"`
	}{Type: problemType, Detail: detail}
	if err := json.NewEncoder(w).Encode(problem); err != nil {
		log.Debug("[SSZ-REST] failed to write problem response", "err", err)
	}
}

func (e *EngineServer) writeSSZDecodeProblem(w http.ResponseWriter, r *http.Request, err error) {
	e.logger.Debug("[SSZ-REST] failed to decode request", "path", r.URL.Path, "fork", r.Header.Get(engineForkHeader), "err", err)
	writeProblem(w, http.StatusBadRequest, problemSSZDecodeError, "")
}

func writeEngineProblem(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	// The Caplin guard uses an unsupported-fork RPC code for a server configuration error.
	if errors.Is(err, errCaplinEnabled) {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	if rpcErr, ok := errors.AsType[rpc.Error](err); ok {
		switch rpcErr.ErrorCode() {
		case engine_helpers.UnknownPayloadErr.Code:
			writeProblem(w, http.StatusNotFound, problemUnknownPayload, err.Error())
			return
		case engine_helpers.InvalidForkchoiceStateErr.Code:
			writeProblem(w, http.StatusConflict, problemInvalidForkchoice, err.Error())
			return
		case engine_helpers.InvalidPayloadAttributesErr.Code:
			writeProblem(w, http.StatusUnprocessableEntity, problemInvalidAttributes, err.Error())
			return
		case engine_helpers.TooLargeRequestErr.Code:
			writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge, err.Error())
			return
		case engine_helpers.ReorgTooDeepErr.Code:
			writeProblem(w, http.StatusConflict, problemReorgTooDeep, err.Error())
			return
		case (&rpc.UnsupportedForkError{}).ErrorCode():
			writeProblem(w, http.StatusBadRequest, problemUnsupportedFork, err.Error())
			return
		case (&rpc.InvalidParamsError{}).ErrorCode():
			writeProblem(w, http.StatusUnprocessableEntity, problemInvalidBody, err.Error())
			return
		}
	}
	writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
}

func (e *EngineServer) checkSSZForkTimestamp(w http.ResponseWriter, forkName string, timestamp uint64) bool {
	if timestampFork := forkNameAtTime(e.config, timestamp); timestampFork != forkName {
		writeProblem(w, http.StatusBadRequest, problemUnsupportedFork,
			fmt.Sprintf("payload timestamp belongs to fork %q, not header fork %q", timestampFork, forkName))
		return false
	}
	return true
}

func checkSSZContentType(w http.ResponseWriter, r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != sszRestContentType {
		writeProblem(w, http.StatusUnsupportedMediaType, problemUnsupportedMediaType, "want "+sszRestContentType)
		return false
	}
	return true
}

func readSSZBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	defer r.Body.Close()
	if r.ContentLength > maxBytes {
		writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge,
			fmt.Sprintf("content length %d exceeds limit %d", r.ContentLength, maxBytes))
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge, err.Error())
		} else {
			writeProblem(w, http.StatusBadRequest, problemInvalidRequest, err.Error())
		}
		return nil, false
	}
	return body, true
}

func writeSSZ(w http.ResponseWriter, obj interface {
	EncodeSSZ([]byte) ([]byte, error)
},
) {
	out, err := obj.EncodeSSZ(nil)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	writeSSZBytes(w, out)
}

func writeSSZBytes(w http.ResponseWriter, out []byte) {
	w.Header().Set("Content-Type", sszRestContentType)
	_, _ = w.Write(out)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", jsonContentType)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Debug("[SSZ-REST] failed to write JSON response", "err", err)
	}
}

func (e *EngineServer) handleSSZNewPayload(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	status, ok := e.sszNewPayload(w, r, forkName, version, nil)
	if !ok {
		return
	}
	e.logger.Debug("[SSZ-REST] handled new payload", "fork", forkName)
	writeSSZ(w, status)
}

func (e *EngineServer) handleSSZNewPayloadWithWitness(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if version < clparams.GloasVersion {
		writeProblem(w, http.StatusBadRequest, problemUnsupportedFork, fmt.Sprintf("payloads/witness is not served for %q", forkName))
		return
	}
	var witness *jsonrpc.ExecutionWitnessResult
	var witnessErr error
	status, ok := e.sszNewPayload(w, r, forkName, version, func(blockHash common.Hash) {
		witness, witnessErr = e.payloadWitness(r.Context(), blockHash)
	})
	if !ok {
		return
	}
	if witnessErr != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, witnessErr.Error())
		return
	}
	out, err := encodePayloadStatusWithWitness(status, witness)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Debug("[SSZ-REST] handled new payload with witness", "fork", forkName)
	writeSSZBytes(w, out)
}

func (e *EngineServer) payloadWitness(ctx context.Context, blockHash common.Hash) (*jsonrpc.ExecutionWitnessResult, error) {
	if witness, ok := e.payloadWitnesses.Get(blockHash); ok {
		return witness, nil
	}
	// Without the payload's state held or its changesets kept, only commitment history can rebuild the witness.
	mode := "canonical"
	witness, err := e.witnessBuilder.ExecutionWitness(ctx, rpc.BlockNumberOrHashWithHash(blockHash, true), &mode)
	if err != nil {
		return nil, fmt.Errorf("payload witness unavailable: the payload's state is no longer held: %w", err)
	}
	e.payloadWitnesses.AddPayloadWitness(blockHash, witness)
	return witness, nil
}

// sszNewPayload returns false once it has written an error response.
func (e *EngineServer) sszNewPayload(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion, onValid func(blockHash common.Hash)) (*engine_types.PayloadStatus, bool) {
	if !checkSSZContentType(w, r) {
		return nil, false
	}
	body, ok := readSSZBody(w, r, sszMaxPayloadRequestBody)
	if !ok {
		return nil, false
	}
	payload, parentRoot, executionRequests, err := decodeNewPayloadEnvelope(body, version)
	if err != nil {
		e.writeSSZDecodeProblem(w, r, err)
		return nil, false
	}
	if !e.checkSSZForkTimestamp(w, forkName, uint64(payload.Timestamp)) {
		return nil, false
	}
	var status *engine_types.PayloadStatus
	switch version {
	case clparams.BellatrixVersion:
		status, err = e.NewPayloadV1(r.Context(), payload)
	case clparams.CapellaVersion:
		status, err = e.NewPayloadV2(r.Context(), payload)
	case clparams.DenebVersion:
		status, err = e.newPayload(r.Context(), payload, nil, &parentRoot, nil, version, true, onValid)
	default:
		status, err = e.newPayload(r.Context(), payload, nil, &parentRoot, executionRequests, version, true, onValid)
	}
	if err != nil {
		writeEngineProblem(w, err)
		return nil, false
	}
	return status, true
}

func (e *EngineServer) handleSSZGetPayload(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion, payloadID string) {
	decodedID, err := parsePayloadIDPath(payloadID)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidRequest, err.Error())
		return
	}
	resp, err := e.getPayload(r.Context(), decodedID, version)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	if !e.checkSSZForkTimestamp(w, forkName, uint64(resp.ExecutionPayload.Timestamp)) {
		return
	}
	out, err := encodeBuiltPayload(resp, version)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Info("[SSZ-REST] handled get payload", "fork", forkName, "payloadId", decodedID)
	writeSSZBytes(w, out)
}

func parsePayloadIDPath(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s) != 16 {
		return 0, errors.New("invalid payload ID length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return engine_types.DecodePayloadID(b)
}

func (e *EngineServer) handleSSZForkchoice(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r, sszMaxForkchoiceRequestBody)
	if !ok {
		return
	}
	state, attrs, custodyColumns, err := decodeForkchoiceUpdate(body, version)
	if err != nil {
		e.writeSSZDecodeProblem(w, r, err)
		return
	}
	// Custody columns are decoded and validated, but are not applied: the current
	// blob backend stores full blobs and does not track custody.
	if custodyColumns != nil {
		e.logger.Debug("[SSZ-REST] ignoring custody columns update", "fork", forkName)
	}
	// Without payload attributes, forkchoice may refer to a head from another fork.
	if attrs != nil && !e.checkSSZForkTimestamp(w, forkName, uint64(attrs.Timestamp)) {
		return
	}
	resp, err := e.forkchoiceUpdated(r.Context(), &state, attrs, version)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	out, err := encodeForkchoiceResponse(resp)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Debug("[SSZ-REST] handled forkchoice", "fork", forkName)
	writeSSZBytes(w, out)
}

func (e *EngineServer) getSSZBodies(ctx context.Context, headers []*types.Header, forkName string) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	entries := make([]*engine_types.ExecutionPayloadBodyV2, len(headers))
	matchingHashes := make([]common.Hash, 0, len(headers))
	responsePositions := make([]int, 0, len(headers))
	// Filter by fork before fetching bodies: retrieval can re-execute blocks
	// to recover missing block access lists.
	for i, header := range headers {
		if header == nil || !misc.IsPoSHeader(header) || forkNameAtTime(e.config, header.Time) != forkName {
			continue
		}
		// Pin each body to the header used for fork filtering, even if the canonical
		// chain changes between reads.
		matchingHashes = append(matchingHashes, header.Hash())
		responsePositions = append(responsePositions, i)
	}
	if len(matchingHashes) == 0 {
		return entries, nil
	}
	bodies, err := e.chainRW.GetPayloadBodiesByHash(ctx, matchingHashes)
	if err != nil {
		return nil, err
	}
	for i, position := range responsePositions {
		entries[position] = bodies[i]
	}
	return entries, nil
}

func (e *EngineServer) handleSSZBodiesByHash(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r, sszMaxBodiesRequestBody)
	if !ok {
		return
	}
	blockHashes, err := decodeHashListRequest(body, sszMaxBodiesRequest)
	if err != nil {
		e.writeSSZDecodeProblem(w, r, err)
		return
	}
	headers := make([]*types.Header, len(blockHashes))
	for i, hash := range blockHashes {
		if e.isKnownBadBlock(hash) {
			continue
		}
		header, err := e.executionService.GetHeader(r.Context(), &hash, nil)
		if err != nil {
			writeEngineProblem(w, err)
			return
		}
		headers[i] = header
	}
	entries, err := e.getSSZBodies(r.Context(), headers, forkName)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	out, err := encodeBodiesResponse(entries, version)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Debug("[SSZ-REST] handled bodies by hash", "fork", forkName, "count", len(blockHashes))
	writeSSZBytes(w, out)
}

func (e *EngineServer) handleSSZBodiesByRange(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	query := r.URL.Query()
	from, errFrom := strconv.ParseUint(query.Get("from"), 10, 64)
	count, errCount := strconv.ParseUint(query.Get("count"), 10, 64)
	if errFrom != nil || errCount != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidRequest, "from and count query parameters are required")
		return
	}
	if count > sszMaxBodiesRequest {
		writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge,
			fmt.Sprintf("count %d exceeds the limit of %d", count, sszMaxBodiesRequest))
		return
	}
	if from == 0 || count == 0 {
		writeProblem(w, http.StatusUnprocessableEntity, problemInvalidBody,
			fmt.Sprintf("invalid from or count, from: %d count: %d", from, count))
		return
	}
	headers, err := e.executionService.GetHeadersByRange(r.Context(), from, count)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	entries, err := e.getSSZBodies(r.Context(), headers, forkName)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	out, err := encodeBodiesResponse(entries, version)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Debug("[SSZ-REST] handled bodies by range", "fork", forkName, "from", from, "count", count)
	writeSSZBytes(w, out)
}

func (e *EngineServer) handleSSZGetBlobs(w http.ResponseWriter, r *http.Request, revision string) {
	if revision == "v4" {
		e.handleSSZGetBlobsV4(w, r)
		return
	}
	if revision != "v1" && revision != "v2" && revision != "v3" {
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
		return
	}
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r, sszMaxBlobsRequestBody)
	if !ok {
		return
	}
	blobHashes, err := decodeHashListRequest(body, maxGetBlobHashes)
	if err != nil {
		e.writeSSZDecodeProblem(w, r, err)
		return
	}

	var out []byte
	switch revision {
	case "v1":
		resp, err := e.GetBlobsV1(r.Context(), blobHashes)
		if err != nil {
			writeBlobsProblem(w, err)
			return
		}
		if resp == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		out, err = encodeBlobsV1Response(resp)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
			return
		}
	case "v2", "v3":
		var resp engine_types.BlobsBundleV2
		var err error
		if revision == "v2" {
			resp, err = e.GetBlobsV2(r.Context(), blobHashes)
		} else {
			resp, err = e.GetBlobsV3(r.Context(), blobHashes)
		}
		if err != nil {
			writeBlobsProblem(w, err)
			return
		}
		// A nil response means the request cannot be served. For v2, this also
		// covers a missing blob because v2 is all-or-nothing.
		if resp == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		out, err = encodeBlobsV2Response(resp)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
			return
		}
	}
	e.logger.Debug("[SSZ-REST] handled get blobs", "revision", revision, "count", len(blobHashes))
	writeSSZBytes(w, out)
}

func (e *EngineServer) handleSSZGetBlobsV4(w http.ResponseWriter, r *http.Request) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r, sszMaxBlobsRequestBody+sszCellBitvectorBytes)
	if !ok {
		return
	}
	hashes, cellMask, err := decodeBlobsV4Request(body)
	if err != nil {
		e.writeSSZDecodeProblem(w, r, err)
		return
	}
	resp, err := e.GetBlobsV4(r.Context(), hashes, cellMask[:])
	if err != nil {
		writeBlobsProblem(w, err)
		return
	}
	if resp == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	out, err := encodeBlobsV4Response(resp, cellMask)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	writeSSZBytes(w, out)
}

func writeBlobsProblem(w http.ResponseWriter, err error) {
	if errors.Is(err, txpool.ErrPoolDisabled) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeEngineProblem(w, err)
}

type sszRestCapabilities struct {
	SupportedForks         []string            `json:"supported_forks"`
	ForkScopedEndpoints    []string            `json:"fork_scoped_endpoints"`
	IndependentlyVersioned map[string][]string `json:"independently_versioned"`
	UnscopedEndpoints      []string            `json:"unscoped_endpoints"`
	Limits                 map[string]uint64   `json:"limits"`
}

func (e *EngineServer) handleSSZCapabilities(w http.ResponseWriter) {
	writeJSON(w, sszRestCapabilities{
		SupportedForks:         engineForkOrder,
		ForkScopedEndpoints:    []string{"payloads", "payloads/witness", "forkchoice", "bodies"},
		IndependentlyVersioned: map[string][]string{"blobs": {"v1", "v2", "v3", "v4"}},
		UnscopedEndpoints:      []string{"capabilities", "identity", "inclusion-list"},
		Limits: map[string]uint64{
			"bodies.max_count":           sszMaxBodiesRequest,
			"blobs.max_versioned_hashes": maxGetBlobHashes,
			"payload.max_bytes":          sszMaxPayloadRequestBody,
		},
	})
}

func (e *EngineServer) handleSSZIdentity(w http.ResponseWriter, r *http.Request) {
	if clientVersion := r.Header.Get("X-Engine-Client-Version"); clientVersion != "" {
		e.logger.Info("[SSZ-REST] client version", "version", clientVersion)
	}
	versions, err := e.GetClientVersionV1(r.Context(), nil)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	writeJSON(w, versions)
}

// handleSSZGetInclusionList returns a bare SSZ transaction list without a container wrapper.
func (e *EngineServer) handleSSZGetInclusionList(w http.ResponseWriter, r *http.Request) {
	resp, err := e.GetInclusionListV1(r.Context())
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	e.logger.Debug("[SSZ-REST] handled get inclusion list", "path", r.URL.Path)
	writeSSZ(w, newTransactionsSSZ(resp))
}
