// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

// HTTP routing for the Engine API v2 REST transport
// (https://github.com/ethereum/execution-apis/pull/793).

package engineapi

import (
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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

const (
	sszRestContentType = "application/octet-stream"
	jsonContentType    = "application/json"
	problemContentType = "application/problem+json"
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
	// trailing slashes are forbidden, so an empty last segment is method-not-found
	if len(parts) < 3 || parts[0] != "engine" || parts[1] != "v2" || parts[len(parts)-1] == "" {
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
		return
	}
	rest := parts[2:]
	switch {
	case rest[0] == "capabilities":
		if len(rest) == 1 && r.Method == http.MethodGet {
			e.handleSSZCapabilities(w)
			return
		}
	case rest[0] == "identity":
		if len(rest) == 1 && r.Method == http.MethodGet {
			e.handleSSZIdentity(w, r)
			return
		}
	case rest[0] == "blobs":
		if len(rest) == 2 && r.Method == http.MethodPost {
			e.handleSSZGetBlobs(w, r, rest[1])
			return
		}
	default:
		e.handleSSZForkScoped(w, r, rest)
		return
	}
	writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
}

func (e *EngineServer) handleSSZForkScoped(w http.ResponseWriter, r *http.Request, rest []string) {
	forkName := rest[0]
	version, ok := engineForkVersion(forkName)
	if !ok || !forkScheduled(e.config, forkName) {
		writeProblem(w, http.StatusBadRequest, problemUnsupportedFork, fmt.Sprintf("unsupported fork %q", forkName))
		return
	}
	sub := rest[1:]
	switch {
	case len(sub) == 1 && sub[0] == "payloads" && r.Method == http.MethodPost:
		e.handleSSZNewPayload(w, r, forkName, version)
	case len(sub) == 2 && sub[0] == "payloads" && r.Method == http.MethodGet:
		e.handleSSZGetPayload(w, r, forkName, version, sub[1])
	case len(sub) == 1 && sub[0] == "forkchoice" && r.Method == http.MethodPost:
		e.handleSSZForkchoice(w, r, forkName, version)
	case len(sub) == 2 && sub[0] == "bodies" && sub[1] == "hash" && r.Method == http.MethodPost:
		e.handleSSZBodiesByHash(w, r, forkName, version)
	case len(sub) == 1 && sub[0] == "bodies" && r.Method == http.MethodGet:
		e.handleSSZBodiesByRange(w, r, forkName, version)
	default:
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
	}
}

func writeProblem(w http.ResponseWriter, status int, problemType, detail string) {
	w.Header().Set("Content-Type", problemContentType)
	w.WriteHeader(status)
	problem := struct {
		Type   string `json:"type"`
		Detail string `json:"detail,omitempty"`
	}{Type: problemType, Detail: detail}
	_ = json.NewEncoder(w).Encode(problem)
}

func writeEngineProblem(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
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

func checkSSZContentType(w http.ResponseWriter, r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != sszRestContentType {
		writeProblem(w, http.StatusUnsupportedMediaType, problemUnsupportedMediaType, "want "+sszRestContentType)
		return false
	}
	return true
}

func readSSZBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, sszMaxRequestBody))
	if err != nil {
		writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge, err.Error())
		return nil, false
	}
	return body, true
}

func writeSSZ(w http.ResponseWriter, obj interface {
	EncodeSSZ([]byte) ([]byte, error)
}) {
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
	_ = json.NewEncoder(w).Encode(v)
}

func (e *EngineServer) handleSSZNewPayload(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r)
	if !ok {
		return
	}
	payload, parentRoot, executionRequests, err := decodeNewPayloadEnvelope(body, version)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemSSZDecodeError, "")
		return
	}
	var status *engine_types.PayloadStatus
	switch version {
	case clparams.BellatrixVersion:
		status, err = e.NewPayloadV1(r.Context(), payload)
	case clparams.CapellaVersion:
		status, err = e.NewPayloadV2(r.Context(), payload)
	case clparams.DenebVersion:
		status, err = e.NewPayloadV3(r.Context(), payload, blobVersionedHashesFromTxs(payload.Transactions), &parentRoot)
	case clparams.ElectraVersion, clparams.FuluVersion:
		status, err = e.NewPayloadV4(r.Context(), payload, blobVersionedHashesFromTxs(payload.Transactions), &parentRoot, executionRequests)
	default:
		status, err = e.NewPayloadV5(r.Context(), payload, blobVersionedHashesFromTxs(payload.Transactions), &parentRoot, executionRequests)
	}
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	e.logger.Debug("[SSZ-REST] handled new payload", "fork", forkName)
	writeSSZ(w, status)
}

func (e *EngineServer) handleSSZGetPayload(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion, payloadID string) {
	id, err := parsePayloadIDPath(payloadID)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidRequest, err.Error())
		return
	}
	decodedID, err := decodePayloadID(id)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidRequest, err.Error())
		return
	}
	resp, err := e.getPayload(r.Context(), decodedID, version)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	out, err := encodeBuiltPayload(resp, version)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternal, err.Error())
		return
	}
	e.logger.Debug("[SSZ-REST] handled get payload", "fork", forkName, "payloadId", decodedID)
	w.Header().Set("Cache-Control", "no-store")
	writeSSZBytes(w, out)
}

func parsePayloadIDPath(s string) (hexutil.Bytes, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s) != 16 {
		return nil, errors.New("invalid payload ID length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return hexutil.Bytes(b), nil
}

func (e *EngineServer) handleSSZForkchoice(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r)
	if !ok {
		return
	}
	state, attrs, custodyColumns, err := decodeForkchoiceUpdate(body, version)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemSSZDecodeError, "")
		return
	}
	if custodyColumns != nil {
		e.logger.Debug("[SSZ-REST] ignoring custody columns update", "fork", forkName)
	}
	if attrs != nil {
		// the URL fork is load-bearing for payload builds only: it must match
		// the fork the new payload would belong to
		if attrsFork := forkNameAtTime(e.config, uint64(attrs.Timestamp)); attrsFork != forkName {
			writeProblem(w, http.StatusBadRequest, problemUnsupportedFork,
				fmt.Sprintf("payload attributes timestamp belongs to fork %q, not URL fork %q", attrsFork, forkName))
			return
		}
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

func (e *EngineServer) handleSSZBodiesByHash(w http.ResponseWriter, r *http.Request, forkName string, version clparams.StateVersion) {
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r)
	if !ok {
		return
	}
	if len(body) > sszMaxBodiesRequest*32 {
		writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge, "too many block hashes")
		return
	}
	hashes := solid.NewHashList(sszMaxBodiesRequest)
	if err := hashes.DecodeSSZ(body, 0); err != nil {
		writeProblem(w, http.StatusBadRequest, problemSSZDecodeError, "")
		return
	}
	blockHashes := hashListValues(hashes)
	bodies, err := e.chainRW.GetPayloadBodiesByHash(r.Context(), blockHashes)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	// blocks outside the URL fork's time range come back as available=false
	entries := make([]*engine_types.ExecutionPayloadBodyV2, len(blockHashes))
	for i := range blockHashes {
		if i >= len(bodies) || bodies[i] == nil {
			continue
		}
		header := e.chainRW.GetHeaderByHash(r.Context(), blockHashes[i])
		if header == nil || forkNameAtTime(e.config, header.Time) != forkName {
			continue
		}
		entries[i] = bodies[i]
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
	bodies, err := e.chainRW.GetPayloadBodiesByRange(r.Context(), from, count)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	// blocks outside the URL fork's time range or past the latest known block
	// come back as available=false
	entries := make([]*engine_types.ExecutionPayloadBodyV2, count)
	for i := uint64(0); i < count; i++ {
		if i >= uint64(len(bodies)) || bodies[i] == nil {
			continue
		}
		header := e.chainRW.GetHeaderByNumber(r.Context(), from+i)
		if header == nil || forkNameAtTime(e.config, header.Time) != forkName {
			continue
		}
		entries[i] = bodies[i]
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
	if revision != "v1" && revision != "v2" && revision != "v3" {
		writeProblem(w, http.StatusNotFound, problemMethodNotFound, "")
		return
	}
	if !checkSSZContentType(w, r) {
		return
	}
	body, ok := readSSZBody(w, r)
	if !ok {
		return
	}
	if len(body) > sszMaxGetBlobHashes*32 {
		writeProblem(w, http.StatusRequestEntityTooLarge, problemRequestTooLarge, "too many blob hashes")
		return
	}
	hashes := solid.NewHashList(sszMaxGetBlobHashes)
	if err := hashes.DecodeSSZ(body, 0); err != nil {
		writeProblem(w, http.StatusBadRequest, problemSSZDecodeError, "")
		return
	}
	blobHashes := hashListValues(hashes)

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
		// nil signals "cannot serve" (and the all-or-nothing miss on v2)
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
		SupportedForks:         supportedForkNames(e.config),
		ForkScopedEndpoints:    []string{"payloads", "forkchoice", "bodies"},
		IndependentlyVersioned: map[string][]string{"blobs": {"v1", "v2", "v3"}},
		UnscopedEndpoints:      []string{"capabilities", "identity"},
		Limits: map[string]uint64{
			"bodies.max_count":           sszMaxBodiesRequest,
			"blobs.max_versioned_hashes": sszMaxGetBlobHashes,
			"payload.max_bytes":          sszMaxRequestBody,
		},
	})
}

func (e *EngineServer) handleSSZIdentity(w http.ResponseWriter, r *http.Request) {
	if clientVersion := r.Header.Get("X-Engine-Client-Version"); clientVersion != "" {
		e.logger.Debug("[SSZ-REST] client version", "version", clientVersion)
	}
	versions, err := e.GetClientVersionV1(r.Context(), nil)
	if err != nil {
		writeEngineProblem(w, err)
		return
	}
	writeJSON(w, versions)
}
