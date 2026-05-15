// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
)

const sszRestContentType = "application/octet-stream"

func (e *EngineServer) SSZRESTHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.handleSSZREST(w, r)
	})
}

func (e *EngineServer) handleSSZREST(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[0] != "engine" || !strings.HasPrefix(parts[1], "v") {
		http.NotFound(w, nil)
		return
	}
	version, err := strconv.Atoi(strings.TrimPrefix(parts[1], "v"))
	if err != nil {
		writeSSZError(w, http.StatusNotFound, "invalid engine version")
		return
	}

	switch {
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "payloads":
		e.handleSSZNewPayload(w, r, version)
	case r.Method == http.MethodGet && len(parts) == 4 && parts[2] == "payloads":
		e.handleSSZGetPayload(w, r, version, parts[3])
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "forkchoice":
		e.handleSSZForkchoice(w, r, version)
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "blobs":
		e.handleSSZGetBlobs(w, r, version)
	case r.Method == http.MethodPost && len(parts) == 4 && parts[2] == "client" && parts[3] == "version" && version == 1:
		e.handleSSZClientVersion(w, r)
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "capabilities" && version == 1:
		e.handleSSZCapabilities(w, r)
	default:
		writeSSZError(w, http.StatusNotFound, "unknown SSZ-REST Engine API endpoint")
	}
}

func sszNewPayloadVersion(version int) (clparams.StateVersion, bool) {
	switch version {
	case 1:
		return clparams.BellatrixVersion, true
	case 2:
		return clparams.CapellaVersion, true
	case 3:
		return clparams.DenebVersion, true
	case 4:
		return clparams.ElectraVersion, true
	case 5:
		return clparams.GloasVersion, true
	default:
		return 0, false
	}
}

func sszGetPayloadVersion(version int) (clparams.StateVersion, bool) {
	switch version {
	case 1:
		return clparams.BellatrixVersion, true
	case 2:
		return clparams.CapellaVersion, true
	case 3:
		return clparams.DenebVersion, true
	case 4:
		return clparams.ElectraVersion, true
	case 5:
		return clparams.FuluVersion, true
	case 6:
		return clparams.GloasVersion, true
	default:
		return 0, false
	}
}

func sszForkchoiceVersion(version int) (clparams.StateVersion, bool) {
	switch version {
	case 1:
		return clparams.BellatrixVersion, true
	case 2:
		return clparams.CapellaVersion, true
	case 3:
		return clparams.DenebVersion, true
	case 4:
		return clparams.GloasVersion, true
	default:
		return 0, false
	}
}

func readSSZBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(http.MaxBytesReader(nil, r.Body, 128<<20))
}

func writeSSZ(w http.ResponseWriter, obj interface {
	EncodeSSZ([]byte) ([]byte, error)
}) {
	out, err := obj.EncodeSSZ(nil)
	if err != nil {
		writeSSZError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", sszRestContentType)
	_, _ = w.Write(out)
}

func writeSSZError(w http.ResponseWriter, code int, msg string) {
	http.Error(w, msg, code)
}

func writeEngineError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		switch rpcErr.ErrorCode() {
		case engine_helpers.UnknownPayloadErr.Code:
			writeSSZError(w, http.StatusNotFound, err.Error())
			return
		case engine_helpers.InvalidForkchoiceStateErr.Code:
			writeSSZError(w, http.StatusConflict, err.Error())
			return
		case engine_helpers.InvalidPayloadAttributesErr.Code:
			writeSSZError(w, http.StatusUnprocessableEntity, err.Error())
			return
		case engine_helpers.TooLargeRequestErr.Code:
			writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
	}
	writeSSZError(w, http.StatusInternalServerError, err.Error())
}

func (e *EngineServer) handleSSZNewPayload(w http.ResponseWriter, r *http.Request, version int) {
	if version < 1 || version > 5 {
		writeSSZError(w, http.StatusNotFound, "unsupported new payload version")
		return
	}
	sv, _ := sszNewPayloadVersion(version)
	body, err := readSSZBody(r)
	if err != nil {
		writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	req := newSSZNewPayloadRequest(sv)
	if err := req.DecodeSSZ(body, int(sv)); err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	payload := req.Payload
	var status *engine_types.PayloadStatus
	switch version {
	case 1:
		status, err = e.NewPayloadV1(r.Context(), payload)
	case 2:
		status, err = e.NewPayloadV2(r.Context(), payload)
	case 3:
		root := req.ParentBeaconBlockRoot
		status, err = e.NewPayloadV3(r.Context(), payload, hashListValues(req.ExpectedBlobHashes), &root)
	case 4:
		root := req.ParentBeaconBlockRoot
		status, err = e.NewPayloadV4(r.Context(), payload, hashListValues(req.ExpectedBlobHashes), &root, req.ExecutionRequests.bytes())
	case 5:
		root := req.ParentBeaconBlockRoot
		status, err = e.NewPayloadV5(r.Context(), payload, hashListValues(req.ExpectedBlobHashes), &root, req.ExecutionRequests.bytes())
	}
	if err != nil {
		writeEngineError(w, err)
		return
	}
	e.logger.Info("[SSZ-REST] handled new payload", "path", r.URL.Path)
	writeSSZ(w, status)
}

func (e *EngineServer) handleSSZGetPayload(w http.ResponseWriter, r *http.Request, version int, payloadID string) {
	if version < 1 || version > 6 {
		writeSSZError(w, http.StatusNotFound, "unsupported get payload version")
		return
	}
	id, err := parsePayloadIDPath(payloadID)
	if err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch version {
	case 1:
		resp, err := e.GetPayloadV1(r.Context(), id)
		if err != nil {
			writeEngineError(w, err)
			return
		}
		resp.SSZVersion = clparams.BellatrixVersion
		e.logger.Info("[SSZ-REST] handled get payload", "path", r.URL.Path)
		writeSSZ(w, resp)
	default:
		resp, err := callGetPayload(r.Context(), e, version, id)
		if err != nil {
			writeEngineError(w, err)
			return
		}
		sv, _ := sszGetPayloadVersion(version)
		out, err := newSSZGetPayloadResponse(resp, sv)
		if err != nil {
			writeSSZError(w, http.StatusInternalServerError, err.Error())
			return
		}
		e.logger.Info("[SSZ-REST] handled get payload", "path", r.URL.Path)
		writeSSZ(w, out)
	}
}

func callGetPayload(ctx context.Context, e *EngineServer, version int, id hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	switch version {
	case 2:
		return e.GetPayloadV2(ctx, id)
	case 3:
		return e.GetPayloadV3(ctx, id)
	case 4:
		return e.GetPayloadV4(ctx, id)
	case 5:
		return e.GetPayloadV5(ctx, id)
	case 6:
		return e.GetPayloadV6(ctx, id)
	default:
		return nil, &rpc.UnsupportedForkError{Message: "unsupported get payload version"}
	}
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

func (e *EngineServer) handleSSZForkchoice(w http.ResponseWriter, r *http.Request, version int) {
	if version < 1 || version > 4 {
		writeSSZError(w, http.StatusNotFound, "unsupported forkchoice version")
		return
	}
	sv, _ := sszForkchoiceVersion(version)
	body, err := readSSZBody(r)
	if err != nil {
		writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	req := &sszForkchoiceRequest{version: sv}
	if err := req.DecodeSSZ(body, int(sv)); err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	var resp *engine_types.ForkChoiceUpdatedResponse
	switch version {
	case 1:
		resp, err = e.ForkchoiceUpdatedV1(r.Context(), &req.State, req.payloadAttributes())
	case 2:
		resp, err = e.ForkchoiceUpdatedV2(r.Context(), &req.State, req.payloadAttributes())
	case 3:
		resp, err = e.ForkchoiceUpdatedV3(r.Context(), &req.State, req.payloadAttributes())
	case 4:
		resp, err = e.ForkchoiceUpdatedV4(r.Context(), &req.State, req.payloadAttributes())
	}
	if err != nil {
		writeEngineError(w, err)
		return
	}
	out, err := forkchoiceResponseToSSZ(resp)
	if err != nil {
		writeSSZError(w, http.StatusInternalServerError, err.Error())
		return
	}
	e.logger.Info("[SSZ-REST] handled forkchoice", "path", r.URL.Path)
	writeSSZ(w, out)
}

func (e *EngineServer) handleSSZGetBlobs(w http.ResponseWriter, r *http.Request, version int) {
	if version < 1 || version > 3 {
		writeSSZError(w, http.StatusNotFound, "unsupported get blobs version")
		return
	}
	body, err := readSSZBody(r)
	if err != nil {
		writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	hashes := solid.NewHashList(sszMaxGetBlobHashes)
	if err := hashes.DecodeSSZ(body, 0); err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	if hashes.Length() > sszMaxGetBlobHashes {
		writeSSZError(w, http.StatusRequestEntityTooLarge, "too many blob hashes")
		return
	}
	switch version {
	case 1:
		resp, err := e.GetBlobsV1(r.Context(), hashListValues(hashes))
		if err != nil {
			writeEngineError(w, err)
			return
		}
		e.logger.Info("[SSZ-REST] handled get blobs", "path", r.URL.Path)
		writeSSZ(w, newSSZGetBlobsV1Response(resp))
		return
	case 2:
		resp, err := e.GetBlobsV2(r.Context(), hashListValues(hashes))
		if err != nil {
			writeEngineError(w, err)
			return
		}
		e.logger.Info("[SSZ-REST] handled get blobs", "path", r.URL.Path)
		writeSSZ(w, newSSZGetBlobsV2Response(resp))
		return
	case 3:
		resp, err := e.GetBlobsV3(r.Context(), hashListValues(hashes))
		if err != nil {
			writeEngineError(w, err)
			return
		}
		e.logger.Info("[SSZ-REST] handled get blobs", "path", r.URL.Path)
		writeSSZ(w, newSSZGetBlobsV3Response(resp))
		return
	}
}

func (e *EngineServer) handleSSZClientVersion(w http.ResponseWriter, r *http.Request) {
	body, err := readSSZBody(r)
	if err != nil {
		writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	req := &sszClientVersionRequest{}
	if err := req.DecodeSSZ(body, 0); err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := e.GetClientVersionV1(r.Context(), req.ClientVersion)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	e.logger.Info("[SSZ-REST] handled client version", "path", r.URL.Path)
	writeSSZ(w, newSSZClientVersionResponse(resp))
}

func ptr[T any](v T) *T { return &v }

func (e *EngineServer) handleSSZCapabilities(w http.ResponseWriter, r *http.Request) {
	body, err := readSSZBody(r)
	if err != nil {
		writeSSZError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	req := &sszCapabilities{}
	if err := req.DecodeSSZ(body, 0); err != nil {
		writeSSZError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp := e.ExchangeCapabilities(req.names())
	e.logger.Info("[SSZ-REST] handled capabilities", "path", r.URL.Path)
	writeSSZ(w, newSSZCapabilities(resp))
}

func hashesToCommon(in []common.Hash) []common.Hash { return in }
