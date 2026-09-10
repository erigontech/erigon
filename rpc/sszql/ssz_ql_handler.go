package sszql

import (
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const sszQLContentType = "application/json"

var blockIDPattern = regexp.MustCompile(`^(?:latest|earliest|safe|finalized|0x[0-9a-fA-F]{64}|0|[1-9][0-9]*)$`)
var errInvalidBlockID = errors.New("invalid block_id")

func NewSSZQLAPI(db kv.RoDB, blockReader dbservices.FullBlockReader) *SSZQLImpl {
	return &SSZQLImpl{
		DB:          db,
		BlockReader: blockReader,
	}
}

func SSZQueryHandler(apis []rpc.API) http.Handler {
	var sszqlAPI SSZQLAPI

	for _, r := range apis {
		if r.Service == nil {
			continue
		}

		if sszqlCandidate, ok := r.Service.(SSZQLAPI); ok {
			sszqlAPI = sszqlCandidate
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleSSZQuery(sszqlAPI, w, r)
	})
}

func handleSSZQuery(api SSZQLAPI, w http.ResponseWriter, r *http.Request) {

	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != sszQLContentType {
		writeQueryError(w, http.StatusUnsupportedMediaType, "unsupported media type, only "+sszQLContentType+" is supported")
		return
	}

	segment := r.PathValue("version")
	if !strings.HasPrefix(segment, "v") {
		writeQueryError(w, http.StatusNotFound, "invalid version segment")
		return
	}

	v := strings.TrimPrefix(segment, "v")
	if len(v) > 1 && v[0] == '0' {
		writeQueryError(w, http.StatusNotFound, "invalid version segment")
		return
	}
	parsed, err := strconv.ParseUint(v, 10, 8)
	if err != nil {
		writeQueryError(w, http.StatusNotFound, "invalid version segment")
		return
	}
	version := uint(parsed)

	blockID := r.PathValue("blockID")

	block, err := parseBlockIDs(r.Context(), api, blockID)
	if err != nil {
		writeQueryError(w, http.StatusNotFound, err.Error())
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req SSZQLRequest

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeQueryError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if dec.More() {
		writeQueryError(w, http.StatusBadRequest, "invalid JSON: unexpected data after request body")
		return
	}
	if len(req.Queries) == 0 {
		writeQueryError(w, http.StatusBadRequest, "invalid JSON: queries must not be empty")
		return
	}

	var res SSZQLResponse

	switch version {
	case 1:
		res, err = parseQueryV1(api, req, version, block)
	default:
		writeQueryError(w, http.StatusNotFound, "unsupported API version")
		return
	}

	if err != nil {
		writeQueryError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeQueryResponse(w, res)
}

type queryError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeQueryError(w http.ResponseWriter, code int, message string) {
	b, err := json.Marshal(queryError{Code: code, Message: message})
	if err != nil {
		b = []byte(`{"code":500,"message":"internal error"}`)
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", sszQLContentType)
	w.WriteHeader(code)
	_, _ = w.Write(append(b, '\n'))
}

func writeQueryResponse(w http.ResponseWriter, res SSZQLResponse) {
	b, err := json.Marshal(res)
	if err != nil {
		writeQueryError(w, http.StatusInternalServerError, "invalid response: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", sszQLContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(b, '\n'))
}

func parseBlockIDs(ctx context.Context, api SSZQLAPI, blockID string) (*types.Block, error) {
	if !blockIDPattern.MatchString(blockID) {
		return nil, errInvalidBlockID
	}

	var bnh rpc.BlockNumberOrHash

	switch blockID {
	case "latest":
		bnh = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	case "earliest":
		bnh = rpc.BlockNumberOrHashWithNumber(rpc.EarliestBlockNumber)
	case "safe":
		bnh = rpc.BlockNumberOrHashWithNumber(rpc.SafeBlockNumber)
	case "finalized":
		bnh = rpc.BlockNumberOrHashWithNumber(rpc.FinalizedBlockNumber)
	}

	if len(blockID) == 66 {
		bnh = rpc.BlockNumberOrHashWithHash(common.HexToHash(blockID), false)
	}

	if bnh.BlockNumber == nil && bnh.BlockHash == nil {
		n, err := strconv.ParseUint(blockID, 10, 63)
		if err != nil {
			return nil, errInvalidBlockID
		}
		bnh = rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(n))
	}

	block, err := api.GetExecutionBlock(ctx, bnh)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (api *SSZQLImpl) GetExecutionBlock(ctx context.Context, bnh rpc.BlockNumberOrHash) (*types.Block, error) {

	tx, err := api.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	_, hash, _, err := rpchelper.GetBlockNumber(ctx, bnh, tx, api.BlockReader, nil)
	if err != nil {
		return nil, err
	}

	block, err := api.BlockReader.BlockByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}
