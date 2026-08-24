package sszql

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
)

const sszQLContentType = "application/json"

var blockIDPattern = regexp.MustCompile(`^(?:latest|earliest|safe|finalized|pending|0x[0-9a-fA-F]{64}|0|[1-9][0-9]*)$`)
var errInvalidBlockID = errors.New("invalid block_id")

func SSZQueryHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleSSZQuery(w, r)
	})
}

func handleSSZQuery(w http.ResponseWriter, r *http.Request) {

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

	if !isValidBlockAndVersion(blockID, version) {
		writeQueryError(w, http.StatusNotFound, "invalid version segment")
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

	bnh, err := parseBlockIDs(blockID)
	if err != nil {
		writeQueryError(w, http.StatusNotFound, err.Error())
		return
	}

	var res SSZQLResponse

	switch version {
	case 1:
		res, err = parseQueryV1(req, version, bnh)
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

func isValidBlockAndVersion(blockID string, version uint) bool {
	if version < 1 || version > 6 {
		return false
	}

	return true
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

func parseBlockIDs(blockID string) (rpc.BlockNumberOrHash, error) {

	if !blockIDPattern.MatchString(blockID) {
		return rpc.BlockNumberOrHash{}, errInvalidBlockID
	}

	switch blockID {
	case "latest":
		return rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), nil
	case "earliest":
		return rpc.BlockNumberOrHashWithNumber(rpc.EarliestBlockNumber), nil
	case "safe":
		return rpc.BlockNumberOrHashWithNumber(rpc.SafeBlockNumber), nil
	case "finalized":
		return rpc.BlockNumberOrHashWithNumber(rpc.FinalizedBlockNumber), nil
	case "pending":
		return rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber), nil
	}

	if len(blockID) == 66 {
		return rpc.BlockNumberOrHashWithHash(common.HexToHash(blockID), false), nil
	}

	n, err := strconv.ParseUint(blockID, 10, 63)
	if err != nil {
		return rpc.BlockNumberOrHash{}, errInvalidBlockID
	}
	return rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(n)), nil
}
