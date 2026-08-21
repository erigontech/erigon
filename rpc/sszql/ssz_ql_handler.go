package sszql

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

const sszQLContentType = "application/json"

const (
	queryPattern              = "POST /eth/{version}/execution/{block_id}/query"
	queryTrailingSlashPattern = queryPattern + "/{$}"
)

func RegisterHandlers(mux *http.ServeMux, fallback http.Handler) {
	h := &queryHandler{fallback: fallback}
	mux.Handle(queryPattern, h)
	mux.Handle(queryTrailingSlashPattern, h)
}

type queryHandler struct {
	fallback http.Handler
}

func (h *queryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version, ok := parseVersion(r.PathValue("version"))
	if !ok {
		h.fallback.ServeHTTP(w, r)
		return
	}

	block_id := r.PathValue("block_id")

	if !isValidBlockAndVersion(block_id, version) {
		http.NotFound(w, r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req SSZQLRequest

	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	res := parseQuery(req, version, block_id)

	writeQueryResponse(w, res)
}

func parseVersion(segment string) (int, bool) {
	if !strings.HasPrefix(segment, "v") {
		return 0, false
	}
	version, err := strconv.Atoi(strings.TrimPrefix(segment, "v"))
	if err != nil {
		return 0, false
	}
	return version, true
}

// TODO: Implement valid block_id checks with its version
func isValidBlockAndVersion(block_id string, version int) bool {
	if version < 1 || version > 6 {
		return false
	}

	return true
}

func writeQueryResponse(w http.ResponseWriter, res SSZQLResponse) {
	b, err := json.Marshal(res)
	if err != nil {
		http.Error(w, "invalid response: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", sszQLContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(b, '\n'))
}
