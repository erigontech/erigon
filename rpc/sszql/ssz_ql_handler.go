package sszql

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

const sszQLContentType = "application/json"

func SSZQueryHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleSSZQuery(w, r)
	})
}

func handleSSZQuery(w http.ResponseWriter, r *http.Request) {

	segment := r.PathValue("version")
	if !strings.HasPrefix(segment, "v") {
		http.NotFound(w, nil)
		return
	}

	v := strings.TrimPrefix(segment, "v")
	if len(v) > 1 && v[0] == '0' {
		http.NotFound(w, nil)
		return
	}
	parsed, err := strconv.ParseUint(v, 10, 8)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	version := uint(parsed)

	block_id := r.PathValue("block_id")

	if !isValidBlockAndVersion(block_id, version) {
		http.NotFound(w, nil)
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

func isValidBlockAndVersion(block_id string, version uint) bool {
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
