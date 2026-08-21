package sszql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const validQueryBody = `{"queries":[{"anchor":"execution_block","path":".transactions[0].to"}]}`

// Stands in for the JSON-RPC server, which answers on any path.
const fallbackStatus = http.StatusTeapot

// queryPattern mirrors the route registered in cmd/rpcdaemon/cli/config.go.
const (
	queryPattern              = "POST /eth/{version}/execution/{block_id}/query"
	queryTrailingSlashPattern = queryPattern + "/{$}"
)

func newTestMux() *http.ServeMux {
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(fallbackStatus)
	})
	mux := http.NewServeMux()
	mux.Handle("/", fallback)
	mux.Handle(queryPattern, SSZQueryHandler())
	mux.Handle(queryTrailingSlashPattern, SSZQueryHandler())
	return mux
}

func doRequest(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	newTestMux().ServeHTTP(rec, req)
	return rec
}

func TestRouteMatchesQueryEndpoint(t *testing.T) {
	for _, path := range []string{
		"/eth/v1/execution/123/query",
		"/eth/v6/execution/head/query",
		"/eth/v1/execution/123/query/",
	} {
		t.Run(path, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, path, validQueryBody)
			if rec.Code != http.StatusOK {
				t.Errorf("got status %d, want %d", rec.Code, http.StatusOK)
			}
		})
	}
}

func TestRouteFallsThroughToJSONRPC(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"root", "/"},
		{"two segments", "/eth/mainnet/query"},
		{"seven segments", "/eth/a/b/c/d/e/query"},
		{"too few segments", "/eth/v1/execution/query"},
		{"too many segments", "/eth/v1/execution/123/extra/query"},
		{"wrong root", "/beacon/v1/execution/123/query"},
		{"wrong domain", "/eth/v1/consensus/123/query"},
		{"wrong suffix", "/eth/v1/execution/123/prove"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, tt.path, validQueryBody)
			if rec.Code != fallbackStatus {
				t.Errorf("path %q: got status %d, want fallback to JSON-RPC (%d)", tt.path, rec.Code, fallbackStatus)
			}
		})
	}
}

// Non-POST requests must reach the JSON-RPC server, which answers on any path.
// That is what keeps the GET health-check shortcut working on this path.
func TestRouteNonPostFallsThrough(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := doRequest(t, method, "/eth/v1/execution/123/query", validQueryBody)
		if rec.Code != fallbackStatus {
			t.Errorf("%s: got status %d, want fallback to JSON-RPC (%d)", method, rec.Code, fallbackStatus)
		}
	}
}

// Version segments strconv.Atoi would have accepted as aliases of a valid version,
// plus the out-of-range ones.
func TestRouteRejectsBogusVersion(t *testing.T) {
	for _, path := range []string{
		"/eth/v0/execution/123/query",
		"/eth/v7/execution/123/query",
		"/eth/vx/execution/123/query",
		"/eth/v+1/execution/123/query",
		"/eth/v-1/execution/123/query",
		"/eth/v01/execution/123/query",
		"/eth/v0001/execution/123/query",
		"/eth/v256/execution/123/query",
	} {
		t.Run(path, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, path, validQueryBody)
			if rec.Code != http.StatusNotFound {
				t.Errorf("got status %d, want %d", rec.Code, http.StatusNotFound)
			}
		})
	}
}

func TestRouteInvalidJSON(t *testing.T) {
	for _, body := range []string{
		"",
		"not json",
		`{"queries":`,
		validQueryBody + `{"junk":1}`,
		validQueryBody + `[1,2]`,
		validQueryBody + `null`,
		validQueryBody + `garbage`,
		`null`,
		`{}`,
		`{"queries":[]}`,
		`{"queries":null}`,
		`[]`,
		`123`,
		`{"queries":[{"path":".a"}],"typo_field":1}`,
	} {
		rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: got status %d, want %d", body, rec.Code, http.StatusBadRequest)
		}
	}
}

// Trailing whitespace is not junk; a client appending a newline must still be served.
func TestRouteTrailingWhitespaceAccepted(t *testing.T) {
	for _, body := range []string{validQueryBody + "\n", validQueryBody + "   ", validQueryBody + " \t\n"} {
		rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body)
		if rec.Code != http.StatusOK {
			t.Errorf("body %q: got status %d, want %d", body, rec.Code, http.StatusOK)
		}
	}
}

func TestRouteValidResponse(t *testing.T) {
	rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", validQueryBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != sszQLContentType {
		t.Errorf("Content-Type: got %q, want %q", ct, sszQLContentType)
	}

	var res SSZQLResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if len(res.Paths) != 1 || res.Paths[0] != ".transactions[0].to" {
		t.Errorf("Paths: got %v, want [.transactions[0].to]", res.Paths)
	}
	if len(res.Results) != 1 {
		t.Errorf("Results: got %d entries, want 1", len(res.Results))
	}
}
