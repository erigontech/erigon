package sszql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const validQueryBody = `{"queries":[{"anchor":"execution_block","path":".transactions[0].to"}]}`

const fallbackStatus = http.StatusTeapot

func doRequest(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(fallbackStatus)
	})
	mux := http.NewServeMux()
	mux.Handle("/", fallback)
	RegisterHandlers(mux, fallback)

	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
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
		name   string
		method string
		path   string
	}{
		{"root", http.MethodPost, "/"},
		{"two segments", http.MethodPost, "/eth/mainnet/query"},
		{"seven segments", http.MethodPost, "/eth/a/b/c/d/e/query"},
		{"too few segments", http.MethodPost, "/eth/v1/execution/query"},
		{"too many segments", http.MethodPost, "/eth/v1/execution/123/extra/query"},
		{"wrong root", http.MethodPost, "/beacon/v1/execution/123/query"},
		{"wrong domain", http.MethodPost, "/eth/v1/consensus/123/query"},
		{"wrong suffix", http.MethodPost, "/eth/v1/execution/123/prove"},
		{"missing v prefix", http.MethodPost, "/eth/1/execution/123/query"},
		{"non-numeric version", http.MethodPost, "/eth/vx/execution/123/query"},
		{"health check on query path", http.MethodGet, "/eth/v1/execution/123/query"},
		{"health check on near-miss path", http.MethodGet, "/eth/foo/query"},
		{"put on query path", http.MethodPut, "/eth/v1/execution/123/query"},
		{"delete on query path", http.MethodDelete, "/eth/v1/execution/123/query"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, tt.method, tt.path, validQueryBody)
			if rec.Code != fallbackStatus {
				t.Errorf("%s %q: got status %d, want fallback to JSON-RPC (%d)", tt.method, tt.path, rec.Code, fallbackStatus)
			}
		})
	}
}

func TestRouteUnsupportedVersion(t *testing.T) {
	for _, path := range []string{
		"/eth/v0/execution/123/query",
		"/eth/v7/execution/123/query",
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
	for _, body := range []string{"", "not json", `{"queries":`} {
		rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: got status %d, want %d", body, rec.Code, http.StatusBadRequest)
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
