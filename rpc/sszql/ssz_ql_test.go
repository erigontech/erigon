package sszql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doRequest(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	SSZQueryHandler().ServeHTTP(rec, req)
	return rec
}

const validQueryBody = `{"queries":[{"anchor":"execution_block","path":".transactions[0].to"}]}`

func TestRouteMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := doRequest(t, method, "/eth/v1/execution/123/query", validQueryBody)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: got status %d, want %d", method, rec.Code, http.StatusMethodNotAllowed)
		}
	}
}

func TestRoutePathMatching(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		status int
	}{
		{"valid v1", "/eth/v1/execution/123/query", http.StatusOK},
		{"valid v6", "/eth/v6/execution/head/query", http.StatusOK},
		{"trailing slash tolerated", "/eth/v1/execution/123/query/", http.StatusOK},
		{"too few segments", "/eth/v1/execution/query", http.StatusNotFound},
		{"too many segments", "/eth/v1/execution/123/extra/query", http.StatusNotFound},
		{"wrong root", "/beacon/v1/execution/123/query", http.StatusNotFound},
		{"missing v prefix", "/eth/1/execution/123/query", http.StatusNotFound},
		{"wrong domain", "/eth/v1/consensus/123/query", http.StatusNotFound},
		{"wrong suffix", "/eth/v1/execution/123/prove", http.StatusNotFound},
		{"non-numeric version", "/eth/vx/execution/123/query", http.StatusNotFound},
		{"version too low", "/eth/v0/execution/123/query", http.StatusNotFound},
		{"version too high", "/eth/v7/execution/123/query", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, tt.path, validQueryBody)
			if rec.Code != tt.status {
				t.Errorf("path %q: got status %d, want %d", tt.path, rec.Code, tt.status)
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
