package beaconhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestHandleEndpoint_SetsEthConsensusVersionHeaderFromBodyVersion(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(map[string]any{"ok": true}).WithVersion(clparams.DenebVersion), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rr := httptest.NewRecorder()
	h(rr, req)

	if got := rr.Header().Get("Eth-Consensus-Version"); got != "deneb" {
		t.Fatalf("Eth-Consensus-Version = %q, want %q", got, "deneb")
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response json: %v", err)
	}
	if got := body["version"]; got != "deneb" {
		t.Fatalf("body.version = %#v, want %q", got, "deneb")
	}
}

func TestWillEncodeSSZ(t *testing.T) {
	tests := []struct {
		accept string
		want   bool
	}{
		{"", false},
		{"*/*", false},
		{"application/json", false},
		{"application/octet-stream", true},
		{"application/json, application/octet-stream", false},
		{"application/octet-stream, application/json", false},
		{"text/html", false},
		{"text/event-stream", false},
		{"application/octet-stream; q=1", true},
		{"text/html, application/octet-stream", false},
	}
	for _, tt := range tests {
		if got := WillEncodeSSZ(tt.accept); got != tt.want {
			t.Errorf("WillEncodeSSZ(%q) = %v, want %v", tt.accept, got, tt.want)
		}
	}
}

func TestHandleEndpoint_DoesNotOverrideEthConsensusVersionHeader(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(map[string]any{"ok": true}).
			WithHeader("Eth-Consensus-Version", "capella").
			WithVersion(clparams.DenebVersion), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rr := httptest.NewRecorder()
	h(rr, req)

	if got := rr.Header().Get("Eth-Consensus-Version"); got != "capella" {
		t.Fatalf("Eth-Consensus-Version = %q, want %q", got, "capella")
	}
}
