package beaconhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
)

type testSSZResponse struct{}

func (testSSZResponse) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, 1, 2, 3), nil
}

func (testSSZResponse) EncodingSizeSSZ() int {
	return 3
}

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

func TestHandleEndpoint_UsesSSZForPreferredWeightedAccept(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(testSSZResponse{}), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept", "application/octet-stream;q=1,application/json;q=0.9")
	rr := httptest.NewRecorder()
	h(rr, req)

	if got := rr.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/octet-stream")
	}
	if got := rr.Body.Bytes(); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("body = %v, want %v", got, []byte{1, 2, 3})
	}
}

func TestHandleEndpoint_UsesJSONForPreferredSSZAcceptWhenResponseDoesNotSupportSSZ(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(map[string]any{"ok": true}), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept", "application/octet-stream;q=1,application/json;q=0.9")
	rr := httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}

	var body map[string]map[string]bool
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response json: %v", err)
	}
	if !body["data"]["ok"] {
		t.Fatalf("body = %#v, want data.ok=true", body)
	}
}

func TestHandleEndpoint_RejectsSSZOnlyAcceptWhenResponseDoesNotSupportSSZ(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(map[string]any{"ok": true}), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept", "application/octet-stream")
	rr := httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}

	var body EndpointError
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response json: %v", err)
	}
	if body.Message != ErrorSszNotSupported.Error() {
		t.Fatalf("error = %#v, want %q", body, ErrorSszNotSupported.Error())
	}
}

func TestHandleEndpoint_UsesSSZWhenExactTypeBeatsWildcard(t *testing.T) {
	h := HandleEndpointFunc(func(w http.ResponseWriter, r *http.Request) (*BeaconResponse, error) {
		return NewBeaconResponse(testSSZResponse{}), nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept", "application/octet-stream, */*")
	rr := httptest.NewRecorder()
	h(rr, req)

	if got := rr.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/octet-stream")
	}
	if got := rr.Body.Bytes(); !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("body = %v, want %v", got, []byte{1, 2, 3})
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
		{"application/octet-stream;q=1,application/json;q=0.9", true},
		{"application/json;q=0.9,application/octet-stream;q=1", true},
		{"application/octet-stream;q=0.8,application/json;q=1", false},
		{"application/octet-stream;q=0,application/json;q=1", false},
		{"application/octet-stream;q=0", false},
		{"text/html", false},
		{"text/event-stream", false},
		{"application/octet-stream; q=1", true},
		{"text/html, application/octet-stream", false},
		{"application/octet-stream, */*", true},
		{"application/octet-stream;q=5", true},
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
