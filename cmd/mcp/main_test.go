package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestAutoDiscoverSkipsNonRPCPort(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not JSON-RPC", http.StatusOK)
	}))
	defer other.Close()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Method != "eth_blockNumber" {
			http.Error(w, "unexpected RPC request", http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": request.ID, "result": "0x2",
		}); err != nil {
			t.Errorf("write RPC response: %v", err)
		}
	}))
	defer rpcServer.Close()

	oldPorts := defaultRPCPorts
	defaultRPCPorts = []uint{
		uint(other.Listener.Addr().(*net.TCPAddr).Port),
		uint(rpcServer.Listener.Addr().(*net.TCPAddr).Port),
	}
	defer func() { defaultRPCPorts = oldPorts }()

	if got := autoDiscover(t.Context(), log.Root()); got != rpcServer.URL {
		t.Fatalf("autoDiscover() = %q, want %q", got, rpcServer.URL)
	}
}

func TestAutoDiscoverKeepsFallbackWhenNoRPCResponds(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not JSON-RPC", http.StatusOK)
	}))
	defer other.Close()

	oldPorts := defaultRPCPorts
	defaultRPCPorts = []uint{uint(other.Listener.Addr().(*net.TCPAddr).Port)}
	defer func() { defaultRPCPorts = oldPorts }()

	if got := autoDiscover(t.Context(), log.Root()); got != "" {
		t.Fatalf("autoDiscover() = %q, want empty result for default fallback", got)
	}
}
