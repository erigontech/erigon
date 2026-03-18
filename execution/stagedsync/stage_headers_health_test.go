package stagedsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

func TestCheckL2RPCEndpointHealth_NilClient(t *testing.T) {
	err := checkL2RPCEndpointHealth(context.Background(), nil, 100, "")
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointHealth_Success(t *testing.T) {
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				},
			},
		},
		"arb_getRawBlockMetadata": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointHealth(context.Background(), client, 100, srv.URL)
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointHealth_BlockError(t *testing.T) {
	responses := map[string]interface{}{
		"eth_getBlockByNumber": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointHealth(context.Background(), client, 100, srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned nil for block")
}

func TestCheckL2RPCEndpointHealth_EmptyTransactions(t *testing.T) {
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number":       "0x64",
			"hash":         "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{},
		},
		"arb_getRawBlockMetadata": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointHealth(context.Background(), client, 100, srv.URL)
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointHealth_BlockMetadataMethodNotSupported(t *testing.T) {
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				},
			},
		},
		// arb_getRawBlockMetadata not registered — will return method not found error
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointHealth(context.Background(), client, 100, srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "arb_getRawBlockMetadata")
}

func newMockRPCServer(t *testing.T, responses map[string]interface{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string        `json:"method"`
			Params []interface{} `json:"params"`
			ID     interface{}   `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, ok := responses[req.Method]
		if !ok {
			resp := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error": map[string]interface{}{
					"code":    -32601,
					"message": "method not found",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		resp := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
}

// TestCheckL2RPCEndpointHealth_Integration tests against real RPC endpoints.
// Set environment variables to run:
//   - L2RPC: block RPC endpoint URL (required)
//   - L2RPC_BLOCK: block number to check (optional, defaults to 1)
//
// Example:
//
//	L2RPC=http://localhost:8547 go test -v -run TestCheckL2RPCEndpointHealth_Integration
func TestCheckL2RPCEndpointHealth_Integration(t *testing.T) {
	l2rpc := os.Getenv("L2RPC")
	if l2rpc == "" {
		t.Skip("L2RPC environment variable not set, skipping integration test")
	}

	blockNum := uint64(1)
	if blockStr := os.Getenv("L2RPC_BLOCK"); blockStr != "" {
		var err error
		blockNum, err = strconv.ParseUint(blockStr, 10, 64)
		require.NoError(t, err, "invalid L2RPC_BLOCK value")
	}

	logger := log.New()

	blockClient, err := rpc.Dial(l2rpc, logger)
	require.NoError(t, err, "failed to connect to L2RPC endpoint")
	defer blockClient.Close()

	err = checkL2RPCEndpointHealth(context.Background(), blockClient, blockNum, l2rpc)
	require.NoError(t, err)

	t.Logf("Health check passed for block %d", blockNum)
	t.Logf("  Block endpoint: %s", l2rpc)
}
