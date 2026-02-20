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

func TestCheckL2RPCEndpointsHealth_NilClients(t *testing.T) {
	ctx := context.Background()

	err := checkL2RPCEndpointsHealth(ctx, nil, nil, 100, "", "")
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointsHealth_NilReceiptClient(t *testing.T) {
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

	err = checkL2RPCEndpointsHealth(context.Background(), client, nil, 100, srv.URL, "")
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointsHealth_Success(t *testing.T) {
	txHash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": txHash,
				},
			},
		},
		"eth_getTransactionReceipt": map[string]interface{}{
			"transactionHash": txHash,
			"status":          "0x1",
		},
		"arb_getRawBlockMetadata": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointsHealth_BlockError(t *testing.T) {
	responses := map[string]interface{}{
		"eth_getBlockByNumber": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned nil for block")
}

func TestCheckL2RPCEndpointsHealth_ReceiptMismatch(t *testing.T) {
	txHash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": txHash,
				},
			},
		},
		"eth_getTransactionReceipt": map[string]interface{}{
			"transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"status":          "0x0",
		},
		"arb_getRawBlockMetadata": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned mismatched receipt")
}

func TestCheckL2RPCEndpointsHealth_EmptyTransactions(t *testing.T) {
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

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
	require.NoError(t, err)
}

func TestCheckL2RPCEndpointsHealth_NilReceipt(t *testing.T) {
	txHash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": txHash,
				},
			},
		},
		"eth_getTransactionReceipt": nil,
		"arb_getRawBlockMetadata": nil,
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "returned nil for tx")
}

func TestCheckL2RPCEndpointsHealth_BlockMetadataMethodNotSupported(t *testing.T) {
	txHash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	responses := map[string]interface{}{
		"eth_getBlockByNumber": map[string]interface{}{
			"number": "0x64",
			"hash":   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"transactions": []interface{}{
				map[string]interface{}{
					"hash": txHash,
				},
			},
		},
		"eth_getTransactionReceipt": map[string]interface{}{
			"transactionHash": txHash,
			"status":          "0x1",
		},
		// arb_getRawBlockMetadata
	}
	srv := newMockRPCServer(t, responses)
	defer srv.Close()

	client, err := rpc.Dial(srv.URL, log.New())
	require.NoError(t, err)
	defer client.Close()

	err = checkL2RPCEndpointsHealth(context.Background(), client, client, 100, srv.URL, srv.URL)
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

func TestGetPublicReceiptFeed(t *testing.T) {
	tests := []struct {
		chainID  uint64
		expected string
	}{
		{421614, "https://sepolia-rollup.arbitrum.io/rpc"},
		{42161, "https://arb1.arbitrum.io/rpc"},
		{42170, "https://nova.arbitrum.io/rpc"},
		{1, ""},      // Ethereum mainnet - no feed
		{999999, ""}, // Unknown chain - no feed
	}

	for _, tt := range tests {
		t.Run(strconv.FormatUint(tt.chainID, 10), func(t *testing.T) {
			result := getPublicReceiptFeed(tt.chainID)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestCheckL2RPCEndpointsHealth_Integration tests against real RPC endpoints.
// Set environment variables to run:
//   - L2RPC: block RPC endpoint URL (required)
//   - L2RPC_RECEIPT: receipt RPC endpoint URL (optional, defaults to L2RPC)
//   - L2RPC_BLOCK: block number to check (optional, defaults to 1)
//
// Example:
//
//	L2RPC=http://localhost:8547 go test -v -run TestCheckL2RPCEndpointsHealth_Integration
//	L2RPC=http://localhost:8547 L2RPC_RECEIPT=http://localhost:8548 L2RPC_BLOCK=1000 go test -v -run TestCheckL2RPCEndpointsHealth_Integration
func TestCheckL2RPCEndpointsHealth_Integration(t *testing.T) {
	l2rpc := os.Getenv("L2RPC")
	if l2rpc == "" {
		t.Skip("L2RPC environment variable not set, skipping integration test")
	}

	l2rpcReceipt := os.Getenv("L2RPC_RECEIPT")
	if l2rpcReceipt == "" {
		l2rpcReceipt = l2rpc
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

	var receiptClient *rpc.Client
	if l2rpcReceipt == l2rpc {
		receiptClient = blockClient
	} else {
		receiptClient, err = rpc.Dial(l2rpcReceipt, logger)
		require.NoError(t, err, "failed to connect to L2RPC_RECEIPT endpoint")
		defer receiptClient.Close()
	}

	err = checkL2RPCEndpointsHealth(context.Background(), blockClient, receiptClient, blockNum, l2rpc, l2rpcReceipt)
	require.NoError(t, err)

	t.Logf("Health check passed for block %d", blockNum)
	t.Logf("  Block metadata endpoint: %s", l2rpc)
	t.Logf("  Receipt endpoint: %s", l2rpcReceipt)
}
