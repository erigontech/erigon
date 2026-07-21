package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

// scriptedCaller plays back canned raw results per JSON-RPC method and
// records the positional args of the last call per method.
type scriptedCaller struct {
	responses map[string]json.RawMessage
	lastArgs  map[string][]any
}

func (s *scriptedCaller) CallContext(_ context.Context, result any, method string, args ...any) error {
	if s.lastArgs == nil {
		s.lastArgs = map[string][]any{}
	}
	s.lastArgs[method] = args
	raw, ok := s.responses[method]
	if !ok {
		return fmt.Errorf("no scripted response for %s", method)
	}
	return json.Unmarshal(raw, result)
}

func readResourceRequest(uri string) mcp.ReadResourceRequest {
	var req mcp.ReadResourceRequest
	req.Params.URI = uri
	return req
}

func resourceJSON(t *testing.T, contents []mcp.ResourceContents) map[string]any {
	t.Helper()
	require.Len(t, contents, 1)
	text, ok := contents[0].(mcp.TextResourceContents)
	require.True(t, ok)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(text.Text), &m))
	return m
}

func TestExtractURIParam(t *testing.T) {
	tests := []struct {
		uri, prefix, suffix, want string
	}{
		{"erigon://address/0xabc/summary", "erigon://address/", "/summary", "0xabc"},
		{"erigon://block/latest/summary", "erigon://block/", "/summary", "latest"},
		{"erigon://node/info", "erigon://node/", "", "info"},
		{"erigon://address//summary", "erigon://address/", "/summary", ""},
		{"erigon://address/0xabc", "erigon://address/", "/summary", ""},
		{"mainnet://address/0xabc/summary", "erigon://address/", "/summary", ""},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, extractURIParam(tt.uri, tt.prefix, tt.suffix), "uri=%s", tt.uri)
	}
}

func TestResourceNetworkStatusReportsSyncProgress(t *testing.T) {
	e := NewErigonMCPServer(&scriptedCaller{responses: map[string]json.RawMessage{
		"erigon_nodeInfo": json.RawMessage(`null`),
		"eth_blockNumber": json.RawMessage(`"0x10"`),
		"eth_syncing":     json.RawMessage(`{"currentBlock":"0x10","highestBlock":"0x20"}`),
	}}, "", false)

	contents, err := e.handleResourceNetworkStatus(context.Background(), readResourceRequest("erigon://network/status"))
	require.NoError(t, err)

	status := resourceJSON(t, contents)
	require.Equal(t, map[string]any{"currentBlock": "0x10", "highestBlock": "0x20"}, status["syncing"])
}

func TestResourceNetworkStatusWhenSynced(t *testing.T) {
	e := NewErigonMCPServer(&scriptedCaller{responses: map[string]json.RawMessage{
		"erigon_nodeInfo": json.RawMessage(`null`),
		"eth_blockNumber": json.RawMessage(`"0x10"`),
		"eth_syncing":     json.RawMessage(`false`),
	}}, "", false)

	contents, err := e.handleResourceNetworkStatus(context.Background(), readResourceRequest("erigon://network/status"))
	require.NoError(t, err)

	status := resourceJSON(t, contents)
	require.Equal(t, false, status["syncing"])
}

func TestResourceAddressSummaryExtractsAddress(t *testing.T) {
	addr := "0x00000000000000000000000000000000deadbeef"
	caller := &scriptedCaller{responses: map[string]json.RawMessage{
		"eth_getBalance":          json.RawMessage(`"0x2a"`),
		"eth_getTransactionCount": json.RawMessage(`"0x7"`),
		"eth_getCode":             json.RawMessage(`"0x6000"`),
	}}
	e := NewErigonMCPServer(caller, "", false)

	contents, err := e.handleResourceAddressSummary(context.Background(), readResourceRequest("erigon://address/"+addr+"/summary"))
	require.NoError(t, err)

	summary := resourceJSON(t, contents)
	require.Equal(t, addr, summary["address"])
	require.Equal(t, []any{addr, "latest"}, caller.lastArgs["eth_getBalance"])
	require.Equal(t, true, summary["is_contract"])
}

func TestResourceAddressSummaryNullsOnRPCFailure(t *testing.T) {
	caller := &scriptedCaller{responses: map[string]json.RawMessage{
		"eth_getBalance": json.RawMessage(`"0x2a"`),
		// eth_getTransactionCount and eth_getCode have no scripted response and error.
	}}
	e := NewErigonMCPServer(caller, "", false)

	contents, err := e.handleResourceAddressSummary(context.Background(), readResourceRequest("erigon://address/0xabc/summary"))
	require.NoError(t, err)

	summary := resourceJSON(t, contents)
	require.Equal(t, "0x2a", summary["balance"])
	require.Nil(t, summary["nonce"])
	require.Nil(t, summary["is_contract"], "a failed eth_getCode must not report a confident EOA verdict")
}

func TestResourceAddressSummaryMissingAddress(t *testing.T) {
	e := NewErigonMCPServer(&scriptedCaller{}, "", false)

	_, err := e.handleResourceAddressSummary(context.Background(), readResourceRequest("erigon://address//summary"))
	require.Error(t, err)
}

func TestResourceBlockSummaryExtractsNumber(t *testing.T) {
	caller := &scriptedCaller{responses: map[string]json.RawMessage{
		"eth_getBlockByNumber": json.RawMessage(`{"number":"0x5"}`),
	}}
	e := NewErigonMCPServer(caller, "", false)

	contents, err := e.handleResourceBlockSummary(context.Background(), readResourceRequest("erigon://block/0x5/summary"))
	require.NoError(t, err)
	require.Equal(t, []any{"0x5", false}, caller.lastArgs["eth_getBlockByNumber"])

	block := resourceJSON(t, contents)
	require.Equal(t, "0x5", block["number"])
}

func TestResourceTransactionAnalysisDerivesStatus(t *testing.T) {
	txHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	uri := "erigon://transaction/" + txHash + "/analysis"

	tests := []struct {
		name    string
		receipt string
		want    string
	}{
		{"reverted", `{"status":"0x0"}`, "reverted"},
		{"success", `{"status":"0x1"}`, "success"},
		{"pre-byzantium", `{"root":"0x01"}`, "unknown"},
		{"not found", `null`, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := &scriptedCaller{responses: map[string]json.RawMessage{
				"eth_getTransactionByHash":  json.RawMessage(`{}`),
				"eth_getTransactionReceipt": json.RawMessage(tt.receipt),
			}}
			e := NewErigonMCPServer(caller, "", false)

			contents, err := e.handleResourceTransactionAnalysis(context.Background(), readResourceRequest(uri))
			require.NoError(t, err)
			require.Equal(t, []any{txHash}, caller.lastArgs["eth_getTransactionReceipt"])

			analysis := resourceJSON(t, contents)
			require.Equal(t, tt.want, analysis["status"])
		})
	}
}
