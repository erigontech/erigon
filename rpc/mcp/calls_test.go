package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

// fakeCaller records the JSON-RPC method and positional args of the last call
// and plays back a canned raw result.
type fakeCaller struct {
	method string
	args   []any
	result json.RawMessage
	err    error
}

func (f *fakeCaller) CallContext(ctx context.Context, result any, method string, args ...any) error {
	f.method = method
	f.args = args
	if f.err != nil {
		return f.err
	}
	if f.result != nil {
		*(result.(*json.RawMessage)) = f.result
	}
	return nil
}

func callTool(t *testing.T, e *ErigonMCPServer, name string, args map[string]any) string {
	t.Helper()
	argsJSON, err := json.Marshal(args)
	require.NoError(t, err)
	msg := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, name, argsJSON)
	resp := e.mcpServer.HandleMessage(context.Background(), []byte(msg))
	jsonResp, ok := resp.(mcp.JSONRPCResponse)
	require.True(t, ok, "unexpected response type %T: %+v", resp, resp)
	result, ok := jsonResp.Result.(*mcp.CallToolResult)
	require.True(t, ok, "unexpected result type %T", jsonResp.Result)
	require.NotEmpty(t, result.Content)
	text, ok := mcp.AsTextContent(result.Content[0])
	require.True(t, ok, "unexpected content type %T", result.Content[0])
	return text.Text
}

func TestToolArgMapping(t *testing.T) {
	tests := []struct {
		tool     string
		args     map[string]any
		result   string
		wantArgs []any
	}{
		{
			tool:     "eth_getBlockByNumber",
			args:     map[string]any{"blockNumber": "0x10", "fullTransactions": true},
			result:   `{"number":"0x10"}`,
			wantArgs: []any{"0x10", true},
		},
		{
			tool:     "eth_getBlockByNumber",
			args:     map[string]any{},
			result:   `{"number":"0x10"}`,
			wantArgs: []any{"latest", false},
		},
		{
			tool:     "eth_getTransactionByBlockNumberAndIndex",
			args:     map[string]any{"blockNumber": "0x10", "index": 5},
			result:   `{}`,
			wantArgs: []any{"0x10", "0x5"},
		},
		{
			tool:     "erigon_blockNumber",
			args:     map[string]any{},
			result:   `"0x10"`,
			wantArgs: []any{},
		},
		{
			tool:     "erigon_blockNumber",
			args:     map[string]any{"blockNumber": "safe"},
			result:   `"0x10"`,
			wantArgs: []any{"safe"},
		},
		{
			tool:     "eth_getProof",
			args:     map[string]any{"address": "0xabc"},
			result:   `{}`,
			wantArgs: []any{"0xabc", json.RawMessage("[]"), "latest"},
		},
		{
			tool:     "eth_getProof",
			args:     map[string]any{"address": "0xabc", "storageKeys": `["0x1"]`},
			result:   `{}`,
			wantArgs: []any{"0xabc", json.RawMessage(`["0x1"]`), "latest"},
		},
		{
			tool:     "ots_searchTransactionsBefore",
			args:     map[string]any{"address": "0xabc", "blockNumber": 100},
			result:   `{}`,
			wantArgs: []any{"0xabc", 100, 25},
		},
		{
			tool:     "eth_getLogs",
			args:     map[string]any{"fromBlock": "0x1", "address": "0xabc", "topics": `[["0xdead"]]`},
			result:   `[]`,
			wantArgs: []any{map[string]any{"fromBlock": "0x1", "address": "0xabc", "topics": json.RawMessage(`[["0xdead"]]`)}},
		},
		{
			tool:     "eth_getLogs",
			args:     map[string]any{"address": `["0xabc","0xdef"]`},
			result:   `[]`,
			wantArgs: []any{map[string]any{"address": []string{"0xabc", "0xdef"}}},
		},
		{
			tool:     "txpool_status",
			args:     map[string]any{},
			result:   `{"pending":"0x0"}`,
			wantArgs: []any{},
		},
		{
			tool:     "txpool_contentFrom",
			args:     map[string]any{"address": "0xabc"},
			result:   `{}`,
			wantArgs: []any{"0xabc"},
		},
		{
			tool:     "debug_traceTransaction",
			args:     map[string]any{"txHash": "0x1"},
			result:   `{}`,
			wantArgs: []any{"0x1", map[string]any{"tracer": "callTracer"}},
		},
		{
			tool:     "debug_traceTransaction",
			args:     map[string]any{"txHash": "0x1", "tracer": "prestateTracer"},
			result:   `{}`,
			wantArgs: []any{"0x1", map[string]any{"tracer": "prestateTracer"}},
		},
		{
			tool:     "debug_traceCall",
			args:     map[string]any{"to": "0xabc", "data": "0x01"},
			result:   `{}`,
			wantArgs: []any{map[string]any{"to": "0xabc", "data": "0x01"}, "latest", map[string]any{"tracer": "callTracer"}},
		},
		{
			tool:     "debug_getModifiedAccountsByNumber",
			args:     map[string]any{"startBlock": "0x1"},
			result:   `[]`,
			wantArgs: []any{"0x1"},
		},
		{
			tool:     "debug_getModifiedAccountsByNumber",
			args:     map[string]any{"startBlock": "0x1", "endBlock": "0x5"},
			result:   `[]`,
			wantArgs: []any{"0x1", "0x5"},
		},
		{
			tool:     "trace_transaction",
			args:     map[string]any{"txHash": "0x1"},
			result:   `[]`,
			wantArgs: []any{"0x1"},
		},
		{
			tool:     "trace_filter",
			args:     map[string]any{"fromBlock": "0x1", "toAddress": "0xabc"},
			result:   `[]`,
			wantArgs: []any{map[string]any{"fromBlock": "0x1", "toAddress": []string{"0xabc"}, "count": 100}},
		},
		{
			tool:     "trace_filter",
			args:     map[string]any{"toAddress": ` ["0xabc","0xdef"]`},
			result:   `[]`,
			wantArgs: []any{map[string]any{"toAddress": []string{"0xabc", "0xdef"}, "count": 100}},
		},
		{
			tool:     "trace_filter",
			args:     map[string]any{"fromBlock": "0x1", "count": 500},
			result:   `[]`,
			wantArgs: []any{map[string]any{"fromBlock": "0x1", "count": 500}},
		},
		{
			tool:     "debug_traceTransaction",
			args:     map[string]any{"txHash": "0x1", "tracer": ""},
			result:   `{}`,
			wantArgs: []any{"0x1"},
		},
		{
			tool:     "debug_traceTransaction",
			args:     map[string]any{"txHash": "0x1", "tracer": " "},
			result:   `{}`,
			wantArgs: []any{"0x1"},
		},
		{
			tool:     "debug_traceBlockByNumber",
			args:     map[string]any{"blockNumber": "22000000"},
			result:   `{}`,
			wantArgs: []any{"0x14fb180", map[string]any{"tracer": "callTracer"}},
		},
		{
			tool:     "debug_getModifiedAccountsByNumber",
			args:     map[string]any{"startBlock": "100", "endBlock": "200"},
			result:   `[]`,
			wantArgs: []any{"0x64", "0xc8"},
		},
		{
			tool:     "trace_filter",
			args:     map[string]any{"fromBlock": "100", "toAddress": "0xabc"},
			result:   `[]`,
			wantArgs: []any{map[string]any{"fromBlock": "0x64", "toAddress": []string{"0xabc"}, "count": 100}},
		},
		{
			tool:     "debug_traceCall",
			args:     map[string]any{"to": "0xabc", "blockNumber": "22000000"},
			result:   `{}`,
			wantArgs: []any{map[string]any{"to": "0xabc"}, "0x14fb180", map[string]any{"tracer": "callTracer"}},
		},
		{
			tool:     "eth_call",
			args:     map[string]any{"to": "0xabc", "data": "0x01", "from": "0xdef"},
			result:   `"0x"`,
			wantArgs: []any{map[string]any{"to": "0xabc", "data": "0x01", "from": "0xdef"}, "latest"},
		},
		{
			tool:     "eth_estimateGas",
			args:     map[string]any{"to": "0xabc"},
			result:   `"0x5208"`,
			wantArgs: []any{map[string]any{"to": "0xabc"}},
		},
		{
			tool:     "eth_getBlockByNumber",
			args:     map[string]any{"blockNumber": "22000000"},
			result:   `{}`,
			wantArgs: []any{"0x14fb180", false},
		},
		{
			tool:     "eth_getBlockByNumber",
			args:     map[string]any{"blockNumber": " 22000000 "},
			result:   `{}`,
			wantArgs: []any{"0x14fb180", false},
		},
		{
			tool:     "eth_getLogs",
			args:     map[string]any{"fromBlock": "100", "toBlock": "200"},
			result:   `[]`,
			wantArgs: []any{map[string]any{"fromBlock": "0x64", "toBlock": "0xc8"}},
		},
		{
			tool:     "eth_getProof",
			args:     map[string]any{"address": "0xabc", "storageKeys": ""},
			result:   `{}`,
			wantArgs: []any{"0xabc", json.RawMessage("[]"), "latest"},
		},
		{
			tool:     "eth_getStorageAt",
			args:     map[string]any{"address": "0xabc", "position": "0x2"},
			result:   `"0x0"`,
			wantArgs: []any{"0xabc", "0x2", "latest"},
		},
		{
			tool:     "ots_getBlockTransactions",
			args:     map[string]any{"blockNumber": "0x10", "pageNumber": 2, "pageSize": 10},
			result:   `{}`,
			wantArgs: []any{"0x10", 2, 10},
		},
		{
			tool:     "ots_getTransactionBySenderAndNonce",
			args:     map[string]any{"address": "0xabc", "nonce": 7},
			result:   `"0x1"`,
			wantArgs: []any{"0xabc", 7},
		},
		{
			tool:     "erigon_getBlockByTimestamp",
			args:     map[string]any{"timestamp": "1700000000", "fullTransactions": true},
			result:   `{}`,
			wantArgs: []any{"1700000000", true},
		},
		{
			tool:     "eth_getStorageValues",
			args:     map[string]any{"requests": `{"0xabc":["0x1"]}`, "blockNumber": "latest"},
			result:   `{}`,
			wantArgs: []any{json.RawMessage(`{"0xabc":["0x1"]}`), "latest"},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%v", tt.tool, tt.args), func(t *testing.T) {
			caller := &fakeCaller{result: json.RawMessage(tt.result)}
			e := NewErigonMCPServer(caller, "", false)
			callTool(t, e, tt.tool, tt.args)
			require.Equal(t, tt.tool, caller.method)
			require.Equal(t, tt.wantArgs, caller.args)
		})
	}
}

func TestToolResultFormatting(t *testing.T) {
	tests := []struct {
		tool   string
		args   map[string]any
		result string
		want   string
	}{
		{tool: "eth_blockNumber", result: `"0x1528f0a"`, want: "Current block: 22187786 (0x1528f0a)"},
		{tool: "eth_getBalance", args: map[string]any{"address": "0xabc"}, result: `"0xde0b6b3a7640000"`, want: "Balance: 1000000000000000000 wei (1.000000 ETH)"},
		{tool: "eth_syncing", result: `false`, want: "Fully synced"},
		{tool: "eth_getBlockByNumber", args: map[string]any{"blockNumber": "0x999"}, result: `null`, want: "Block not found"},
		{tool: "eth_getCode", args: map[string]any{"address": "0xabc"}, result: `"0x"`, want: "No code (EOA)"},
		{tool: "eth_getCode", args: map[string]any{"address": "0xabc"}, result: `"0x6001"`, want: "Code (2 bytes): 0x6001"},
		{tool: "eth_gasPrice", result: `"0x3b9aca00"`, want: "Gas price: 1000000000 wei (1.00 Gwei)"},
		{tool: "ots_hasCode", args: map[string]any{"address": "0xabc"}, result: `true`, want: "has code (is a contract)"},
		{tool: "ots_getTransactionError", args: map[string]any{"txHash": "0x1"}, result: `"0x"`, want: "Transaction succeeded (no error)"},
		{tool: "ots_getTransactionError", args: map[string]any{"txHash": "0x1"}, result: `"0x08c379a0"`, want: "Transaction error: 0x08c379a0"},
		{tool: "eth_getBlockTransactionCountByNumber", args: map[string]any{"blockNumber": "0x999"}, result: `null`, want: "Block not found"},
		{tool: "eth_getUncleCountByBlockNumber", args: map[string]any{"blockNumber": "0x999"}, result: `null`, want: "Block not found"},
		{tool: "eth_chainId", result: `"0x27d8"`, want: "Chain ID: 10200 (0x27d8) - chiado"},
		{tool: "eth_chainId", result: `"0xfffffe"`, want: "Chain ID: 16777214 (0xfffffe)"},
		{tool: "ots_getTransactionBySenderAndNonce", args: map[string]any{"address": "0xabc", "nonce": 1}, result: `null`, want: "Transaction not found"},
		{tool: "ots_getTransactionBySenderAndNonce", args: map[string]any{"address": "0xabc", "nonce": 1}, result: `"0xdead"`, want: "Transaction hash: 0xdead"},
		{tool: "eth_syncing", result: `{"currentBlock":"0x10"}`, want: `"currentBlock": "0x10"`},
		{tool: "net_peerCount", result: `"0x19"`, want: "Peers: 25"},
		{tool: "net_version", result: `"10200"`, want: "Network ID: 10200"},
		{tool: "net_listening", result: `true`, want: "Listening: true"},
		{tool: "admin_peers", result: `[]`, want: "No peers connected"},
	}

	for _, tt := range tests {
		t.Run(tt.tool+"/"+tt.want, func(t *testing.T) {
			caller := &fakeCaller{result: json.RawMessage(tt.result)}
			e := NewErigonMCPServer(caller, "", false)
			got := callTool(t, e, tt.tool, tt.args)
			require.Contains(t, got, tt.want)
		})
	}
}

func TestToolCallError(t *testing.T) {
	caller := &fakeCaller{err: errors.New("boom")}
	e := NewErigonMCPServer(caller, "", false)
	got := callTool(t, e, "eth_blockNumber", nil)
	require.Contains(t, got, "boom")
}

func TestTraceFilterCountBounds(t *testing.T) {
	caller := &fakeCaller{result: json.RawMessage(`[]`)}
	e := NewErigonMCPServer(caller, "", false)

	got := callTool(t, e, "trace_filter", map[string]any{"count": 5000})
	require.Empty(t, caller.method, "out-of-range count must not reach dispatch")
	require.Contains(t, got, "count must be between")

	got = callTool(t, e, "trace_filter", map[string]any{"count": 0})
	require.Empty(t, caller.method)
	require.Contains(t, got, "count must be between")
}

// Schema validation must reject mistyped arguments instead of letting the
// mcp-go Get* helpers silently substitute defaults (which would issue a
// valid-looking JSON-RPC call with the wrong arguments).
func TestInputValidationRejectsMistypedArgs(t *testing.T) {
	caller := &fakeCaller{result: json.RawMessage(`"0x0"`)}
	e := NewErigonMCPServer(caller, "", false)

	got := callTool(t, e, "eth_getBalance", map[string]any{"address": "0xabc", "blockNumber": 12345678})
	require.Empty(t, caller.method, "mistyped argument must not reach the JSON-RPC dispatch, got call %s(%v) — result: %s", caller.method, caller.args, got)

	got = callTool(t, e, "ots_getTransactionBySenderAndNonce", map[string]any{"address": "0xabc", "nonce": "0xa"})
	require.Empty(t, caller.method, "string nonce must not silently become 0 — result: %s", got)
}

// Every table entry must be well-formed: unique names, and a param that can be
// omitted from the call must be the last one (dropping a middle positional
// argument would shift the ones after it).
func TestToolCallTable(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range rpcToolCalls() {
		require.False(t, seen[c.name], "duplicate tool %s", c.name)
		seen[c.name] = true
		require.NotEmpty(t, c.desc, "tool %s has no description", c.name)
		for i, p := range c.params {
			if p.omit {
				require.Equal(t, len(c.params)-1, i, "tool %s: omittable param %s must be last", c.name, p.name)
			}
			require.False(t, p.required && (p.def != "" || p.defInt != 0),
				"tool %s: param %s is required but has a default — schema-validating clients would reject calls the handler accepts", c.name, p.name)
		}
	}
	require.Contains(t, seen, "eth_blockNumber")
	require.Contains(t, seen, "ots_getContractCreator")
	require.Contains(t, seen, "eth_getStorageValues")
	require.Contains(t, seen, "txpool_status")
	require.Contains(t, seen, "txpool_content")
	require.Contains(t, seen, "txpool_contentFrom")
	require.Contains(t, seen, "net_version")
	require.Contains(t, seen, "net_listening")
	require.Contains(t, seen, "net_peerCount")
	require.Contains(t, seen, "admin_nodeInfo")
	require.Contains(t, seen, "admin_peers")
	for _, mutating := range []string{"admin_addPeer", "admin_removePeer", "admin_addTrustedPeer", "admin_removeTrustedPeer"} {
		require.NotContains(t, seen, mutating, "mutating admin methods must not be exposed")
	}
	require.Contains(t, seen, "debug_traceTransaction")
	require.Contains(t, seen, "debug_traceBlockByNumber")
	require.Contains(t, seen, "debug_traceCall")
	require.Contains(t, seen, "debug_getModifiedAccountsByNumber")
	require.Contains(t, seen, "trace_transaction")
	require.Contains(t, seen, "trace_block")
	require.Contains(t, seen, "trace_filter")
}
