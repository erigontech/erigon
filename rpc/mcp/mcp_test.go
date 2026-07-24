package mcp

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/require"
)

func TestToJSONText(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "null",
		},
		{
			name:     "simple string",
			input:    "test",
			expected: "\"test\"",
		},
		{
			name:     "simple number",
			input:    42,
			expected: "42",
		},
		{
			name:     "map",
			input:    map[string]string{"key": "value"},
			expected: "{\n  \"key\": \"value\"\n}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toJSONText(tt.input)
			if result != tt.expected {
				t.Errorf("toJSONText(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToJSONIndentNull(t *testing.T) {
	require.Equal(t, "null", toJSONIndent(nil))
	require.Equal(t, "null", toJSONIndent(json.RawMessage("null")))
	require.Equal(t, "{}", toJSONIndent(json.RawMessage("{}")))
}

func resourceTemplateURIs(t *testing.T, srv *server.MCPServer) []string {
	t.Helper()
	resp := srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"resources/templates/list"}`))
	jsonResp, ok := resp.(mcp.JSONRPCResponse)
	require.True(t, ok, "unexpected response type %T", resp)
	result, ok := jsonResp.Result.(mcp.ListResourceTemplatesResult)
	require.True(t, ok, "unexpected result type %T", jsonResp.Result)
	uris := make([]string, 0, len(result.ResourceTemplates))
	for _, tmpl := range result.ResourceTemplates {
		uris = append(uris, tmpl.URITemplate.Raw())
	}
	return uris
}

// TestServerCatalog pins the composition of the server: JSON-RPC tools from
// the call table, local log/metrics tools, prompts, resources and templates.
func TestServerCatalog(t *testing.T) {
	e := NewErigonMCPServer(nil, "", false)

	tools := e.mcpServer.ListTools()
	require.Len(t, tools, len(rpcToolCalls())+6, "expected call-table tools plus 4 logs_* and 2 metrics_* tools")
	for _, name := range []string{"eth_blockNumber", "erigon_nodeInfo", "ots_traceTransaction", "logs_tail", "logs_grep", "metrics_list", "metrics_get"} {
		require.Contains(t, tools, name)
	}

	require.NotEmpty(t, e.mcpServer.ListPrompts())
	require.NotEmpty(t, e.mcpServer.ListResources())
	require.NotEmpty(t, resourceTemplateURIs(t, e.mcpServer))
}

// Every tool is a read-only query; declaring it lets MCP clients skip
// per-call approval prompts for them.
func TestAllToolsDeclareReadOnlyHint(t *testing.T) {
	e := NewErigonMCPServer(nil, "", false)
	for name, st := range e.mcpServer.ListTools() {
		hint := st.Tool.Annotations.ReadOnlyHint
		require.NotNil(t, hint, "tool %s missing readOnlyHint annotation", name)
		require.True(t, *hint, "tool %s must declare readOnlyHint=true", name)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestJSONMarshaling(t *testing.T) {
	// Test that our utility functions work with complex nested structures
	complexData := map[string]any{
		"block": map[string]any{
			"number":     12345,
			"hash":       "0xabcdef",
			"timestamp":  1234567890,
			"difficulty": "1000000",
			"transactions": []string{
				"0x1111111111111111111111111111111111111111111111111111111111111111",
				"0x2222222222222222222222222222222222222222222222222222222222222222",
			},
		},
	}

	result := toJSONText(complexData)

	// Verify it's valid JSON
	var parsed map[string]any
	err := json.Unmarshal([]byte(result), &parsed)
	if err != nil {
		t.Errorf("toJSONText produced invalid JSON: %v", err)
	}

	// Verify structure is preserved
	if block, ok := parsed["block"].(map[string]any); ok {
		if number, ok := block["number"].(float64); !ok || number != 12345 {
			t.Errorf("Block number not preserved correctly")
		}
	} else {
		t.Errorf("Block structure not preserved")
	}
}
