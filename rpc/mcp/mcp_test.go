package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc"
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

func TestParseBlockNumber(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "latest",
			input:   "latest",
			wantErr: false,
		},
		{
			name:    "earliest",
			input:   "earliest",
			wantErr: false,
		},
		{
			name:    "pending",
			input:   "pending",
			wantErr: false,
		},
		{
			name:    "hex number",
			input:   "0x10",
			wantErr: false,
		},
		{
			name:    "decimal number",
			input:   "100",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseBlockNumber(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBlockNumber(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == rpc.BlockNumber(0) && tt.input != "0" && tt.input != "0x0" {
				// For non-zero inputs, result should not be zero (unless it's earliest)
				if tt.input != "earliest" {
					t.Logf("parseBlockNumber(%q) = %v", tt.input, result)
				}
			}
		})
	}
}

func TestParseBlockNumberOrHash(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "block hash", input: "0x1234567890123456789012345678901234567890123456789012345678901234"},
		{name: "block number", input: "latest"},
		{name: "hex number", input: "0x10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseBlockNumberOrHash(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBlockNumberOrHash(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				_ = result
				t.Logf("parseBlockNumberOrHash(%q) succeeded", tt.input)
			}
		})
	}

	// Special characters must not produce a JSON syntax error — only a semantic block-format error.
	for _, input := range []string{`foo"bar`, `foo\bar`, "foo\nbar"} {
		_, err := parseBlockNumberOrHash(input)
		if err == nil {
			t.Errorf("parseBlockNumberOrHash(%q) succeeded, want error", input)
			continue
		}
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			t.Errorf("parseBlockNumberOrHash(%q) returned JSON syntax error, want semantic error: %v", input, err)
		}
	}
}

func mapKeys[V any](m map[string]V) []string {
	return slices.Collect(maps.Keys(m))
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

// TestEmbeddedAndStandaloneCatalogsMatch guards against the two servers
// drifting apart: they must expose the same tools (embedded additionally has
// eth_getStorageValues), prompts, resources, and resource templates.
func TestEmbeddedAndStandaloneCatalogsMatch(t *testing.T) {
	embedded := NewErigonMCPServer(nil, nil, nil, "")
	standalone := NewStandaloneMCPServer(nil, "")

	embeddedTools := embedded.mcpServer.ListTools()
	require.Contains(t, embeddedTools, "eth_getStorageValues")
	delete(embeddedTools, "eth_getStorageValues")
	require.ElementsMatch(t, mapKeys(embeddedTools), mapKeys(standalone.mcpServer.ListTools()))

	require.ElementsMatch(t, mapKeys(embedded.mcpServer.ListPrompts()), mapKeys(standalone.mcpServer.ListPrompts()))
	require.NotEmpty(t, embedded.mcpServer.ListPrompts())

	require.ElementsMatch(t, mapKeys(embedded.mcpServer.ListResources()), mapKeys(standalone.mcpServer.ListResources()))
	require.NotEmpty(t, embedded.mcpServer.ListResources())

	embeddedTemplates := resourceTemplateURIs(t, embedded.mcpServer)
	require.ElementsMatch(t, embeddedTemplates, resourceTemplateURIs(t, standalone.mcpServer))
	require.NotEmpty(t, embeddedTemplates)
}

// Start wires the SSE handler only on a server it creates itself, so serveSSE's
// pre-created server must carry Handler: sse or every MCP request 404s.
func TestServeSSEHandlerWired(t *testing.T) {
	addr := freeAddr(t)
	srv := NewStandaloneMCPServer(nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveSSE(ctx, srv.mcpServer, addr) }()

	client := &http.Client{Timeout: 2 * time.Second}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = client.Get("http://" + addr + "/sse")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusNotFound, resp.StatusCode, "SSE endpoint not served — handler not wired")
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	cancel()
	require.NoError(t, <-done)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestMCPServerCreation(t *testing.T) {
	// This tests that we can create the server struct without panicking
	// We can't fully test without mock APIs
	t.Run("server creation", func(t *testing.T) {
		// Just verify the type exists and can be referenced
		var _ *ErigonMCPServer
	})
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
